package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/uber-go/zap"
	"os"
)

var (
	logger = zap.NewJSON()
)

type Txn struct {
	Session   string
	Begin     int64
	End       int64
	Bytes     int64
	Direction string
}

type GStat struct {
	Bytes     int64
	Diff      int64
	Direction string
	Count     int64
}

func main() {
	gstats := make(map[string]*GStat)
	gstats["txn up"] = &GStat{Direction: "txn up"}
	gstats["txn down"] = &GStat{Direction: "txn down"}

	txns := make(map[string]*Txn)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			var obj interface{}
			lineBuffer := bytes.NewBuffer([]byte(line))
			decoder := json.NewDecoder(lineBuffer)
			decoder.UseNumber()
			err := decoder.Decode(&obj)
			if err != nil {
				logger.Error(
					"parse fail",
					zap.String("err", err.Error()),
					zap.String("line", line),
				)
			} else {
				fmt.Println(line)
				logger.Debug("json")
				record, record_ok := obj.(map[string]interface{})
				if record_ok {
					logger.Debug("record")
					ts, ts_ok := record["ts"].(json.Number)
					msg, msg_ok := record["msg"].(string)
					fields, fields_ok := record["fields"].(map[string]interface{})
					if msg_ok && fields_ok && ts_ok {
						logger.Debug("session")
						session, session_ok := fields["session"].(string)
						tsVal, err := ts.Int64()
						if err != nil {
							logger.Error("ts convert fail", zap.Object("ts", ts))
						}
						if session_ok {
							switch msg {
							case "txn begin":
								logger.Debug("begin")
								txns[session] = &Txn{Session: session}
								txns[session].Session = session
								txns[session].Begin = tsVal
							case "txn end":
								logger.Debug("end")
								txns[session].End = tsVal
								txn := txns[session]
								diff := float64(txn.End - txn.Begin)
								xput := float64(txn.Bytes) / diff
								logger.Info(
									"txn",
									zap.Float64("throughput", xput),
									zap.String("session", session),
									zap.String("direction", txn.Direction),
									zap.Int64("latency", txn.End-txn.Begin),
								)
								txns[session] = nil
								gstats[txn.Direction].Bytes += txn.Bytes
								gstats[txn.Direction].Diff += txn.End - txn.Begin
								gstats[txn.Direction].Count += int64(1)
							case "txn down", "txn up":
								logger.Debug("bytes")
								bytes, bytes_ok := fields["bytes"].(json.Number)
								if bytes_ok {
									bytesVal, err := bytes.Int64()
									if err != nil {
										logger.Error("bytes convert fail", zap.Object("bytes", bytes))
									}
									txns[session].Bytes = bytesVal
									txns[session].Direction = msg
								}
							}
						}
					}
				}
			}
		}
	}
	for k, v := range gstats {
		xput := float64(v.Bytes) / float64(v.Diff)
		latency := float64(v.Diff) / float64(v.Count)
		bytes := float64(v.Bytes) / float64(v.Count)
		logger.Info(
			"gstat",
			zap.String("direction", k),
			zap.Float64("throughput", xput),
			zap.Float64("latency", latency),
			zap.Float64("bytes", bytes),
		)
	}
}
