package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/uber-go/zap"
	"os"
	"strings"
)

const (
	TString  = 1
	TInt64   = 2
	TFloat64 = 3
)

var (
	logger = zap.NewJSON()
)

type Txn struct {
	Session   string
	Begin     int64
	End       int64
	Bytes   int64
	Type string
}

type GStat struct {
	Bytes int64
	Diff  int64
	Count int64
}

type GStats struct {
	Up   GStat
	Down GStat
}

//Duplicating this because zap's members are private.  :-(
type NamedParm struct {
	Key        string
	VType      int
	StringVal  string
	Int64Val   int64
	Float64Val float64
}

func String(k string, v string) NamedParm {
	return NamedParm{
		Key:       k,
		VType:     TString,
		StringVal: v,
	}
}

func Int64(k string, v int64) NamedParm {
	return NamedParm{
		Key:      k,
		VType:    TInt64,
		Int64Val: v,
	}
}

func Float64(k string, v float64) NamedParm {
	return NamedParm{
		Key:        k,
		VType:      TFloat64,
		Float64Val: v,
	}
}

func Val(p NamedParm) string {
	switch p.VType {
	case TString:
		return p.StringVal
	case TInt64:
		return fmt.Sprintf("%d", p.Int64Val)
	case TFloat64:
		return fmt.Sprintf("%f", p.Float64Val)
	}
	return ""
}

func ToZap(params ...NamedParm) []zap.Field {
	var zapFields []zap.Field
	for _, p := range params {
		switch p.VType {
		case TString:
			zapFields = append(zapFields, zap.String(p.Key, p.StringVal))
		case TInt64:
			zapFields = append(zapFields, zap.Int64(p.Key, p.Int64Val))
		case TFloat64:
			zapFields = append(zapFields, zap.Float64(p.Key, p.Float64Val))
		}
	}
	return zapFields
}

var command = flag.String("command", "{}", "json command config")
var commandStructure interface{}
var doEchoInput = false
var doGstat = true
var doStat = false
var doSelected []string

func commandSetup() error {
	flag.Parse()
	commandBuffer := bytes.NewBuffer([]byte(*command))
	commandDecoder := json.NewDecoder(commandBuffer)
	commandDecoder.UseNumber()
	err := commandDecoder.Decode(&commandStructure)
	if err != nil {
		return err
	}
	topLevel, topLevel_ok := commandStructure.(map[string]interface{})
	if topLevel_ok {
		echo, echo_ok := topLevel["echo"].(bool)
		if echo_ok {
			doEchoInput = echo
		}
		gstat, gstat_ok := topLevel["gstat"].(bool)
		if gstat_ok {
			doGstat = gstat
		}
		stat, stat_ok := topLevel["stat"].(bool)
		if stat_ok {
			doStat = stat
		}
		selected, selected_ok := topLevel["selected"].([]interface{})
		if selected_ok {
			for _, v := range selected {
				vStr, vStr_ok := v.(string)
				if vStr_ok {
					doSelected = append(doSelected, vStr)
				}
			}
		}
	}
	return nil
}

//standard json marshaling doesn't preserve key order, which we must do, because
//we are essentially rendering a table
func render(msg string, fields ...NamedParm) {
	if len(doSelected) > 0 {
		fields = append(fields, String("msg", msg))
		w := os.Stdout
		w.Write([]byte("{"))
		i := 0
		for _, s := range doSelected {
			for _, f := range fields {
				if s == f.Key {
					if i > 0 {
						w.Write([]byte(","))
					}
					switch f.VType {
					case TString:
						w.Write([]byte(fmt.Sprintf("\"%s\"=\"%s\"", f.Key, Val(f))))
					case TInt64, TFloat64:
						w.Write([]byte(fmt.Sprintf("\"%s\"=%s", f.Key, Val(f))))
					}
					i++
				}
			}
		}
		w.Write([]byte("}\n"))
	} else {
		logger.Info(msg, ToZap(fields...)...)
	}
}

func main() {
	//logger.SetLevel(-1)
	err := commandSetup()
	if err != nil {
		logger.Error("command setup", zap.String("err", err.Error()))
		os.Exit(-1)
	}
	txnBegin := "transaction start"
	txnEnd := "transaction end"
	txnUp := "transaction up"
	txnDown := "transaction down"
	gstats := GStats{}
	txns := make(map[string]*Txn)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] != '{' {
			idx := strings.Index(line, "{")
			if idx > 0 {
				line = line[idx:]
			}
		}
		if line != "" {
			var obj interface{}
			lineBuffer := bytes.NewBuffer([]byte(line))
			decoder := json.NewDecoder(lineBuffer)
			decoder.UseNumber()
			err = decoder.Decode(&obj)
			if err != nil {
				logger.Error(
					"parse fail",
					zap.String("err", err.Error()),
					zap.String("line", line),
				)
			} else {
				if doEchoInput {
					fmt.Println(line)
				}
				record, record_ok := obj.(map[string]interface{})
				if record_ok {
					ts, ts_ok := record["ts"].(json.Number)
					msg, msg_ok := record["msg"].(string)
					fields, fields_ok := record["fields"].(map[string]interface{})
					if msg_ok && fields_ok && ts_ok {
						session, session_ok := fields["session"].(string)
						//Convert to s
						tsVal, err := ts.Int64()
						tsVal = tsVal / (1000*1000)
						if err != nil {
							logger.Error("ts convert fail", zap.Object("ts", ts))
						}
						if session_ok {
							logger.Debug(msg)
							switch msg {
							case txnBegin:
								txns[session] = &Txn{Session: session}
								txns[session].Session = session
								txns[session].Begin = tsVal
							case txnEnd:
								txn := txns[session]
								if txn != nil {
									txn.End = tsVal
									diff := (txn.End - txn.Begin)
									xput := float64(txn.Bytes) / float64(diff)
									if doStat {
										render(
											"txn",
											Float64("throughput", xput),
											String("session", session),
											String("counter", txn.Type),
											Int64("latency", txn.End-txn.Begin),
										)
									}
									gstats.Up.Diff += diff
									gstats.Up.Count += int64(1)
									gstats.Down.Diff += diff
									gstats.Down.Count += int64(1)
								} else {
									logger.Error("end without begin", zap.String("session", session))
								}
							case txnDown, txnUp:
								bytes, bytes_ok := fields["bytes"].(json.Number)
								if bytes_ok {
									bytesVal, err := bytes.Int64()
									if err != nil {
										logger.Error("bytes convert fail", zap.Object("bytes", bytes))
									}
									txn := txns[session]
									if txn != nil {
										switch msg {
										case txnUp:
											txn.Type = txnUp
											txn.Bytes += bytesVal
											gstats.Up.Bytes += bytesVal
										case txnDown:
											txn.Type = txnDown
											txn.Bytes += bytesVal
											gstats.Down.Bytes += bytesVal
										}
									} else {
										logger.Error("count without begin", zap.String("session", session))
									}
								}
							}
						}
					}
				}
			}
		}
	}
	if doGstat {
		renderGStat(&gstats.Up, "gstat up")
		renderGStat(&gstats.Down, "gstat down")
	}
}

func renderGStat(g *GStat, nm string) {
	xput := (float64(g.Bytes) / float64(g.Diff)) 
	latency := float64(g.Diff) / float64(g.Count)
	bytes := float64(g.Bytes) / float64(g.Count)

	render(
		nm,
		String("counter", nm),
		Float64("throughput", xput),
		Float64("latency", latency),
		Float64("bytes", bytes),
	)
}
