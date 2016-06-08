# collect

Given logs generated with uber-go zap, and piped in via stdin, and some naming conventions on events,
generate statistical logs from the incoming events.


The collect is a stdin program that takes a json argument from bash.
This is the ./runit script:
```
#!/bin/bash

go build && ( 
cat data.json | go run collect.go -command='{
  "echo":false,
  "gstat":true,
  "stat":false,
  "selected":[
    "msg",
    "session",
    "counter",
    "throughput",
    "latency"
  ]
}'
)
```

Data input looks like uber-go/zap json serializer output.  It will optionally deal with a token in front, as comes with docker-compose:
```
docker_odrive_1 {"msg":"txn begin","level":"info","ts":100,"fields":{"session":"6"}}
docker_odrive_1 {"msg":"txn begin","level":"info","ts":2013,"fields":{"session":"43"}}
docker_odrive_1 {"msg":"txn up","level":"info","ts":3023,"fields":{"session":"43","bytes":104813}}
docker_odrive_1 {"msg":"txn begin","level":"info","ts":5000,"fields":{"session":"42"}}
docker_odrive_1 {"msg":"txn down","level":"info","ts":5300,"fields":{"session":"42","bytes":5123}}
docker_odrive_1 {"msg":"txn end","level":"info","ts":10000,"fields":{"session":"42"}}
docker_odrive_1 {"msg":"txn end","level":"info","ts":50324,"fields":{"session":"43"}}
docker_odrive_1 {"msg":"txn up","level":"info","ts":90300,"fields":{"session":"6","bytes":3248923}}
docker_odrive_1 {"msg":"txn end","level":"info","ts":100324,"fields":{"session":"6"}}
```

Executing ./runit will produce output according to how the script was configured:
```	
{"msg"="gstat","counter"="txn up","throughput"="22.578759","latency"="74267.500000"}
{"msg"="gstat","counter"="txn down","throughput"="1.024600","latency"="5000.000000"}
```
