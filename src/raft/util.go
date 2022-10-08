package raft

import (
	"encoding/json"
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


func convertJson(v interface{}) string {
	if data, err := json.Marshal(v); err != nil || v == nil {
		log.Fatalf("json Unmarshal err:%v", err)
	}else{
		return string(data)
	}

	return ""
}