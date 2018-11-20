package gocelery

import (
	jsonj "encoding/json"
	"gotest.tools/assert"
	"testing"
)

var msgJsonPretty = `{
	"body": "W1tdLCB7InkiOiAyODc4LCAieCI6IDU0NTZ9LCB7ImNob3JkIjogbnVsbCwgImNhbGxiYWNrcyI6IG51bGwsICJlcnJiYWNrcyI6IG51bGwsICJjaGFpbiI6IG51bGx9XQ==",
	"headers": {
		"origin": "gen4015@huangzhen-PC",
		"root_id": "f0e6eb81-179b-4881-89fb-674a83ee640f",
		"expires": null,
		"shadow": null,
		"id": "f0e6eb81-179b-4881-89fb-674a83ee640f",
		"kwargsrepr": "{'y': 2878, 'x': 5456}",
		"lang": "py",
		"retries": 0,
		"task": "worker.add_reflect",
		"group": null,
		"timelimit": [null, null],
		"parent_id": null,
		"argsrepr": "()",
		"eta": null
	},
	"content-type": "application/json",
	"properties": {
		"priority": 0,
		"body_encoding": "base64",
		"correlation_id": "f0e6eb81-179b-4881-89fb-674a83ee640f",
		"reply_to": "c2481f78-c447-3bf1-9803-2872caa519bd",
		"delivery_info": {
			"routing_key": "celery",
			"exchange": ""
		},
		"delivery_mode": 2,
		"delivery_tag": "6b2c261d-4eee-4331-abb8-557cad9e1763"
	},
	"content-encoding": "utf-8"
}`

func TestParser(t *testing.T) {
	var msgJson = `{"body": "W1tdLCB7InkiOiAyODc4LCAieCI6IDU0NTZ9LCB7ImNob3JkIjogbnVsbCwgImNhbGxiYWNrcyI6IG51bGwsICJlcnJiYWNrcyI6IG51bGwsICJjaGFpbiI6IG51bGx9XQ==", "headers": {"origin": "gen18066@huangzhen-PC", "root_id": "ed13b762-aadc-4451-b525-d8eb1ec3e8c3", "expires": null, "shadow": null, "id": "ed13b762-aadc-4451-b525-d8eb1ec3e8c3", "kwargsrepr": "{'y': 2878, 'x': 5456}", "lang": "py", "retries": 0, "task": "worker.add_reflect", "group": null, "timelimit": [null, null], "parent_id": null, "argsrepr": "()", "eta": null}, "content-type": "application/json", "properties": {"priority": 0, "body_encoding": "base64", "correlation_id": "ed13b762-aadc-4451-b525-d8eb1ec3e8c3", "reply_to": "2790276f-4aba-3e88-94af-154ed9df4a0f", "delivery_info": {"routing_key": "celery", "exchange": ""}, "delivery_mode": 2, "delivery_tag": "b7dfd826-df49-45b0-8cf7-20bd7c6611b0"}, "content-encoding": "utf-8"}`
	var message = &CeleryMessage{}
	var jsonbytes = []byte(msgJson)
	t.Logf("bytes: %s", jsonbytes)
	jsonj.Unmarshal(jsonbytes, message)
	t.Logf("body: %s", message.Body)
	deserializedMsg, err := jsonj.Marshal(message)
	if err != nil {
		t.Fatalf("parse failed: %s", err.Error())
	}
	// assert.Equal(t, msgJson, deserializedMsg)
	t.Logf("parsed: %s", deserializedMsg)
}

func TestDecodeBody(t *testing.T) {
	t.Logf("-------")
	// [[], {"y": 2878, "x": 5456}, {"chord": null, "callbacks": null, "errbacks": null, "chain": null}]
	var encodedStr = "W1tdLCB7InkiOiAyODc4LCAieCI6IDU0NTZ9LCB7ImNob3JkIjogbnVsbCwgImNhbGxiYWNrcyI6IG51bGwsICJlcnJiYWNrcyI6IG51bGwsICJjaGFpbiI6IG51bGx9XQ=="
	var body = DecodeBody(encodedStr)
	assert.Equal(t, body.Kwargs["x"], float64(5456))
	t.Logf("%v", body)
}
