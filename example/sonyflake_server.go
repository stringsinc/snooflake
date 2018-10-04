package main

import (
	"encoding/json"
	"net/http"

	"awsutil"
	"snooflake"
)

var sf *snooflake.Snooflake

func init() {
	var st snooflake.Settings
	st.MachineID = awsutil.AmazonEC2MachineID
	sf = snooflake.NewSnooflake(st)
	if sf == nil {
		panic("snooflake not created")
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	id, err := sf.NextID()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := json.Marshal(snooflake.Decompose(id))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header()["Content-Type"] = []string{"application/json; charset=utf-8"}
	w.Write(body)
}

func main() {
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}
