package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"runtime/debug"

	"github.com/cugbliwei/dlog"
	"github.com/cugbliwei/go-hbase/util"
)

var health bool
var hbaseClient *util.HbaseClient

func init() {
	health = true
	hbaseClient = util.NewHbaseClient()
}

func HandleHealth(w http.ResponseWriter, req *http.Request) {
	if health {
		fmt.Fprint(w, "yes")
	} else {
		http.Error(w, "no", http.StatusNotFound)
	}
}

func HandleStart(w http.ResponseWriter, req *http.Request) {
	health = true
	fmt.Fprint(w, "ok")
}

func HandleShutdown(w http.ResponseWriter, req *http.Request) {
	health = false
	fmt.Fprint(w, "ok")
}

func PutCell(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			dlog.Error("http put cell submit error: %v", r)
			debug.PrintStack()
		}
	}()

	r.ParseForm()
	table := r.FormValue("table")
	rowkey := r.FormValue("rowkey")
	family := r.FormValue("family")
	column := r.FormValue("column")
	value := r.FormValue("value")

	if len(table) == 0 || len(rowkey) == 0 || len(family) == 0 || len(column) == 0 || len(value) == 0 {
		dlog.Error("the one of params is empty")
		fmt.Fprint(w, "false")
		return
	}

	if err := hbaseClient.Put(table, rowkey, family, column, value); err != nil {
		dlog.Error("put data to hbase error: %v", err)
		fmt.Fprint(w, "false")
		return
	}

	dlog.Info("succeed put rowkey: %s to hbase", rowkey)
	fmt.Fprint(w, "true")
}

func PutFileCell(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			dlog.Error("http put batch cell submit error: %v", r)
			debug.PrintStack()
		}
	}()
	r.ParseMultipartForm(32 << 20)
	file, _, _ := r.FormFile("uploadfile")
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		dlog.Error("err: %v", err)
		fmt.Fprint(w, "false")
		return
	}
	var exdata map[string]string
	err = json.Unmarshal(data, &exdata)
	if err != nil {
		dlog.Error("err: %v", err)
		fmt.Fprint(w, "false")
		return
	}
	table := exdata["table"]
	rowkey := exdata["rowkey"]
	family := exdata["family"]
	column := exdata["column"]
	value := exdata["value"]

	if len(table) == 0 || len(rowkey) == 0 || len(family) == 0 || len(column) == 0 || len(value) == 0 {
		dlog.Error("the one of params is empty")
		fmt.Fprint(w, "false")
		return
	}

	if err := hbaseClient.Put(table, rowkey, family, column, value); err != nil {
		dlog.Error("put data to hbase error: %v", err)
		fmt.Fprint(w, "false")
		return
	}

	dlog.Info("succeed put rowkey: %s to hbase", rowkey)
	fmt.Fprint(w, "true")
}

func PutBatchCell(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			dlog.Error("http put batch cell submit error: %v", r)
			debug.PrintStack()
		}
	}()

	r.ParseForm()
	batch := r.FormValue("batch")
	var res []map[string]string
	if err := json.Unmarshal([]byte(batch), &res); err != nil {
		dlog.Warn("err: %v", err)
		fmt.Fprint(w, "false")
		return
	}

	if err := hbaseClient.Puts(res); err != nil {
		dlog.Error("puts batch data to hbase error: %v", err)
		fmt.Fprint(w, "false")
		return
	}

	dlog.Info("succeed puts batch to hbase")
	fmt.Fprint(w, "true")
}

func GetCell(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			dlog.Error("http get cell submit error: %v", r)
			debug.PrintStack()
		}
	}()

	r.ParseForm()
	table := r.FormValue("table")
	rowkey := r.FormValue("rowkey")
	family := r.FormValue("family")
	column := r.FormValue("column")

	w.Header().Set("Content-Type", "application/json")
	resp := make(map[string]string, 3)
	resp["status"] = "true"
	resp["data"] = ""
	resp["msg"] = ""

	if len(table) == 0 || len(rowkey) == 0 || len(family) == 0 || len(column) == 0 {
		dlog.Error("the one of the params is empty")
		resp["status"] = "false"
		resp["msg"] = "the one of the params is empty"
		w.Write(mapToBytes(resp))
		return
	}

	value, err := hbaseClient.Get(table, rowkey, family, column)
	if err != nil {
		dlog.Warn("get rowkey: %s from hbase error: %v", rowkey, err)
		resp["status"] = "false"
		resp["msg"] = err.Error()
	} else {
		dlog.Info("succeed get rowkey: %s from hbase", rowkey)
		resp["data"] = value
		resp["msg"] = "succeed get data from hbase"
	}

	w.Write(mapToBytes(resp))
}

func mapToBytes(m map[string]string) []byte {
	b, err := json.Marshal(m)
	if err != nil {
		return []byte{}
	}
	return b
}

func main() {
	runtime.GOMAXPROCS(4)

	http.HandleFunc("/start", HandleStart)
	http.HandleFunc("/shutdown", HandleShutdown)
	http.HandleFunc("/health", HandleHealth)
	http.HandleFunc("/hbase/put", PutCell)
	http.HandleFunc("/hbase/putfile", PutFileCell)
	http.HandleFunc("/hbase/puts", PutBatchCell)
	http.HandleFunc("/hbase/get", GetCell)

	l, e := net.Listen("tcp", ":8089")
	if e != nil {
		dlog.Fatal("listen error: %v", e)
	}
	http.Serve(l, nil)
}
