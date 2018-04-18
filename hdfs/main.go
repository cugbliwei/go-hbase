package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/cugbliwei/dlog"
)

var health bool

func init() {
	health = true
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

func PutFile(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			dlog.Error("http put cell submit error: %v", r)
			debug.PrintStack()
		}
	}()

	r.ParseForm()
	f, _, err := r.FormFile("file")
	if err != nil {
		dlog.Error("receive file: %v, length: %d, url: %s", err, r.ContentLength, r.RequestURI)
		fmt.Fprint(w, "false")
		return
	}

	filename := r.FormValue("filename")
	date := r.FormValue("date")

	if len(filename) == 0 || len(date) == 0 {
		dlog.Error("the one of params is empty")
		fmt.Fprint(w, "false")
		return
	}

	body, err := ioutil.ReadAll(f)
	if err != nil {
		dlog.Error("read file: %v, length: %d, url: %s", err, r.ContentLength, r.RequestURI)
		fmt.Fprint(w, "false")
		return
	}
	f.Close()

	rd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rint := rd.Intn(10000000)
	if strings.HasSuffix(filename, ".log") {
		filename = filename[0:len(filename)-4] + "_" + strconv.Itoa(rint) + ".log"
	}

	fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		dlog.Error("open file error: %v", err)
		return
	}
	fp.Write(body)
	fp.Close()

	hdfsMkdir := "hdfs dfs -mkdir -p /user/yisou/crawler/crawler_log/" + date
	hdfsPut := "hdfs dfs -put " + filename + " /user/yisou/crawler/crawler_log/" + date + "/" + filename

	out := shell(hdfsMkdir)
	output := shell(hdfsPut)
	if len(out) > 0 || len(output) > 0 {
		dlog.Warn("hdfsMkdir: %s, hdfsPut: %s", out, output)
	} else {
		dlog.Info("succeed put file: %s to hdfs path: %s", filename, "/user/yisou/crawler/crawler_log/"+date+"/"+filename)
	}

	os.Remove(filename)
	fmt.Fprint(w, "true")
}

func shell(s string) string {
	cmd := exec.Command("/bin/sh", "-c", s)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return out.String() + stderr.String()
	}
	return out.String()
}

func main() {

	http.HandleFunc("/start", HandleStart)
	http.HandleFunc("/shutdown", HandleShutdown)
	http.HandleFunc("/health", HandleHealth)
	http.HandleFunc("/hdfs/put", PutFile)

	l, e := net.Listen("tcp", ":8088")
	if e != nil {
		dlog.Fatal("listen error: %v", e)
	}
	http.Serve(l, nil)
}
