package util

import (
	"bytes"
	"errors"
	"strings"
	"sync"

	"github.com/cugbliwei/dlog"
	hbase "github.com/cugbliwei/go-hbase"
)

const (
	ZKHOST = "host:port,host:port"
	ZKROOT = "/hbase"
	ZKUSER = "xxxx"
)

type HbaseClient struct {
	client *hbase.Client
	lock   *sync.Mutex
}

func NewHbaseClient() *HbaseClient {
	return &HbaseClient{
		client: hbase.NewClient(strings.Split(ZKHOST, ","), ZKROOT, ZKUSER),
		lock:   &sync.Mutex{},
	}
}

func (self *HbaseClient) Put(table, rowkey, family, column, value string) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	put := hbase.CreateNewPut([]byte(rowkey))
	put.AddStringValue(family, column, value)
	res, err := self.client.Put(table, put)
	if err != nil {
		return err
	}

	if !res {
		return errors.New("no put results")
	}

	return nil
}

func (self *HbaseClient) Puts(res []map[string]string) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	tablePuts := make(map[string][]*hbase.Put, 1)
	for _, re := range res {
		table, _ := re["table"]
		rowkey, _ := re["rowkey"]
		family, _ := re["family"]
		column, _ := re["column"]
		value, _ := re["value"]

		if len(table) == 0 || len(rowkey) == 0 || len(family) == 0 || len(column) == 0 || len(value) == 0 {
			dlog.Error("the one of params is empty")
			continue
		}

		put := hbase.CreateNewPut([]byte(rowkey))
		put.AddStringValue(family, column, value)
		ps, ok := tablePuts[table]
		if ok {
			ps = append(ps, put)
			tablePuts[table] = ps
		} else {
			var puts []*hbase.Put
			puts = append(puts, put)
			tablePuts[table] = puts
		}
	}

	for table, puts := range tablePuts {
		result, err := self.client.Puts(table, puts)
		if err != nil {
			return err
		}

		if !result {
			return errors.New("no put results")
		}
	}

	return nil
}

func (self *HbaseClient) Get(table, rowkey, family, column string) (string, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	get := hbase.CreateNewGet([]byte(rowkey))
	result, err := self.client.Get(table, get)
	if err != nil {
		return "", err
	}

	if result == nil {
		return "", errors.New("hbase result in nil")
	}

	if result.Row == nil {
		return "", nil
	}

	if !bytes.Equal(result.Row, []byte(rowkey)) {
		return "", errors.New("result rowkey: " + string(result.Row) + " is not: " + rowkey)
	}

	value := result.Columns[family+":"+column].Value
	return string(value), nil
}
