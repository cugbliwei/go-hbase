package hbase

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/cugbliwei/dlog"
	"github.com/cugbliwei/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
)

type Client struct {
	zkClient         *zk.Conn
	zkHosts          []string
	zkRoot           string
	zkRootRegionPath string
	user             string

	servers               map[string]*connection
	cachedRegionLocations map[string]map[string]*regionInfo

	maxRetries int

	prefetched map[string]bool

	rootServer   *proto.ServerName
	masterServer *proto.ServerName
}

type silentLogger struct{}

func (silentLogger) Printf(format string, a ...interface{}) {}

func init() {
	zk.DefaultLogger = &silentLogger{}
}

func NewClient(zkHosts []string, zkRoot, user string) *Client {
	cl := &Client{
		zkHosts:          zkHosts,
		zkRoot:           zkRoot,
		zkRootRegionPath: "/meta-region-server",
		user:             user,

		servers:               make(map[string]*connection),
		cachedRegionLocations: make(map[string]map[string]*regionInfo),
		prefetched:            make(map[string]bool),
		maxRetries:            max_action_retries,
	}

	cl.initZk()

	return cl
}

func (c *Client) initZk() {
	zkclient, _, err := zk.Connect(c.zkHosts, time.Second*30)
	if err != nil {
		dlog.Warn("zk connect error: %v", err)
		return
	}

	c.zkClient = zkclient

	res, _, _, err := c.zkClient.GetW(c.zkRoot + c.zkRootRegionPath)

	if err != nil {
		dlog.Warn("zk getw zkRootRegionPath error: %v", err)
		return
	}

	c.rootServer = c.decodeMeta(res)
	c.getRegionConnection(c.getServerName(c.rootServer))

	res, _, _, err = c.zkClient.GetW(c.zkRoot + "/master")

	if err != nil {
		dlog.Warn("zk getw zkRoot error: %v", err)
		return
	}

	c.masterServer = c.decodeMeta(res)
}

func (c *Client) decodeMeta(data []byte) *proto.ServerName {
	if data[0] != magic {
		return nil
	}

	var n int32
	binary.Read(bytes.NewBuffer(data[1:]), byte_order, &n)

	dataOffset := magic_size + id_length_size + int(n)

	data = data[(dataOffset + 4):]

	var mrs proto.MetaRegionServer
	err := pb.Unmarshal(data, &mrs)
	if err != nil {
		panic(err)
	}

	return mrs.GetServer()
}

func (c *Client) getServerName(server *proto.ServerName) string {
	return fmt.Sprintf("%s:%d", server.GetHostName(), server.GetPort())
}

func (c *Client) getRegionConnection(server string) *connection {
	if s, ok := c.servers[server]; ok {
		return s
	}

	conn, err := newConnection(server, c.user, false)
	if err != nil {
		panic(err)
	}

	c.servers[server] = conn

	return conn
}

func (c *Client) getMasterConnection() *connection {
	server := c.getServerName(c.masterServer)
	if s, ok := c.servers[server]; ok {
		return s
	}

	conn, err := newConnection(server, c.user, true)
	if err != nil {
		panic(err)
	}

	c.servers[server] = conn

	return conn
}

func (c *Client) adminAction(req pb.Message) chan pb.Message {
	conn := c.getMasterConnection()
	cl := newCall(req)

	err := conn.call(cl)

	if err != nil {
		panic(err)
	}

	return cl.responseCh
}

func (c *Client) action(table, row []byte, action action, useCache bool, retries int) chan pb.Message {
	region := c.locateRegion(table, row, useCache)
	conn := c.getRegionConnection(region.server)

	regionSpecifier := &proto.RegionSpecifier{
		Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte(region.name),
	}

	var cl *call = nil
	switch a := action.(type) {
	case *Get:
		cl = newCall(&proto.GetRequest{
			Region: regionSpecifier,
			Get:    a.toProto().(*proto.Get),
		})
	case *Put, *Delete:
		cl = newCall(&proto.MutateRequest{
			Region:   regionSpecifier,
			Mutation: a.toProto().(*proto.MutationProto),
		})
	}

	result := make(chan pb.Message)

	go func() {
		r := <-cl.responseCh

		switch r.(type) {
		case *exception:
			if retries <= c.maxRetries {
				// retry action
				dlog.Info("exception retrying action rowkey: %s for the %d time", string(row), retries+1)
				newr := c.action(table, row, action, false, retries+1)
				result <- <-newr
			} else {
				result <- r
			}
			return
		default:
			result <- r
		}
	}()

	if cl != nil {
		err := conn.call(cl)

		if err != nil {
			dlog.Warn("Error return while attempting call [err=%#v]", err)
			// purge dead server
			delete(c.servers, region.server)

			if retries <= c.maxRetries {
				// retry action
				dlog.Info("call warning retrying action rowkey: %s for the %d time", string(row), retries+1)
				c.action(table, row, action, false, retries+1)
			}
		}
	}

	return result
}

type multiaction struct {
	row    []byte
	action action
}

func (c *Client) multiaction(table []byte, actions []multiaction, useCache bool, retries int) chan pb.Message {
	actionsByServer := make(map[string]map[string][]multiaction)

	for _, action := range actions {
		region := c.locateRegion(table, action.row, useCache)

		if _, ok := actionsByServer[region.server]; !ok {
			actionsByServer[region.server] = make(map[string][]multiaction)
		}

		if _, ok := actionsByServer[region.server][region.name]; ok {
			actionsByServer[region.server][region.name] = append(actionsByServer[region.server][region.name], action)
		} else {
			actionsByServer[region.server][region.name] = []multiaction{action}
		}
	}

	chs := make([]chan pb.Message, 0)

	for server, as := range actionsByServer {
		region_actions := make([]*proto.RegionAction, len(as))

		i := 0
		for region, acts := range as {
			racts := make([]*proto.Action, len(acts))
			for j, act := range acts {
				racts[j] = &proto.Action{
					Index: pb.Uint32(uint32(j)),
				}

				switch a := act.action.(type) {
				case *Get:
					racts[j].Get = a.toProto().(*proto.Get)
				case *Put, *Delete:
					racts[j].Mutation = a.toProto().(*proto.MutationProto)
				}
			}

			region_actions[i] = &proto.RegionAction{
				Region: &proto.RegionSpecifier{
					Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
					Value: []byte(region),
				},
				Action: racts,
			}

			i++
		}

		req := &proto.MultiRequest{
			RegionAction: region_actions,
		}

		cl := newCall(req)

		result := make(chan pb.Message)

		go func(actionsByServer map[string]map[string][]multiaction, server string) {
			r := <-cl.responseCh

			switch r.(type) {
			case *exception:
				actions := make([]multiaction, 0)
				for _, acts := range actionsByServer[server] {
					actions = append(actions, acts...)
				}
				newr := c.multiaction(table, actions, false, retries+1)

				for x := range newr {
					result <- x
				}
				return
			default:
				result <- r
			}

			close(result)
		}(actionsByServer, server)

		conn := c.getRegionConnection(server)
		err := conn.call(cl)

		if err != nil {
			delete(c.servers, server)
			cl.complete(err, nil)
		}

		chs = append(chs, result)
	}

	return merge(chs...)
}

func (c *Client) locateRegion(table, row []byte, useCache bool) *regionInfo {
	metaRegion := &regionInfo{
		startKey: []byte{},
		endKey:   []byte{},
		name:     string(meta_region_name),
		server:   c.getServerName(c.rootServer),
	}

	if bytes.Equal(table, meta_table_name) {
		return metaRegion
	}

	c.prefetchRegionCache(table)

	if r := c.getCachedLocation(table, row); r != nil && useCache {
		return r
	}

	conn := c.getRegionConnection(metaRegion.server)

	regionRow := c.createRegionName(table, row, "", true)

	call := newCall(&proto.GetRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: meta_region_name,
		},
		Get: &proto.Get{
			Row: regionRow,
			Column: []*proto.Column{&proto.Column{
				Family: []byte("info"),
			}},
			ClosestRowBefore: pb.Bool(true),
		},
	})

	conn.call(call)

	response := <-call.responseCh

	switch r := response.(type) {
	case *proto.GetResponse:
		rr := newResultRow(r.GetResult())
		if region := c.parseRegion(rr); region != nil {
			c.cacheLocation(table, region)
			return region
		}
	}

	return nil
}

func (c *Client) createRegionName(table, startKey []byte, id string, newFormat bool) []byte {
	if len(startKey) == 0 {
		startKey = make([]byte, 1)
	}

	b := bytes.Join([][]byte{table, startKey, []byte(id)}, []byte(","))

	if newFormat {
		m := md5.Sum(b)
		mhex := []byte(hex.EncodeToString(m[:]))
		b = append(bytes.Join([][]byte{b, mhex}, []byte(".")), []byte(".")...)
	}
	return b
}

func (c *Client) prefetchRegionCache(table []byte) {
	if bytes.Equal(table, meta_table_name) {
		return
	}

	if v, ok := c.prefetched[string(table)]; ok && v {
		return
	}

	startRow := table
	stopRow := incrementByteString(table, len(table)-1)

	scan := newScan(meta_table_name, c)

	scan.StartRow = startRow
	scan.StopRow = stopRow

	scan.Map(func(r *ResultRow) {
		region := c.parseRegion(r)
		if region != nil {
			c.cacheLocation(table, region)
		}
	})

	c.prefetched[string(table)] = true
}

func (c *Client) parseRegion(rr *ResultRow) *regionInfo {
	if regionInfoCol, ok := rr.Columns["info:regioninfo"]; ok {
		offset := strings.Index(regionInfoCol.Value.String(), "PBUF") + 4
		regionInfoBytes := regionInfoCol.Value[offset:]

		var info proto.RegionInfo
		err := pb.Unmarshal(regionInfoBytes, &info)

		if err != nil {
			dlog.Error("Unable to parse region location: %#v", err)
		}

		return &regionInfo{
			server:         rr.Columns["info:server"].Value.String(),
			startKey:       info.GetStartKey(),
			endKey:         info.GetEndKey(),
			name:           rr.Row.String(),
			tableNamespace: string(info.GetTableName().GetNamespace()),
			tableName:      string(info.GetTableName().GetQualifier()),
			ts:             rr.Columns["info:server"].Timestamp.String(),
		}
	}

	dlog.Error("Unable to parse region location (no regioninfo column): %#v", rr)

	return nil
}

func (c *Client) cacheLocation(table []byte, region *regionInfo) {
	tablestr := string(table)
	if _, ok := c.cachedRegionLocations[tablestr]; !ok {
		c.cachedRegionLocations[tablestr] = make(map[string]*regionInfo)
	}

	c.cachedRegionLocations[tablestr][region.name] = region
}

func (c *Client) getCachedLocation(table, row []byte) *regionInfo {
	tablestr := string(table)

	if regions, ok := c.cachedRegionLocations[tablestr]; ok {
		for _, region := range regions {
			if (len(region.endKey) == 0 ||
				bytes.Compare(row, region.endKey) < 0) &&
				(len(region.startKey) == 0 ||
					bytes.Compare(row, region.startKey) >= 0) {

				return region
			}
		}
	}

	return nil
}
