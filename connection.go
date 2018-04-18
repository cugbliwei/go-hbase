package hbase

import (
	"fmt"
	"net"

	"github.com/cugbliwei/go-hbase/proto"
	pb "github.com/golang/protobuf/proto"
)

type connection struct {
	connstr string
	user    string

	id   int
	name string

	socket net.Conn
	in     *inputStream

	calls  map[int]*call
	callId *atomicCounter

	isMaster bool
}

var connectionIds *atomicCounter = newAtomicCounter()

func newConnection(connstr, user string, isMaster bool) (*connection, error) {
	id := connectionIds.IncrAndGet()

	socket, err := net.Dial("tcp", connstr)

	if err != nil {
		return nil, err
	}

	c := &connection{
		connstr: connstr,
		user:    user,

		id:   id,
		name: fmt.Sprintf("connection(%s) id: %d", connstr, id),

		socket: socket,
		in:     newInputStream(socket),

		calls:  make(map[int]*call),
		callId: newAtomicCounter(),

		isMaster: isMaster,
	}

	err = c.init()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *connection) init() error {

	err := c.writeHead()
	if err != nil {
		return err
	}

	err = c.writeConnectionHeader()
	if err != nil {
		return err
	}

	go c.processMessages()

	return nil
}

func (c *connection) writeHead() error {
	buf := newOutputBuffer()
	buf.Write(hbase_header_bytes)
	buf.WriteByte(0)
	buf.WriteByte(80)

	_, err := c.socket.Write(buf.Bytes())
	return err
}

func (c *connection) writeConnectionHeader() error {
	buf := newOutputBuffer()
	service := pb.String("ClientService")
	if c.isMaster {
		service = pb.String("MasterService")
	}

	err := buf.WritePBMessage(&proto.ConnectionHeader{
		UserInfo: &proto.UserInformation{
			EffectiveUser: pb.String(c.user),
		},
		ServiceName: service,
	})
	if err != nil {
		return err
	}

	err = buf.PrependSize()
	if err != nil {
		return err
	}

	_, err = c.socket.Write(buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (c *connection) call(request *call) error {
	id := c.callId.IncrAndGet()
	rh := &proto.RequestHeader{
		CallId:       pb.Uint32(uint32(id)),
		MethodName:   pb.String(request.methodName),
		RequestParam: pb.Bool(true),
	}

	request.setid(uint32(id))

	bfrh := newOutputBuffer()
	err := bfrh.WritePBMessage(rh)
	if err != nil {
		panic(err)
	}

	bfr := newOutputBuffer()
	err = bfr.WritePBMessage(request.request)
	if err != nil {
		panic(err)
	}

	buf := newOutputBuffer()
	buf.writeDelimitedBuffers(bfrh, bfr)

	c.calls[id] = request
	n, err := c.socket.Write(buf.Bytes())

	if err != nil {
		return err
	}

	if n != len(buf.Bytes()) {
		return fmt.Errorf("Sent bytes not match number bytes [n=%d] [actual_n=%d]", n, len(buf.Bytes()))
	}

	return nil
}

func (c *connection) processMessages() {
	for {
		msgs := c.in.processData()
		if msgs == nil || len(msgs) == 0 || len(msgs[0]) == 0 {
			continue
		}

		var rh proto.ResponseHeader
		err := pb.Unmarshal(msgs[0], &rh)
		if err != nil {
			panic(err)
		}

		callId := rh.GetCallId()
		call, ok := c.calls[int(callId)]
		if !ok {
			panic(fmt.Errorf("Invalid call id: %d", callId))
		}

		delete(c.calls, int(callId))

		exception := rh.GetException()
		if exception != nil {
			call.complete(fmt.Errorf("Exception returned: %s\n%s", exception.GetExceptionClassName(), exception.GetStackTrace()), nil)
		} else if len(msgs) == 2 {
			call.complete(nil, msgs[1])
		}
	}
}
