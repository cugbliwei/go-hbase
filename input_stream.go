package hbase

import (
	"encoding/binary"
	"io"
	"net"

	pb "github.com/golang/protobuf/proto"
)

type inputStream struct {
	src net.Conn
}

func newInputStream(rdr net.Conn) *inputStream {
	return &inputStream{
		src: rdr,
	}
}

func (in *inputStream) readInt32() (int32, error) {
	var n int32
	err := binary.Read(in.src, byte_order, &n)
	return n, err
}

func (in *inputStream) readN(n int32) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(in.src, b)
	return b, err
}

func (in *inputStream) processData() [][]byte {
	// Read the number of bytes expected
	nBytesExpecting, err := in.readInt32()

	if err != nil && err == io.EOF {
		panic("Unexpected closed socket")
	}

	if nBytesExpecting > 0 {
		buf, err := in.readN(nBytesExpecting)

		if err != nil && err == io.EOF {
			panic("Unexpected closed socket")
		}

		payloads := in.processMessage(buf)

		if len(payloads) > 0 {
			return payloads
		}
	}

	return nil
}

func (in *inputStream) processMessage(msg []byte) [][]byte {
	buf := pb.NewBuffer(msg)
	payloads := make([][]byte, 0)

	for {
		hbytes, err := buf.DecodeRawBytes(true)
		if err != nil {
			break
		}

		payloads = append(payloads, hbytes)
	}

	return payloads
}
