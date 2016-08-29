// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"net"
)

type client struct {
	// TODO: implement this!
	conn net.Conn
	connId int
	msgReceivedChan chan Message //收到消息后，不会直接处理，而是写入chan，在goroutine中处理
	msgReceivedNotAck map[int]Message //only save msg that type = MsgData，maxlen = windowSize
	msgWritten map[int]Message //保存已经发送，但未收到ack的msg，所以msg类型只能是MsgConnect或者MsgData，收到ack后从map中删除
	msgToWriteChan chan Message //当调用Write时，不会真的将msg发送，而是写入chan，在goroutine中真正发送
	epochLimit int
	epochMillis int
	windowSize int

}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	return nil, errors.New("not yet implemented")
}

func (c *client) ConnID() int {
	return -1
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
