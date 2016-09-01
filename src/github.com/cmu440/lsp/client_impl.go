// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"log"
	"os"
	"time"
)

var (
	clientLogFile, _ = os.OpenFile("client_log.txt", os.O_RDWR|os.O_TRUNC, 0)
)

type client struct {
	connID                        int
	nextWriteIndex                int
	conn                          *lspnet.UDPConn
	addr                          *lspnet.UDPAddr
	windowSize                    int
	sendWindow                    map[int]bool
	recvWindow                    map[int]bool
	sendQueue                     *list.List
	writeToServerChan             chan []byte
	clientWriteMethodChan         chan []byte
	recvFromServerChan            chan Message
	recvMessageQueue              *list.List
	haveSuccessSendIndex          int
	haveBeenReadIndex             int
	successReadIndex              int
	epochChan                     chan struct{}
	clientCloseMethodChanResponse chan bool
	clientGotingToClose           bool
	clientCloseMethodChan         chan struct{}
	epochTimes                    int
	epochLimit                    int
	epochMillis                   int
	successConnect                chan bool
	alreadyShutDown               bool
	clientReadMethodBufferChan    chan Message
	logger                        *log.Logger
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
	udpaddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	/* If laddr is not nil, it is used as the local address for the connection.*/
	udpconn, err := lspnet.DialUDP("udp", nil, udpaddr)
	if err != nil {
		return nil, err
	}
	c := new(client)
	c.addr = udpaddr
	c.alreadyShutDown = false
	c.clientCloseMethodChan = make(chan struct{})
	c.clientCloseMethodChanResponse = make(chan bool)
	c.clientGotingToClose = false
	/*	Sending to a buffered channel will not block unless the buffer is full (the capacity is completely
		used), and reading from a buffered channel will not block unless the buffer is empty.*/
	c.clientReadMethodBufferChan = make(chan Message, 10000)
	c.clientWriteMethodChan = make(chan []byte)
	c.conn = udpconn
	c.epochChan = make(chan struct{})
	c.epochLimit = params.EpochLimit
	c.epochMillis = params.EpochMillis
	c.epochTimes = 0
	c.haveBeenReadIndex = 0
	c.haveSuccessSendIndex = 0
	c.nextWriteIndex = 1
	c.recvFromServerChan = make(chan Message)
	c.recvMessageQueue = list.New()
	c.recvWindow = make(map[int]bool)
	c.sendQueue = list.New()
	c.sendWindow = make(map[int]bool)
	c.successConnect = make(chan bool)
	c.successReadIndex = 0
	c.windowSize = params.WindowSize
	c.writeToServerChan = make(chan []byte)
	c.logger = log.New(clientLogFile, "Client-- ", log.Lmicroseconds|log.Lshortfile)

	go c.ReadFromServer()
	go c.FireEpoch()
	go c.WriteToServer()
	go c.EventHandler()

	return c.Launch()
}

func (c *client) Launch() (Client, error) {
	mes := NewConnect()

	buf, _ := json.Marshal(mes)
	c.logger.Printf("ID:%d Launch::send message:%s\n", c.ConnID(), buf)

	c.writeToServerChan <- buf
	for {
		select {
		case <-c.successConnect:
			c.logger.Printf("ID:%d Launch::new client is created\n", c.connID)
			return c, nil
		}
	}
}

func (c *client) EventHandler() {
	for {
		select {
		case buf := <-c.clientWriteMethodChan: //[]byte

			mes := NewData(c.connID, c.nextWriteIndex, buf)
			c.sendQueue.PushBack(*mes)
			c.nextWriteIndex++

			if len(c.sendWindow) < c.windowSize {
				buf, _ := json.Marshal(mes)
				c.writeToServerChan <- buf
				c.logger.Printf("ID:%d EventHandler:: send message:%s\n", c.connID, string(buf))
				c.sendWindow[mes.SeqNum] = false
			}
		case mes := <-c.recvFromServerChan: //message
			if mes.Type == MsgAck {

				if mes.SeqNum == 0 { //establish connection and receive connid
					{
						buf, _ := json.Marshal(mes)
						c.logger.Printf("ID:%d EventHandler:: recv message:%s\n", c.connID, string(buf))
					}
					c.connID = mes.ConnID
					c.successConnect <- true
				}

				if mes.SeqNum > c.haveSuccessSendIndex {
					c.sendWindow[mes.SeqNum] = true //have received ack
					{
						buf, _ := json.Marshal(mes)
						c.logger.Printf("ID:%d EventHandler:: recv message:%s\n", c.connID, string(buf))
					}
					i := c.haveSuccessSendIndex + 1
					for {
						val, isPresent := c.sendWindow[i]
						if !isPresent || !val {
							break
						}
						delete(c.sendWindow, i)
						c.sendQueue.Remove(c.sendQueue.Front())
						i++
					}
					c.haveSuccessSendIndex = i - 1
					i = c.haveSuccessSendIndex + len(c.sendWindow) + 1

					for e := c.sendQueue.Front(); i <= c.haveSuccessSendIndex+c.windowSize && e != nil; e = e.Next() {
						c.sendWindow[i] = false //have send but not received ack

						buf, _ := json.Marshal(e.Value.(Message))
						c.writeToServerChan <- buf
						i++
						c.logger.Printf("ID:%d EventHandler::send message %s\n", c.connID, string(buf))
					}
				}
			} else if mes.Type == MsgData {
				if _, ok := c.recvWindow[mes.SeqNum]; !ok {
					c.recvMessageQueue.PushBack(mes)
					c.recvWindow[mes.SeqNum] = true
					{
						buf, _ := json.Marshal(mes)
						c.logger.Printf("ID:%d EventHandler::recv message:%s\n", c.connID, string(buf))
					}
					if mes.SeqNum > c.successReadIndex+c.windowSize {
						for i := mes.SeqNum - c.windowSize; i > c.successReadIndex; i-- {
							for e := c.recvMessageQueue.Front(); e != nil; e = e.Next() {
								if e.Value.(Message).SeqNum == i {
									c.recvMessageQueue.Remove(e)
									delete(c.recvWindow, i)
									break
								}
							}
						}
						c.successReadIndex = mes.SeqNum - c.windowSize
					}
					var i int
					for i = c.haveBeenReadIndex + 1; i <= c.successReadIndex+c.windowSize && c.recvMessageQueue.Len() > 0; i++ {
						if _, ok := c.recvWindow[i]; !ok {
							break
						}
						flag := false
						for e := c.recvMessageQueue.Front(); e != nil; e = e.Next() {
							if e.Value.(Message).SeqNum == i {
								{
									buf, _ := json.Marshal(e.Value.(Message))
									c.logger.Printf("ID:%d EventHandler::read into buffer message:%s\n", c.connID, string(buf))
								}
								c.clientReadMethodBufferChan <- e.Value.(Message)
								c.recvMessageQueue.Remove(e)
								mes := NewAck(c.connID, e.Value.(Message).SeqNum)
								buf, _ := json.Marshal(mes)
								c.conn.Write(buf)
								flag = true
								c.logger.Printf("ID:%d EventHandler::send message:%s\n", c.connID, string(buf))
								break
							}
						}
						if !flag {
							break
						}
					}
					c.haveBeenReadIndex = i - 1
				}
			}
		case <-c.epochChan:
		/*
			If the client’s connection request has not yet been acknowledged
			by the server, then resend the connection request.
			If the connection request has been sent and acknowledged, but no
			data messages have been received, then send an acknowledgment with
			sequence number 0.
			For each data message that has been sent but not yet acknowledged,
			resend the data message.
			Resend an acknowledgment message for each of the last ω (or possibly fewer)
			distinct data messages that have been received.
		*/
			c.epochTimes++
			if c.alreadyShutDown {
				return
			} else if len(c.sendWindow) == 0 && c.sendQueue.Len() == 0 && c.clientGotingToClose {
				c.clientCloseMethodChanResponse <- true
				c.logger.Printf("ID:%d EventHandler::epoch closed the connection\n", c.connID)
				return
			} else {
				/*if nothing is received from the other end of an established
				connection over a total period of K · δ seconds, then the connection
				should be assumed lost.*/
				if c.epochTimes >= c.epochLimit {
					if c.clientGotingToClose {
						c.logger.Printf("ID:%d EventHandler:: epothTimes exceed\n", c.connID)
						c.clientCloseMethodChanResponse <- false
					} else { //server lost
						c.logger.Printf("ID:%d EventHandler:: epothTimes exceed server lost\n", c.connID)
						c.closeAllThing()
					}
					return
				} else {
					if c.connID == 0 {
						mes := NewConnect()
						buf, _ := json.Marshal(mes)
						c.logger.Printf("ID:%d EventHandler:: reconnection\n", c.connID)
						c.writeToServerChan <- buf
					} else {
						for i := c.successReadIndex + 1; i <= c.haveBeenReadIndex; i++ {
							ack := NewAck(c.connID, i)
							buf, _ := json.Marshal(*ack)
							c.logger.Printf("ID:%d EventHandler::resend ack message:%s\n", c.connID, string(buf))
							c.conn.Write(buf)
						}
						for i, ok := range c.sendWindow {
							if !ok {
								for e := c.sendQueue.Front(); e != nil; e = e.Next() {
									if e.Value.(Message).SeqNum == i {
										buf, _ := json.Marshal(e.Value.(Message))
										c.writeToServerChan <- buf
										c.logger.Printf("ID:%d EventHandler::resend message:%s\n", c.connID, buf)
										break
									}
								}
							}
						}
					}
				}
			}
		case <-c.clientCloseMethodChan:
			c.clientGotingToClose = true
		}
	}
}

func (c *client) FireEpoch() error {
	tick := time.Tick(time.Duration(c.epochMillis) * time.Millisecond)
	for {
		if c.alreadyShutDown {
			return nil
		}
		select {
		case <-tick:
			c.logger.Printf("ID:%d fire epoch\n", c.ConnID())
			c.epochChan <- struct{}{}
		}
	}
	return nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) WriteToServer() {
	for {
		select {
		case buf := <-c.writeToServerChan:
			c.logger.Printf("ID:%d WriteToServer::send message %s\n", c.connID, buf)
			c.conn.Write(buf)
		}
	}
}

func (c *client) ReadFromServer() error {
	var buf [1500]byte
	var mes Message
	for {
		if c.alreadyShutDown {
			return nil
		}
		n, err := c.conn.Read(buf[0:])
		if err != nil { //server crash
			c.logger.Printf("ID:%d ReadFromServer::error:%s\n", c.ConnID(), err.Error())
			c.clientCloseMethodChan <- struct{}{}
			return nil
		}
		json.Unmarshal(buf[0:n], &mes)
		c.recvFromServerChan <- mes
		c.logger.Printf("ID:%d ReadFromServer::receive message %s\n", c.ConnID(), string(buf[0:n]))
	}
}

/*You may assume that Read, Write, and Close will not be called after Close has been
called.*/
func (c *client) Read() ([]byte, error) {
	if c.alreadyShutDown {
		return nil, errors.New("the connection is terminate")
	}
	msg, ok := <-c.clientReadMethodBufferChan
	if ok {
		{
			buf, _ := json.Marshal(msg)
			c.logger.Printf("ID:%d Read::read message:%s\n", c.connID, string(buf))
		}
		return msg.Payload, nil

	} else {
		c.logger.Printf("ID:%d Read::connection terminate\n", c.ConnID())
		return nil, errors.New("the connection is terminate")
	}
}

func (c *client) Write(payload []byte) error {
	c.clientWriteMethodChan <- payload
	c.logger.Printf("ID:%d Write::send message:%s\n", c.connID, string(payload))
	return nil
}

func (c *client) Close() error {
	c.logger.Printf("ID:%d Close::method invoked\n", c.connID)
	c.clientCloseMethodChan <- struct{}{}
	select {
	case boo := <-c.clientCloseMethodChanResponse:
		if boo {
			c.closeAllThing()
			c.logger.Printf("ID:%d Close::closed finished\n", c.ConnID())
			return nil
		} else {
			//c.closeAllThing()
			return errors.New("the connection is closed before all messages are sent out")
		}
	}
	return nil
}

func (c *client) closeAllThing() {
	c.alreadyShutDown = true
	close(c.clientWriteMethodChan)
	c.conn.Close()
	close(c.clientReadMethodBufferChan)
	c.logger.Printf("ID:%d closeAllThing::close all thing\n", c.connID)
}
