// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"encoding/json"
	"fmt"
	"time"
	"container/list"
	"github.com/cmu440/lspnet"
	"strconv"
)

const MTU = 1500

type client struct {
	conn                *lspnet.UDPConn
	connId              int             //if connId = 0, then the client is not init successfully yet. the epoch routine will use this
	epochLimit          int             // params
	epochMillis         int
	windowSize          int
	//init
	initChan            chan bool
	shutdownChan	    chan bool
	//write
	nextSeqNum          int             // next msg seq num
	writeNotAckEarliest int
	msgWrittenMap       map[int]Message //has written to conn, but not receive ack, the epoch will read this and resend msg
	msgWrittenAckMap    map[int]bool    //when write a msg, will record false, when receive ack, record true
	msgToWriteCacheChan chan []byte     //when call Write, will mot write msg directly, but write to this chan, the eventHandlerRoutine will write data to msgToWriteChan
	msgToWriteQueue     *list.List      //if msg seqNum >= writeNotAckEarList + windowSize, then msg that from msgToWriteCacheChan will write this queue first, when receive ack, the queue will write msg to msgConnectChan
	msgConnectChan      chan Message    //the writeChan will read this chan and write to chan
	//read
	msgReceivedQueue     *list.List     //only save msg that type = MsgData
	msgToProcessChan     chan Message   //receive msg, not process
	msgReceivedChan      chan Message   //processed msg, call Read to read msg from this chan
	//close
	closeChan            chan bool      //use poison pills to close client
	//epoch
	epochCount           int
	epochSignalChan      chan bool
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
	cli := &client{
		connId: 0,
		epochLimit: params.EpochLimit,
		epochMillis: params.EpochMillis,
		windowSize: params.WindowSize,
		initChan: make(chan bool),
		shutdownChan: make(chan bool),
		nextSeqNum: 0,
		writeNotAckEarliest: 0,
		msgWrittenMap: make(map[int]Message),
		msgWrittenAckMap: make(map[int]bool),
		msgToWriteCacheChan: make(chan []byte),
		msgToWriteQueue: list.New(),
		msgConnectChan: make(chan Message, params.WindowSize),
		msgReceivedQueue: list.New(),
		msgToProcessChan: make(chan  Message),
		msgReceivedChan: make(chan Message, params.WindowSize),
		closeChan: make(chan(bool), 5),
		epochCount: 0,
		epochSignalChan: make(chan bool),
	}

	udpaddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	udpconn, err := lspnet.DialUDP("udp", nil, udpaddr)
	if err != nil {
		return nil, err
	}
	cli.conn = udpconn
	go cli.writeRoutine()
	go cli.readRoutine()
	go cli.epochRoutine()
	go cli.eventHandlerRoutine();
	msg := NewConnect();
	cli.msgConnectChan <- *msg
	select {
	case <- cli.initChan:
		fmt.Println("client init success")
		return Client(cli), nil;
	case <- cli.shutdownChan:
		fmt.Println("cli shut down")
		return nil, errors.New("client init failed")
	}
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
	select {
	case msg := <- c.msgReceivedChan:
		return msg.Payload, nil
	case <- c.closeChan:
		return nil, errors.New("client is closed")
	}
}

func (c *client) Write(payload []byte) error {
	c.msgToWriteCacheChan <- payload
	return nil
}

func (c *client) Close() error {
	for i := 0; i < 3; i++ {
		c.closeChan <- true
	}
	c.conn.Close()
	return nil;
}

func (c *client) writeRoutine()  {
	for {
		select {
		case msg := <-c.msgConnectChan:
			msgData, _ := json.Marshal(msg)
			fmt.Println("write msg =" + string(msgData))
			c.conn.Write(msgData)
		case <-c.closeChan:
			fmt.Println("write routine exit")
			return
		}
	}
}

func (c *client) readRoutine() {
	var buf [MTU]byte;
	var msg Message;
	for {
		n, err := c.conn.Read(buf[:])
		fmt.Println("read msg =" + string(buf[0:n]))
		if err != nil {
			fmt.Println("read failed, read routine exit")
			return
		}
		json.Unmarshal(buf[0:n], &msg);
		c.msgToProcessChan <- msg;
	}
}

func (c *client) epochRoutine() {
	tick := time.Tick(time.Duration(c.epochMillis) * time.Millisecond)
	for {
		select {
		case <-tick:
			c.epochSignalChan <- true
		case <-c.closeChan:
			fmt.Println("client epoch routing exit")
			return
		}
	}
}

func (c *client) eventHandlerRoutine() {
	for {
		select {
		case msg := <-c.msgToProcessChan:
			c.epochCount = 0;
			if msg.Type == MsgData {
				c.msgReceivedQueue.PushBack(msg)
				if c.msgReceivedQueue.Len() > c.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
					c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
				}
				ack := NewAck(c.connId, msg.SeqNum)
				c.msgConnectChan <- *ack
				c.msgReceivedChan <- msg
			} else if msg.Type == MsgAck {
				if msg.SeqNum == 0 {
					c.connId = msg.ConnID
					c.nextSeqNum++
					c.writeNotAckEarliest++
					c.initChan <- true
				} else {
					if msg.SeqNum == c.writeNotAckEarliest {
						oldEarliest := c.writeNotAckEarliest
						c.writeNotAckEarliest++
						c.msgWrittenAckMap[msg.SeqNum] = true
						delete(c.msgWrittenMap, msg.SeqNum)
						i := c.writeNotAckEarliest
						for {
							isAck, ok := c.msgWrittenAckMap[i]
							if !ok || !isAck {
								break
							}
							c.writeNotAckEarliest++
							delete(c.msgWrittenMap, i)
							delete(c.msgWrittenAckMap, i)
							i++
						}
						for i := 0; i < c.writeNotAckEarliest - oldEarliest && c.msgToWriteQueue.Len() > 0; i++ {
							cacheMsg := c.msgToWriteQueue.Front()
							c.msgToWriteQueue.Remove(cacheMsg)
							c.msgConnectChan <- cacheMsg.Value.(Message)
							c.msgWrittenMap[msg.SeqNum] = msg
							c.msgWrittenAckMap[msg.SeqNum] = false
						}
					} else if msg.SeqNum > c.writeNotAckEarliest {
						delete(c.msgWrittenMap, msg.SeqNum)
						c.msgWrittenAckMap[msg.SeqNum] = true
					} else {
						fmt.Println("receive duplicate ack msg, seqNum=" + strconv.Itoa(msg.SeqNum))
					}
				}
			} else {
				fmt.Println("msg that type is connect is illegal for client")
			}
		case payload := <-c.msgToWriteCacheChan:
			msg := NewData(c.connId, c.nextSeqNum, payload)
			c.nextSeqNum++
			if msg.SeqNum < c.writeNotAckEarliest + c.windowSize {
				c.msgConnectChan <- *msg
				c.msgWrittenMap[msg.SeqNum] = *msg
				c.msgWrittenAckMap[msg.SeqNum] = false
			} else {
				c.msgToWriteQueue.PushBack(*msg)
			}
		case <- c.epochSignalChan:
			c.epochCount++
			if c.epochCount >= c.epochLimit {
				if c.connId == 0 {
					c.Close()
					c.shutdownChan <- true
					continue
				} else {
					c.Close()
					continue
				}
			}
			if c.connId == 0 {
				fmt.Println("epoch resend connect msg")
				msg := NewConnect();
				c.msgConnectChan <- *msg;
			} else {
				if c.nextSeqNum == 1 {
					fmt.Println("epoch send msg seqNum = 0")
					msg := NewAck(c.connId, 0)
					c.msgConnectChan <- *msg
				} else {
					for k, v := range c.msgWrittenMap {
						if isAck, ok := c.msgWrittenAckMap[k];  ok{
							if !isAck {
								c.msgConnectChan <- v
							}
						}
					}
					for iter := c.msgReceivedQueue.Front(); iter != nil; iter = iter.Next() {
						msg := NewAck(c.connId, iter.Value.(Message).SeqNum)
						c.msgConnectChan <- *msg
					}
				}
			}
		case <- c.closeChan:
			fmt.Println("event handler exit")
			return
		}
	}
}
