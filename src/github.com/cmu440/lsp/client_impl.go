// Contains the implementation of a LSP client.

package lsp

import (
	"errors"
	"encoding/json"
	"time"
	"container/list"
	"github.com/cmu440/lspnet"
	"strconv"
	"log"
	"os"
)

const MTU = 1500

var (
	clientLogFile, _ = os.OpenFile("client_log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0)
)

type client struct {
	conn                *lspnet.UDPConn
	connId              int             //if connId = 0, then the client is not init successfully yet. the epoch routine will use this
	epochLimit          int             // params
	epochMillis         int
	windowSize          int
					    //init
	initChan            chan bool
	shutdownChan        chan bool
					    //write
	writeNextSeqNum     int             // next msg seq num
	writeNotAckEarliest int
	msgWrittenMap       map[int]Message //has written to conn, but not receive ack, the epoch will read this and resend msg
	msgWrittenAckMap    map[int]bool    //when write a msg, will record false, when receive ack, record true
	msgToWriteCacheChan chan []byte     //when call Write, will mot write msg directly, but write to this chan, the eventHandlerRoutine will write data to msgToWriteChan
	msgToWriteQueue     *list.List      //if msg seqNum >= writeNotAckEarList + windowSize, then msg that from msgToWriteCacheChan will write this queue first, when receive ack, the queue will write msg to msgConnectChan
	msgConnectChan      chan Message    //the writeChan will read this chan and write to chan
					    //read
	msgReceivedQueue    *list.List     //only save msg that type = MsgData
	msgToProcessChan    chan Message   //receive msg, not process
	msgReceivedChan     chan Message   //processed msg, call Read to read msg from this chan
	receiveHasAckSeqNum int
	receivedMsgNotAckMap	    map[int]bool
	receivedMsgMap map[int] Message

					    //close
	closeChan           chan bool      //use poison pills to close client
					    //epoch
	epochCount          int
	epochSignalChan     chan bool
	//log
	logger *log.Logger
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

		writeNextSeqNum: 0,
		writeNotAckEarliest: 0,
		msgWrittenMap: make(map[int]Message),
		msgWrittenAckMap: make(map[int]bool),
		msgToWriteCacheChan: make(chan []byte, 10000),
		msgToWriteQueue: list.New(),
		msgConnectChan: make(chan Message, 10000),

		msgReceivedQueue: list.New(),
		msgToProcessChan: make(chan Message, 10000),
		msgReceivedChan: make(chan Message, 10000),
		receiveHasAckSeqNum: 0,
		receivedMsgNotAckMap: make(map[int]bool),
		receivedMsgMap: make(map[int]Message),

		closeChan: make(chan(bool), 5),
		epochCount: 0,
		epochSignalChan: make(chan bool, 100),
		logger: log.New(clientLogFile, "Client-- ", log.Lmicroseconds|log.Lshortfile),
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
		cli.logger.Println("client init success")
		return Client(cli), nil;
	case <- cli.shutdownChan:
		cli.logger.Println("cli shut down")
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
	c.conn.Close()
	for i := 0; i < 3; i++ {
		c.closeChan <- true
	}
	return nil;
}

func (c *client) writeRoutine()  {
	for {
		select {
		case msg := <-c.msgConnectChan:
			msgData, _ := json.Marshal(msg)
			c.logger.Printf("write msg = %s, payload=%s \n", string(msgData), string(msg.Payload))
			c.conn.Write(msgData)
		case <-c.closeChan:
			c.logger.Printf("client %d write routine exit\n", c.connId)
			return
		}
	}
}

func (c *client) readRoutine() {
	var buf [MTU]byte;
	var msg Message;
	for {
		n, err := c.conn.Read(buf[:])
		if err != nil {
			c.logger.Printf("client %d read failed\n", c.connId)
			return
		}
		e := json.Unmarshal(buf[0:n], &msg);
		if e != nil {
			c.logger.Printf("client read routine unmarshal json failed")
			continue
		}
		c.logger.Printf("read msg = %s, payload= %s\n", string(buf[0:n]), string(msg.Payload))
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
			c.logger.Println("client epoch routing exit")
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
				ack := NewAck(c.connId, msg.SeqNum)
				c.msgConnectChan <- *ack
				if msg.SeqNum < c.receiveHasAckSeqNum + 1 {
					c.logger.Printf("process receive duplicate msg data, seqNum=%d\n", msg.SeqNum)
					continue
				} else if msg.SeqNum == c.receiveHasAckSeqNum + 1 {
					c.logger.Printf("process receive data msg, seqNum=%d\n", msg.SeqNum)
					c.msgReceivedQueue.PushBack(msg)
					if c.msgReceivedQueue.Len() > c.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
						c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
					}
					c.msgReceivedChan <- msg
					c.receiveHasAckSeqNum++
					i := c.receiveHasAckSeqNum + 1
					for ; i <= c.receiveHasAckSeqNum + c.windowSize; i++{
						isReceived, ok := c.receivedMsgNotAckMap[i]
						if !ok || !isReceived {
							break;
						}
						//ack := NewAck(c.connId, c.receivedMsgMap[i].SeqNum)
						//c.msgConnectChan <- *ack
						c.msgReceivedChan <- c.receivedMsgMap[i]
					}
					c.receiveHasAckSeqNum = i - 1
				} else if msg.SeqNum <= c.receiveHasAckSeqNum + c.windowSize{
					if _, ok := c.receivedMsgMap[msg.SeqNum]; !ok {
						c.logger.Printf("process receive data msg, larger than nextSeq=%d, seqNum=%d\n", c.receiveHasAckSeqNum+1, msg.SeqNum)
						c.msgReceivedQueue.PushBack(msg)
						if c.msgReceivedQueue.Len() > c.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
							c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
						}
						c.receivedMsgMap[msg.SeqNum] = msg
						c.receivedMsgNotAckMap[msg.SeqNum] = true
					}
				} else {
					if _, ok := c.receivedMsgMap[msg.SeqNum]; !ok {
						c.logger.Printf("process receive data msg, larger than nextSeq=%d, seqNum=%d\n", c.receiveHasAckSeqNum+1, msg.SeqNum)
						c.msgReceivedQueue.PushBack(msg)
						if c.msgReceivedQueue.Len() > c.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
							c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
						}
						c.receivedMsgMap[msg.SeqNum] = msg
						c.receivedMsgNotAckMap[msg.SeqNum] = true
						for i := c.receiveHasAckSeqNum + 1; i <= msg.SeqNum - c.windowSize; i++ {
							delete(c.receivedMsgMap, i)
							delete(c.receivedMsgNotAckMap, i)
						}
						c.receiveHasAckSeqNum = msg.SeqNum - c.windowSize
					}
				}
			} else if msg.Type == MsgAck {
				if msg.SeqNum == 0 {
					c.connId = msg.ConnID
					c.writeNextSeqNum++
					c.writeNotAckEarliest++
					c.initChan <- true
				} else {
					if msg.SeqNum == c.writeNotAckEarliest {
						oldEarliest := c.writeNotAckEarliest
						c.writeNotAckEarliest++
						delete(c.msgWrittenAckMap, msg.SeqNum)
						delete(c.msgWrittenMap, msg.SeqNum)
						i := c.writeNotAckEarliest
						for {
							isAck, ok := c.msgWrittenAckMap[i]
							if !ok || !isAck {
								break
							}
							c.writeNotAckEarliest++
							delete(c.msgWrittenAckMap, i)
							i++
						}
						for i := 0; i < c.writeNotAckEarliest - oldEarliest && i < c.windowSize && c.msgToWriteQueue.Len() > 0; i++ {
							cacheMsg := c.msgToWriteQueue.Front()
							curSeqNum := cacheMsg.Value.(Message).SeqNum
							if curSeqNum > c.writeNotAckEarliest+c.windowSize-1{
								break
							}
							c.msgToWriteQueue.Remove(cacheMsg)
							c.msgConnectChan <- cacheMsg.Value.(Message)
							c.msgWrittenMap[cacheMsg.Value.(Message).SeqNum] = cacheMsg.Value.(Message)
							c.msgWrittenAckMap[cacheMsg.Value.(Message).SeqNum] = false
						}
					} else if msg.SeqNum > c.writeNotAckEarliest {
						delete(c.msgWrittenMap, msg.SeqNum)
						c.msgWrittenAckMap[msg.SeqNum] = true
					} else {
						c.logger.Println("receive duplicate ack msg, seqNum=" + strconv.Itoa(msg.SeqNum))
					}
				}
			} else {
				c.logger.Println("msg that type is connect is illegal for client")
			}
		case payload := <-c.msgToWriteCacheChan:
			msg := NewData(c.connId, c.writeNextSeqNum, payload)
			c.writeNextSeqNum++
			if msg.SeqNum < c.writeNotAckEarliest + c.windowSize {
				c.msgConnectChan <- *msg
				c.msgWrittenMap[msg.SeqNum] = *msg
				c.msgWrittenAckMap[msg.SeqNum] = false
			} else {
				c.msgToWriteQueue.PushBack(*msg)
			}
		case <- c.epochSignalChan:
			c.epochCount++
			if c.epochCount > c.epochLimit {
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
				c.logger.Println("epoch resend connect msg")
				msg := NewConnect();
				c.msgConnectChan <- *msg;
			} else {
				if c.writeNextSeqNum == 1 {
					c.logger.Println("epoch send msg seqNum = 0")
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
			c.logger.Printf("client %d event handler exit\n", c.connId)
			return
		}
	}
}
