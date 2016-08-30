// Contains the implementation of a LSP server.

package lsp

import (
	"errors"
	"github.com/cmu440/lspnet"
	"container/list"
	"strconv"
	"fmt"
	"time"
	"encoding/json"
)

type server struct {
	port int
	epochLimit int
	epochMillis int
	windowSize int
	//init
	initChan chan bool
	shutdownChan chan bool
	//accept
	nextConnId int
	cliMap map[int]clientInfo
	//read
	msgReceivedChan chan Message
	//close
	closeChan chan bool
	isShutdown bool
}

type clientInfo struct {
	server           *server
	connId           int
	conn             *lspnet.UDPConn
	//read
	msgReceivedQueue     *list.List     //only save msg that type = MsgData
	msgToProcessChan     chan Message   //receive msg, not process
	msgReceivedChan      chan Message   //processed msg, call Read to read msg from this chan
	//write
	msgConnectChan   chan Message
	msgToWriteQueue  *list.List
	msgToWriteChan   chan []byte
	nextSeqNum          int             // next msg seq num
	writeNotAckEarliest int
	msgWrittenMap       map[int]Message //has written to conn, but not receive ack, the epoch will read this and resend msg
	msgWrittenAckMap    map[int]bool    //when write a msg, will record false, when receive ack, record true
	//close
	closeChan        chan bool
	//epoch
	epochSignalChan chan bool
	epochCount int
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := &server{
		port: port,
		epochLimit: params.EpochLimit,
		epochMillis: params.EpochMillis,
		windowSize: params.WindowSize,

		initChan: make(chan bool),
		shutdownChan: make(chan bool),

		nextConnId: 1,
		cliMap: make(map[int]clientInfo),

		msgReceivedChan: make(chan Message),

		closeChan: make(chan bool),
		isShutdown: false,

	}
	udpaddr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+string(strconv.Itoa(s.port)))
	if err != nil {
		fmt.Println("resolve udp addr failed")
		return nil, err
	}
	_, e := lspnet.ListenUDP("udp", udpaddr)
	if e != nil {
		fmt.Println("listen udp failed")
		return nil, e
	}

	go s.acceptRoutine()

	return Server(s), nil
}

func (s *server) Read() (int, []byte, error) {
	if s.isShutdown {
		return -1, nil, errors.New("server has bean shutdown")
	}
	msg := <- s.msgReceivedChan
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {
	cli, ok := s.cliMap[connID]
	if !ok {
		return errors.New("connID is illegal")
	}
	cli.msgToWriteChan <- payload
	return nil
}

func (s *server) CloseConn(connID int) error {
	cli, ok := s.cliMap[connID]
	if !ok {
		return errors.New("connID is illegal")
	}
	cli.close()
	delete(s.cliMap, connID)
	return nil
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

func NewClientInfo(conn *lspnet.UDPConn, s *server, connId int) (cli *clientInfo) {
	cli = &clientInfo{
		conn: conn,
		server: s,
		connId: connId,

		msgToProcessChan: make(chan Message),

		msgConnectChan: make(chan Message),
		msgToWriteQueue: list.New(),
		msgToWriteChan: make(chan []byte),

		closeChan: make(chan bool),
	}
	return cli
}

func (s *server) acceptRoutine() {
	udpaddr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+string(strconv.Itoa(s.port)))
	if err != nil {
		return nil, err
	}
	for {
		conn, e := lspnet.ListenUDP("udp", udpaddr)
		if e != nil {
			fmt.Println("listen failed")
			continue
		}
		cli := NewClientInfo(conn, s, s.nextConnId)
		s.nextConnId++
		s.cliMap[cli.connId] = *cli

		notBlockChan := make(chan bool, 1)
		notBlockChan <- true
		select {
		case <- s.closeChan:
			return
		case <- notBlockChan:
			continue
		}
	}
}

func (c *clientInfo) close() {
	c.closeChan <- true
}

func (c *clientInfo) clientReadRoutine() {
	var buf [1500]byte
	var msg Message
	for {
		n, err := c.conn.Read(buf[:])
		if  err != nil{
			fmt.Println("clientInfo read failed, clientInfo read routine exit")
			return
		}
		e := json.Unmarshal(buf[0:n], msg)
		if e != nil {
			fmt.Println("json unmarshal failed, msg="+string(buf))
			continue
		}
		c.msgToProcessChan <- msg
	}
}

func (c *clientInfo) epochRoutine() {
	tick := time.Tick(time.Duration(c.server.epochMillis) * time.Millisecond)
	for {
		select {
		case <-tick:
			c.epochSignalChan <- true
		case <-c.closeChan:
			fmt.Println("clientInfo epoch routing exit")
			return
		}
	}
}

func (c *clientInfo) clientWriteRoutine() {
	for {
		select {
		case msg := <-c.msgConnectChan:
			msgJson, e := json.Marshal(msg)
			if e != nil {
				continue
			}
			_, err := c.conn.Write(msgJson)
			if err != nil {
				continue
			}
		case <- c.closeChan:
			fmt.Println("clientInfo write routine exit")
			return
		}
	}
}

func (c *clientInfo) eventHandlerRoutine() {
	for {
		select {
		case payload := <- c.msgToWriteChan:
			pMsg := NewData(c.connId, c.nextSeqNum, payload)
			c.nextSeqNum++
			if pMsg.SeqNum < c.writeNotAckEarliest + c.server.windowSize {
				c.msgConnectChan <- *pMsg
				c.msgWrittenMap[pMsg.SeqNum] = *pMsg
				c.msgWrittenAckMap[pMsg.SeqNum] = *pMsg
			} else {
				c.msgToWriteQueue.PushBack(*pMsg)
			}
		case msg := <- c.msgToProcessChan:
			c.epochCount = 0
			if msg.Type == MsgData {
				c.msgReceivedQueue.PushBack(msg)
				if c.msgReceivedQueue.Len() > c.server.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
					c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
				}
				ack := NewAck(c.connId, msg.SeqNum)
				c.msgConnectChan <- *ack
				c.server.msgReceivedChan <- msg
			} else if msg.Type == MsgAck {
				if msg.SeqNum == 0 {
					ackMsg := NewAck(c.connId, 0)
					c.msgConnectChan <- ackMsg
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
			} else if msg.Type == MsgConnect {

			}
		case <- c.epochSignalChan:

		case <- c.closeChan:
			fmt.Println("clientInfo eventHandler routine exit")
			return
		}
	}
}

