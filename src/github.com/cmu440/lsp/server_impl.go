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
	"os"
	"log"
)

var (
	serverLogFile, _ = os.OpenFile("server_log.txt", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0)
)

type server struct {
	port             int
	epochLimit       int
	epochMillis      int
	windowSize       int
	//init
	conn             *lspnet.UDPConn
	//accept
	nextConnId       int
	acceptClientChan chan *lspnet.UDPAddr
	clientMap        map[int] *clientInfo
	closeClientSignalChan chan int
	clientEOF        chan int
	//read
	msgReceivedChan  chan Message
	msgToProcessChan chan Message
	//write
	msgToWriteChan chan Message
	msgToWriteResponseChan chan bool
	//close client
	closeClientRequestChan chan int
	closeClientResponseChan chan bool
	//close
	closeChan        chan bool
	isShutdown       bool

	//log
	logger           *log.Logger
}

type clientInfo struct {
	addr *lspnet.UDPAddr
	server              *server
	connId              int
					    //read
	msgReceivedQueue    *list.List      //only save msg that type = MsgData
	msgToProcessChan    chan Message    //receive msg, not process
	receiveHasAckSeqNum int
	receivedMsgNotAckMap	    map[int]bool
	receivedMsgMap map[int] Message
					    //write
	msgToWriteQueue     *list.List
	msgToWriteChan      chan []byte
	writeNextSeqNum     int             // next msg seq num
	writeNotAckEarliest int
	msgWrittenMap       map[int]Message //has written to conn, but not receive ack, the epoch will read this and resend msg
	msgWrittenAckMap    map[int]bool    //when write a msg, will record false, when receive ack, record true
					    //close
	closeChan           chan bool
					    //epoch
	epochSignalChan     chan bool
	epochCount          int
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

		nextConnId: 1,
		clientMap: make(map[int] *clientInfo),
		closeClientSignalChan: make(chan int, 10000),
		clientEOF: make(chan int),
		acceptClientChan: make(chan *lspnet.UDPAddr),
		msgReceivedChan: make(chan Message, 10000),
		msgToProcessChan: make(chan Message, 10000),

		msgToWriteChan: make(chan Message),
		msgToWriteResponseChan: make(chan bool),

		closeClientRequestChan: make(chan int),
		closeClientResponseChan: make(chan bool),

		closeChan: make(chan bool, 100),
		isShutdown: false,
		logger: log.New(serverLogFile, "Server-- ", log.Lmicroseconds|log.Lshortfile),

	}
	udpaddr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		fmt.Println("resolve udp addr failed")
		return nil, err
	}
	conn, e := lspnet.ListenUDP("udp", udpaddr)
	if e != nil {
		fmt.Println("listen udp failed")
		return nil, e
	}
	s.conn = conn
	go s.serverReadRoutine()
	go s.serverEventHandlerRoutine()

	return Server(s), nil
}

func (s *server) Read() (int, []byte, error) {
	if s.isShutdown {
		return -1, nil, errors.New("server has bean shutdown")
	}
	select {
	case msg := <- s.msgReceivedChan:
		return msg.ConnID, msg.Payload, nil
	case connID := <- s.clientEOF:
		return connID, nil, errors.New("client has been closed")
	}
}

func (s *server) Write(connID int, payload []byte) error {
	if s.isShutdown {
		return errors.New("server has bean shutdown")
	}
	msg := NewData(connID, -1, payload)
	s.msgToWriteChan <- *msg
	select {
	case ok := <-s.msgToWriteResponseChan:
		if ok {
			return nil
		} else {
			return errors.New("the connID doesn't exist")
		}
	}
}

func (s *server) CloseConn(connID int) error {
	if s.isShutdown {
		return errors.New("server has bean shutdown")
	}
	s.closeClientRequestChan <- connID
	select {
	case ok := <-s.closeClientResponseChan:
		if ok {
			return nil
		} else {
			return errors.New("the connID doesn't exist")
		}
	}
	return nil
}

func (s *server) Close() error {
	if s.isShutdown {
		return errors.New("server has bean shutdown")
	}
	s.logger.Printf("server Close method is invoked\n")
	for i := 0; i < 3; i++ {
		s.closeChan <- true
	}
	return nil
}

func NewClientInfo(addr *lspnet.UDPAddr, s *server, connId int) (cli *clientInfo) {
	cli = &clientInfo{
		addr: addr,
		server: s,
		connId: connId,

		msgReceivedQueue: list.New(),
		msgToProcessChan: make(chan Message),
		receiveHasAckSeqNum: 0,
		receivedMsgNotAckMap: make(map[int]bool),
		receivedMsgMap: make(map[int]Message),

		msgToWriteQueue: list.New(),
		msgToWriteChan: make(chan []byte),
		writeNextSeqNum: 1,
		writeNotAckEarliest: 1,
		msgWrittenMap: make(map[int]Message),
		msgWrittenAckMap: make(map[int]bool),

		closeChan: make(chan bool),

		epochSignalChan: make(chan bool),
		epochCount: 0,
	}
	go cli.clientEpochRoutine()
	go cli.clientEventHandlerRoutine()
	return cli
}

func (s *server) serverEventHandlerRoutine() {
	for {
		select {
		case addr := <- s.acceptClientChan:
			cli := NewClientInfo(addr, s, s.nextConnId)
			s.nextConnId++
			s.clientMap[cli.connId] = cli
			pAck := NewAck(cli.connId, 0)
			buf, _ := json.Marshal(*pAck)
			s.conn.WriteToUDP(buf, cli.addr)
		case <- s.closeChan:
			s.isShutdown = true
			for _, cli := range s.clientMap {
				cli.clientClose()
			}
			s.logger.Printf("server event handler get close signal, exit\n")
			return
		case connId:= <- s.closeClientSignalChan:
			if cli, ok := s.clientMap[connId]; ok {
				cli.clientClose()
				delete(s.clientMap, connId)
				s.closeClientResponseChan <- true
			} else {
				s.closeClientResponseChan <- false
			}
		case msg := <- s.msgToWriteChan:
			if cli, ok := s.clientMap[msg.ConnID]; ok {
				cli.msgToWriteChan <- msg.Payload
				s.msgToWriteResponseChan <- true
			} else {
				s.msgToWriteResponseChan <- false
			}
		case msg := <- s.msgToProcessChan:
			if cli, ok := s.clientMap[msg.ConnID]; ok {
				cli.msgToProcessChan <- msg
			} else {
				s.logger.Printf("server receive msg, type=%v, connId=%d, can not find a client with this connId\n", msg.Type, msg.ConnID)
			}
		}
	}
}

func (s *server) serverReadRoutine() {
	var buf [MTU]byte
	var mes Message
	for {
		n, addr, err := s.conn.ReadFromUDP(buf[0:])
		if err != nil {
			s.logger.Printf("ReadFromAllClients::error:%s\n", err.Error())
			return
		}
		s.logger.Printf("ReadFromAllClients::receive message %s\n", string(buf[0:n]))
		json.Unmarshal(buf[0:n], &mes)

		if mes.Type == MsgConnect {
			s.acceptClientChan <- addr
		} else {
			s.msgToProcessChan <- mes
		}
	}
}

func (c *clientInfo) clientEpochRoutine() {
	tick := time.Tick(time.Duration(c.server.epochMillis) * time.Millisecond)
	for {
		select {
		case <-tick:
			c.epochSignalChan <- true
		case <-c.closeChan:
			c.server.logger.Println("clientInfo epoch routing exit")
			return
		}
	}
}

func (c *clientInfo) clientEventHandlerRoutine() {
	for {
		select {
		case payload := <- c.msgToWriteChan:
			pMsg := NewData(c.connId, c.writeNextSeqNum, payload)
			c.writeNextSeqNum++
			if pMsg.SeqNum < c.writeNotAckEarliest + c.server.windowSize {
				buf, _ := json.Marshal(*pMsg)
				c.server.logger.Printf("server write msg: %s", string(buf))
				c.server.conn.WriteToUDP(buf, c.addr)
				c.msgWrittenMap[pMsg.SeqNum] = *pMsg
				c.msgWrittenAckMap[pMsg.SeqNum] = false
			} else {
				c.msgToWriteQueue.PushBack(*pMsg)
			}
		case msg := <- c.msgToProcessChan:
			c.epochCount = 0
			if msg.Type == MsgData {
				pAck := NewAck(c.connId, msg.SeqNum)
				buf, _ := json.Marshal(*pAck)
				c.server.logger.Printf("server write msg: %s", string(buf))
				c.server.conn.WriteToUDP(buf, c.addr)
				if msg.SeqNum < c.receiveHasAckSeqNum + 1 {
					c.server.logger.Printf("server process receive duplicate msg data, seqNum=%d\n", msg.SeqNum)
					continue
				} else if msg.SeqNum == c.receiveHasAckSeqNum + 1 {
					c.server.logger.Printf("server process receive data msg, seqNum=%d\n", msg.SeqNum)
					c.msgReceivedQueue.PushBack(msg)
					if c.msgReceivedQueue.Len() > c.server.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
						c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
					}
					c.server.msgReceivedChan <- msg
					c.receiveHasAckSeqNum++
					i := c.receiveHasAckSeqNum + 1
					for ; i <= c.receiveHasAckSeqNum + c.server.windowSize; i++{
						isReceived, ok := c.receivedMsgNotAckMap[i]
						if !ok || !isReceived {
							break;
						}
						//ack := NewAck(c.connId, c.receivedMsgMap[i].SeqNum)
						//c.msgConnectChan <- *ack
						c.server.msgReceivedChan <- c.receivedMsgMap[i]
					}
					c.receiveHasAckSeqNum = i - 1
				} else if msg.SeqNum <= c.receiveHasAckSeqNum + c.server.windowSize {
					if _, ok := c.receivedMsgMap[msg.SeqNum]; !ok {
						c.server.logger.Printf("server process receive data msg, larger than nextSeq=%d, seqNum=%d\n", c.receiveHasAckSeqNum+1, msg.SeqNum)
						c.msgReceivedQueue.PushBack(msg)
						if c.msgReceivedQueue.Len() > c.server.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
							c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
						}
						c.receivedMsgMap[msg.SeqNum] = msg
						c.receivedMsgNotAckMap[msg.SeqNum] = true
					}
				} else {
					if _, ok := c.receivedMsgMap[msg.SeqNum]; !ok {
						c.server.logger.Printf("process receive data msg, larger than nextSeq=%d, seqNum=%d\n", c.receiveHasAckSeqNum+1, msg.SeqNum)
						c.msgReceivedQueue.PushBack(msg)
						if c.msgReceivedQueue.Len() > c.server.windowSize { //only save the latest windowSize msg, epoch will use this to resend ack
							c.msgReceivedQueue.Remove(c.msgReceivedQueue.Front())
						}
						c.receivedMsgMap[msg.SeqNum] = msg
						c.receivedMsgNotAckMap[msg.SeqNum] = true
						for i := c.receiveHasAckSeqNum + 1; i <= msg.SeqNum - c.server.windowSize; i++ {
							delete(c.receivedMsgMap, i)
							delete(c.receivedMsgNotAckMap, i)
						}
						c.receiveHasAckSeqNum = msg.SeqNum - c.server.windowSize
					}
				}
			} else if msg.Type == MsgAck {
				if msg.SeqNum == 0 {
					c.server.logger.Printf("server receive keep alive ack msg\n")
				} else if msg.SeqNum >= c.writeNotAckEarliest {
					if msg.SeqNum == c.writeNotAckEarliest {
						oldEarliest := c.writeNotAckEarliest
						c.writeNotAckEarliest++
						c.msgWrittenAckMap[msg.SeqNum] = false
						delete(c.msgWrittenAckMap, msg.SeqNum)
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
							buf, _ := json.Marshal(cacheMsg.Value.(Message))
							c.server.logger.Printf("server write msg: %s", string(buf))
							c.server.conn.WriteToUDP(buf, c.addr)
							c.msgWrittenMap[msg.SeqNum] = msg
							c.msgWrittenAckMap[msg.SeqNum] = false
						}
					} else if msg.SeqNum > c.writeNotAckEarliest {
						delete(c.msgWrittenMap, msg.SeqNum)
						c.msgWrittenAckMap[msg.SeqNum] = true
					}
				} else {
					c.server.logger.Println("receive duplicate ack msg, seqNum=" + strconv.Itoa(msg.SeqNum))
				}
			}
		case <- c.epochSignalChan:
			if c.epochCount > c.server.epochLimit && c.msgToWriteQueue.Len() == 0 {
				c.clientClose()
				continue
			}
			c.epochCount++
			if c.writeNextSeqNum == 1 {
				c.server.logger.Println("epoch send msg seqNum = 0")
				msg := NewAck(c.connId, 0)
				buf, _ := json.Marshal(*msg)
				c.server.logger.Printf("server write msg: %s", string(buf))
				c.server.conn.WriteToUDP(buf, c.addr)
			} else {
				for k, v := range c.msgWrittenMap {
					if isAck, ok := c.msgWrittenAckMap[k]; ok{
						if !isAck {
							buf, _ := json.Marshal(v)
							c.server.logger.Printf("server write msg: %s", string(buf))
							c.server.conn.WriteToUDP(buf, c.addr)
						}
					}
				}
				for iter := c.msgReceivedQueue.Front(); iter != nil; iter = iter.Next() {
					msg := NewAck(c.connId, iter.Value.(Message).SeqNum)
					buf, _ := json.Marshal(*msg)
					c.server.logger.Printf("server write msg: %s", string(buf))
					c.server.conn.WriteToUDP(buf, c.addr)
				}
			}
		case <- c.closeChan:
			fmt.Println("clientInfo eventHandler routine exit")
			return
		}
	}
}

func (c *clientInfo) clientClose() error {
	for i := 0; i < 20; i++ {
		c.closeChan <- true
	}
	c.server.clientEOF <- c.connId
	c.server.closeClientSignalChan <- c.connId
	return nil;
}

