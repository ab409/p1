// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"log"
	"os"
	"strconv"
	"time"
)

var (
	serverLogFile, _ = os.OpenFile("server_log.txt", os.O_RDWR|os.O_TRUNC, 0)
)

type clientInfo struct {
	clientAddr         *lspnet.UDPAddr
	connID             int
	nextWriteseqIndex  int
	successWriteIndex  int
	successReadIndex   int
	haveBeenReadIndex  int
	recvMessageChan    chan Message
	sendMessageChan    chan Message
	sendMessageQueue   *list.List
	recvMessageQueue   *list.List
	sendWindow         map[int]bool
	recvWindow         map[int]bool
	clientGoingToclose bool
	clientCloseChan    chan struct{}
	epoch              int
	clientEpochChan    chan struct{}
	clientShutDownChan chan struct{}
}

type server struct {
	serverWriteMethodChan             chan Message
	serverWriteMethodChanResponse     chan bool
	conn                              *lspnet.UDPConn
	serverGoingToClose                bool
	clientConnChan                    chan *lspnet.UDPAddr
	nextClientID                      int
	clientQueue                       map[int]*clientInfo
	windowSize                        int //both the client and server will have the same window sizes
	serverCloseConnMethodChan         chan int
	serverCloseConnMethodChanResponse chan bool
	serverCloseMethodCollect          chan struct{}
	serverCloseMethodChan             chan struct{}
	serverCloseMethodChanResponse     chan bool
	epochlimit                        int
	epochMillis                       int
	serverReadMethodBufferChan        chan Message
	clientEOF                         chan int
	logger                            *log.Logger
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	addr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	s := new(server)
	s.clientEOF = make(chan int)
	s.clientConnChan = make(chan *lspnet.UDPAddr)
	s.clientQueue = make(map[int]*clientInfo)
	s.conn = conn
	s.epochMillis = params.EpochMillis
	s.epochlimit = params.EpochLimit
	s.logger = log.New(serverLogFile, "Server-- ", log.Lmicroseconds|log.Lshortfile)
	s.nextClientID = 1
	s.serverCloseMethodCollect = make(chan struct{})
	s.serverCloseMethodChan = make(chan struct{})
	s.serverCloseMethodChanResponse = make(chan bool)
	s.serverCloseConnMethodChan = make(chan int)
	s.serverCloseConnMethodChanResponse = make(chan bool)
	s.serverGoingToClose = false
	s.serverReadMethodBufferChan = make(chan Message, 10000)
	s.serverWriteMethodChan = make(chan Message)
	s.serverWriteMethodChanResponse = make(chan bool)
	s.windowSize = params.WindowSize

	go s.ServerEventHandle()
	go s.ReadFromAllClients()
	return s, nil
}

func (s *server) ServerEventHandle() {
	for {
		select {
		case addr := <-s.clientConnChan:

			c := new(clientInfo)
			c.clientAddr = addr
			c.clientCloseChan = make(chan struct{})
			c.clientEpochChan = make(chan struct{})
			c.clientGoingToclose = false
			c.clientShutDownChan = make(chan struct{})
			c.connID = s.nextClientID
			c.epoch = 0
			c.haveBeenReadIndex = 0
			c.nextWriteseqIndex = 1
			c.recvMessageChan = make(chan Message)
			c.recvMessageQueue = list.New()
			c.recvWindow = make(map[int]bool)
			c.sendMessageChan = make(chan Message)
			c.sendMessageQueue = list.New()
			c.sendWindow = make(map[int]bool)
			c.successReadIndex = 0
			c.successWriteIndex = 0

			s.logger.Printf("ServerEventHandle:: get new client connection ID:%d\n", c.connID)

			s.clientQueue[c.connID] = c
			s.nextClientID++
			go c.ClientEventHandle(s)
			go c.FireEpoch(s)
			ack := NewAck(c.connID, 0)
			buf, _ := json.Marshal(*ack)
			s.conn.WriteToUDP(buf, c.clientAddr)
		case mes := <-s.serverWriteMethodChan:
			if _, ok := s.clientQueue[mes.ConnID]; ok {
				s.serverWriteMethodChanResponse <- true
				s.clientQueue[mes.ConnID].sendMessageChan <- mes
			} else {
				s.serverWriteMethodChanResponse <- false
			}
		case <-s.serverCloseMethodChan:
			s.logger.Printf("ServerEventHandle::server begin close\n")
			s.serverGoingToClose = true

			clientInfoLen := len(s.clientQueue)
			for i := 0; i < clientInfoLen; i++ {
				<-s.serverCloseMethodCollect
			}
			s.logger.Printf("ServerEventHandle::closing finish\n")
			s.serverCloseMethodChanResponse <- true
		case connID := <-s.serverCloseConnMethodChan:
			if c, ok := s.clientQueue[connID]; ok {
				s.serverCloseConnMethodChanResponse <- true //synchronization
				c.clientCloseChan <- struct{}{}
				delete(s.clientQueue, connID)
			} else {
				s.serverCloseConnMethodChanResponse <- false
			}
		}
	}
}

func (c *clientInfo) ClientEventHandle(s *server) {
	for {
		select {
		case mes := <-c.sendMessageChan:
			if mes.Type == MsgAck {
				buf, _ := json.Marshal(mes)
				s.conn.WriteToUDP(buf, c.clientAddr)
				s.logger.Printf("ClientEventHandle::send ack to client ID:%d SeqNum:%d\n", mes.ConnID, mes.SeqNum)
			} else {
				if mes.SeqNum == -1 {
					mes.SeqNum = c.nextWriteseqIndex
					c.nextWriteseqIndex++
				}
				c.sendMessageQueue.PushBack(mes)
				if len(c.sendWindow) < s.windowSize {
					buf, _ := json.Marshal(mes)
					s.conn.WriteToUDP(buf, c.clientAddr)
					c.sendWindow[mes.SeqNum] = false
					s.logger.Printf("ClientEventHandle::send message to client ID:%d SeqNum:%d\n", mes.ConnID, mes.SeqNum)
				}
			}
		case mes := <-c.recvMessageChan:
			if mes.Type == MsgAck {
				if mes.SeqNum > c.successWriteIndex {
					c.sendWindow[mes.SeqNum] = true
					{
						buf, _ := json.Marshal(mes)
						s.logger.Printf("ClientEventHandle::recv message:%s\n", string(buf))
					}
					i := c.successWriteIndex + 1
					for {
						val, isPresent := c.sendWindow[i]
						if !isPresent || !val {
							break
						}
						s.logger.Printf("ClientEventHandle::delete message:%d\n", i)
						delete(c.sendWindow, i)
						c.sendMessageQueue.Remove(c.sendMessageQueue.Front())
						i++
					}
					c.successWriteIndex = i - 1

					j := c.successWriteIndex + len(c.sendWindow) + 1
					for e := c.sendMessageQueue.Front(); j <= c.successWriteIndex+s.windowSize && e != nil; e = e.Next() {
						c.sendWindow[j] = false
						buf, _ := json.Marshal(e.Value.(Message))
						s.conn.WriteToUDP(buf, c.clientAddr)
						j++
						s.logger.Printf("ClientEventHandle::send message:%s\n", string(buf))
					}
				} else if mes.SeqNum == 0 { //keep alive
					c.epoch = 0
				}
			} else { //receive data

				/*
					both clients and servers will also need to
					maintain a sliding window for their ω most recently
					sent acknowledgments
				*/

				if _, ok := c.recvWindow[mes.SeqNum]; !ok {
					//have never received this Message
					c.recvMessageQueue.PushBack(mes)
					c.recvWindow[mes.SeqNum] = true

					if mes.SeqNum > c.successReadIndex+s.windowSize {
						for i := mes.SeqNum - s.windowSize; i > c.successReadIndex; i-- {
							for e := c.recvMessageQueue.Front(); e != nil; e = e.Next() {
								if e.Value.(Message).SeqNum == i {
									c.recvMessageQueue.Remove(e)
									delete(c.recvWindow, i)
									break
								}
							}
						}
						c.successReadIndex = mes.SeqNum - s.windowSize
					}

					var i int
					for i = c.haveBeenReadIndex + 1; i <= c.successReadIndex+s.windowSize && c.recvMessageQueue.Len() > 0; i++ {
						if _, ok := c.recvWindow[i]; !ok {
							break
						}
						for e := c.recvMessageQueue.Front(); e != nil; e = e.Next() {
							if e.Value.(Message).SeqNum == i {
								s.serverReadMethodBufferChan <- e.Value.(Message)
								c.recvMessageQueue.Remove(e)
								delete(c.recvWindow, i)

								mes := NewAck(c.connID, e.Value.(Message).SeqNum)
								buf, _ := json.Marshal(mes)
								s.conn.WriteToUDP(buf, c.clientAddr)
								s.logger.Printf("ClientEventHandle::send ack:%s\n", buf)
								break
							}
						}
					}
					c.haveBeenReadIndex = i - 1
				}
			}
		case <-c.clientEpochChan:
		/*If no data messages have been received from the client,
		then resend an acknowledg-ment message for the client’s connection request.
		For each data message that has been sent, but not yet acknowledged,
		resend the data message.
		Resend an acknowledgment message for each of the last ω
		(or possibly fewer) distinct data messages that have been received.*/
			s.logger.Printf("fire epoch\n")
			if len(c.sendWindow) == 0 && c.sendMessageQueue.Len() == 0 && c.clientGoingToclose {
				if s.serverGoingToClose {
					s.logger.Printf("ClientEventHandle::epoch  client %d is going to close\n", c.connID)
					s.serverCloseMethodCollect <- struct{}{}
					c.ShutDownAllThing(s)
				}
				c.ShutDownAllThing(s)
				return
			} else {
				/*we will track at the endpoint of each connection the number
				of epochs that have passed since a message (of any type) was
				received from the other end. Once this count reaches a specified epoch
				limit, which we denote with the symbol K, we will declare that the
				connection has been lost.*/
				c.epoch++
				if c.epoch >= s.epochlimit {
					s.logger.Printf("ClientEventHandle::epoch exceed\n")
					if s.serverGoingToClose {
						c.ShutDownAllThing(s)
						s.serverCloseMethodCollect <- struct{}{}
					}
					c.ShutDownAllThing(s)
					return
				} else {
					for i := c.successReadIndex + 1; i <= c.haveBeenReadIndex; i++ {
						mes := NewAck(c.connID, i)
						buf, _ := json.Marshal(*mes)
						s.conn.WriteToUDP(buf, c.clientAddr)
					}

					for i, ok := range c.sendWindow {
						if !ok {
							for e := c.sendMessageQueue.Front(); e != nil; e = e.Next() {
								if e.Value.(Message).SeqNum == i {
									buf, _ := json.Marshal(e.Value.(Message))
									s.conn.WriteToUDP(buf, c.clientAddr)
									break
								}
							}
						}
					}
				}
			}
		case <-c.clientCloseChan:
			s.logger.Printf("ClientEventHandle::close ID:%d\n", c.connID)
			c.clientGoingToclose = true
		}
	}
}

func (s *server) ReadFromAllClients() {
	var buf [1500]byte
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
			if !s.serverGoingToClose {
				s.clientConnChan <- addr
			}
		} else {
			for _, c := range s.clientQueue {
				if c.connID == mes.ConnID {
					c.recvMessageChan <- mes
					break
				}
			}
		}
	}
}

func (c *clientInfo) FireEpoch(s *server) {
	tick := time.Tick(time.Duration(s.epochMillis) * time.Millisecond)
	for {
		select {
		case <-tick:
			c.clientEpochChan <- struct{}{}
		case <-c.clientShutDownChan:
			return
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	select {
	case mes := <-s.serverReadMethodBufferChan:
		return mes.ConnID, mes.Payload, nil
	case connID := <-s.clientEOF:
		return connID, nil, errors.New("client has been closed")
	}
}

func (s *server) Write(connID int, payload []byte) error {
	mes := NewData(connID, -1, payload)
	s.serverWriteMethodChan <- *mes
	{
		buf, _ := json.Marshal(mes)
		s.logger.Printf("Write::write message %s\n", string(buf))
	}
	select {
	case ok := <-s.serverWriteMethodChanResponse:
		if ok {
			return nil
		} else {
			return errors.New("the connID doesn't exist")
		}
	}
}

func (s *server) CloseConn(connID int) error {
	s.serverCloseConnMethodChan <- connID
	select {
	case ok := <-s.serverCloseConnMethodChanResponse:
		if ok {
			return nil
		} else {
			return errors.New("the connID doesn't exist")
		}
	}
}

func (c *clientInfo) ShutDownAllThing(s *server) {
	s.clientEOF <- c.connID //tell server I have already been closed
}

func (s *server) Close() error {
	s.logger.Printf("Close::close\n")
	s.serverGoingToClose = true

	s.serverCloseMethodChan <- struct{}{}
	for _, c := range s.clientQueue {
		c.clientCloseChan <- struct{}{}
	}
	flag, _ := <-s.serverCloseMethodChanResponse
	if flag {
		s.logger.Printf("Close::closed\n")
		return nil
	} else {
		return errors.New("not all client are closed\n")
	}
}
