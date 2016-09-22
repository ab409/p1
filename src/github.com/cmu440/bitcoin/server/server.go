package main

import (
	"fmt"
	"os"
	"strconv"
	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
	"encoding/json"
	"container/list"
)

type requestWrap struct {
	id              int
	clientID        int
	parent          *requestWrap
	request         *bitcoin.Message
	splitCount      int
	resultCount     int
	childrenRequest *list.List
	result          *bitcoin.Message
}

var requestID int = 1

func newRequestWrap(request *bitcoin.Message, parent *requestWrap, clientID int) *requestWrap {
	wrap := &requestWrap{
		id: requestID,
		clientID: clientID,
		request: request,
		splitCount: 1,
		resultCount: 0,
		childrenRequest: list.New(),
		parent: parent,
	}
	requestID++
	return wrap
}

type minerPool struct {
	availableMinerChan  chan int
	availableMinerMap map[int]bool
	workingMinerRequestMap map[int]*requestWrap
	requestQueue chan *requestWrap
	clientMap map[int]bool
	s lsp.Server
}

func newMinerPool(s lsp.Server) *minerPool {
	pool := &minerPool{
		availableMinerChan: make(chan int, 100000),
		availableMinerMap: make(map[int]bool),
		workingMinerRequestMap: make(map[int]*requestWrap),
		requestQueue: make(chan *requestWrap, 100000),
		clientMap: make(map[int]bool),
		s: s,
	}
	return pool
}

func (p *minerPool) execute(r *requestWrap) {
	p.clientMap[r.clientID] = true
	availableMinerCnt := len(p.availableMinerMap)
	if availableMinerCnt == 0 {
		p.requestQueue <- r
		return
	}
	requestRange := r.request.Upper - r.request.Lower + 1
	if requestRange <= availableMinerCnt {
		r.splitCount = int(requestRange)
		for i := 0; i < requestRange; i++ {
			miner := <-p.availableMinerChan
			if _, ok := p.availableMinerMap[miner]; !ok {
				i--
				continue
			}
			p.rmAvailableMiner(miner)
			var perRequest *bitcoin.Message = bitcoin.NewRequest(r.request.Data, r.request.Lower + uint64(i), r.request.Lower + uint64(i))
			childRequestWarp := newRequestWrap(*perRequest, r, r.clientID)
			r.childrenRequest.PushBack(childRequestWarp)
			buf, _ := json.Marshal(perRequest)
			err := p.s.Write(miner, buf)
			if err != nil {
				p.requestQueue <- childRequestWarp
				p.s.CloseConn(miner)
			} else {
				p.workingMinerRequestMap[miner] = childRequestWarp
			}
		}
	} else {
		r.splitCount = int(availableMinerCnt)
		perRequestRange := requestRange / availableMinerCnt
		mod := requestRange % availableMinerCnt
		var lower uint64 = r.request.Lower
		var upper uint64 = r.request.Lower + perRequestRange - 1
		if mod > 0 {
			upper += 1
			mod--
		}
		for i := 0; i < availableMinerCnt; i++ {
			miner := <-p.availableMinerChan
			if _, ok := p.availableMinerMap[miner]; !ok {
				i--
				continue
			}
			p.rmAvailableMiner(miner)
			var perRequest *bitcoin.Message = bitcoin.NewRequest(r.request.Data, lower, upper)
			childRequestWarp := newRequestWrap(*perRequest, r, r.clientID)
			r.childrenRequest.PushBack(childRequestWarp)
			buf, _ := json.Marshal(perRequest)
			err := p.s.Write(miner, buf)
			if err != nil {
				p.requestQueue <- perRequest
				p.s.CloseConn(miner)
			} else {
				p.workingMinerRequestMap[miner] = childRequestWarp
			}
			lower = upper + 1
			upper = lower + perRequestRange - 1
			if mod > 0 {
				upper += 1
				mod--
			}
		}
	}
}

func (p *minerPool) isRequestQueueEmpty() bool {
	return len(p.requestQueue) == 0
}

func (p *minerPool) executeQueuedReq() {
	for len(p.availableMinerMap) > 0 {
		if p.isRequestQueueEmpty() {
			return
		}
		request := <- p.requestQueue
		if _, ok := p.clientMap[request.clientID]; !ok {
			continue
		}
		p.execute(request)
	}
}

func (p *minerPool) addAvailableMiner(connID int) {
	p.availableMinerChan <- connID
	p.availableMinerMap[connID] = true
	delete(p.workingMinerRequestMap, connID)
}

func (p *minerPool) rmAvailableMiner(connID int) {
	delete(p.availableMinerMap, connID)
}

func (p *minerPool) isMinerWorking(connID int) bool {
	_, ok := p.workingMinerRequestMap[connID]
	return ok
}

func (p *minerPool) getMinerWorkingRequest(minerID int) *requestWrap {
	if !p.isMinerWorking(minerID) {
		return nil
	}
	return p.workingMinerRequestMap[minerID]
}

func (p *minerPool) collectResult(request *requestWrap, result *bitcoin.Message) (bool, *bitcoin.Message) {
	if _, ok := p.clientMap[request.clientID]; !ok {
		return false, nil
	}
	if result.Hash < request.result.Hash {
		request.result = result
	}
	request.resultCount++
	if request.splitCount != request.resultCount {
		return false, nil
	}
	if request.parent == nil {
		return true, request.result
	}
	return p.collectResult(request.parent, request.result)
}

func (p *minerPool) cancelReq(clientID int) {
	delete(p.clientMap, clientID)
}

func (p *minerPool) reExecuteReq(minerID int) {
	req := p.getMinerWorkingRequest(minerID)
	if req == nil {
		return
	}
	p.execute(req)
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}

	// TODO: implement this!
	port, _ := strconv.Atoi(os.Args[1])
	s, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		fmt.Println("server init failed")
		return
	}
	defer s.Close()
	pool := newMinerPool(s)

	for {
		connID, buf, err := s.Read()
		//handle client or miner disconnect
		if err != nil {
			// if client discount, cancel client req
			pool.cancelReq(connID)
			// if miner discount, miner is available, delete it from available miner map
			pool.rmAvailableMiner(connID)
			// if miner discount, miner is working, schedule it to another miner
			pool.reExecuteReq(connID)
			continue
		}
		var msg bitcoin.Message
		json.Unmarshal(buf, &msg)
		switch msg.Type {
		case bitcoin.Join:
			//miner join server
			pool.addAvailableMiner(connID)
			pool.executeQueuedReq()
		case bitcoin.Request:
			//client request
			pool.execute(newRequestWrap(&msg, nil, connID))
		case bitcoin.Result:
			//miner return result
			if req := pool.getMinerWorkingRequest(connID); req != nil {
				if ok, result := pool.collectResult(req, msg); ok {
					buf, _ := json.Marshal(result)
					err := s.Write(req.clientID, buf)
					if err != nil {
						s.CloseConn(req.clientID)
					}
				}
			}
			pool.addAvailableMiner(connID)
			pool.executeQueuedReq()
		}
	}
}