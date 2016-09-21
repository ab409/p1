package main

import (
	"fmt"
	"os"
	"strconv"
	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
	"encoding/json"
)

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

	var availableMiner chan int = make(chan int, 10000)
	var workingMiner map[int]int = make(map[int]int)				//key is miner connID, value is requestID
	var requestSplitCount map[int]int = make(map[int]int)
	var requestResult map[int]bitcoin.Message = make(map[int]bitcoin.Message)
	var requestID = 1
	var requestQueue chan bitcoin.Message = make(chan bitcoin.Message, 10000)	//if len(availableMiner) == 0, put request into this queue
	var requestClient map[int]int = make(map[int]int)				//key is requestID, value is request client connID

	for {
		connID, buf, err := s.Read()
		//handle client or miner disconnect
		if err != nil {

			continue
		}
		var msg bitcoin.Message
		json.Unmarshal(buf, &msg)
		switch msg.Type {
		case bitcoin.Join:
			//miner join server
			availableMiner <- connID
			if len(requestQueue) > 0 {
				request := <-requestQueue
				minerConnID := <- availableMiner
				s.Write(minerConnID, )
			}
		case bitcoin.Request:
			//client request

		case bitcoin.Result:
			//miner return result

		}
	}
}

func splitRequest(request bitcoin.Message, availableMiner <-chan int, requestSplitCount map[int]int, requestID int, s lsp.Server, requestQueue chan<- bitcoin.Message) {
	availableMinerCnt := len(availableMiner)
	requestRange := request.Upper - request.Lower
	perRequestRange := requestRange / availableMinerCnt
	if requestRange % availableMinerCnt != 0{
		perRequestRange += 1
	}
	for i := 0; i < availableMinerCnt; i++ {
		var perRequest *bitcoin.Message
		if i == availableMinerCnt - 1 {
			perRequest = bitcoin.NewRequest(request.Data, request.Lower + uint64(i * perRequestRange), request.Upper)
		} else {
			perRequest = bitcoin.NewRequest(request.Data, request.Lower + uint64(i * perRequestRange), request.Lower - 1 + uint64((i+1) * perRequestRange))
		}
		buf, _ := json.Marshal(perRequest)
		miner := <-availableMiner
		err := s.Write(miner, buf)
		if err != nil {
			requestQueue <- perRequest
		}
	}
}
