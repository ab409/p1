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

	for {
		connID, buf, err := s.Read()
		//handle client or miner disconnect
		if err != nil {

			continue
		}
		var msg bitcoin.Message
		json.Unmarshal(buf, &msg)

	}

}
