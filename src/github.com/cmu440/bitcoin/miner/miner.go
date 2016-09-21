package main

import (
	"fmt"
	"os"
	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
	"encoding/json"
	"math"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	// TODO: implement this!
	c, err := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err != nil {
		fmt.Println("miner init lsp client failed")
		return
	}
	defer c.Close()
	join := bitcoin.NewJoin()
	buf, _ := json.Marshal(join)
	err = c.Write(buf)
	if err != nil {
		fmt.Println("miner join server failed")
		return
	}
	for {
		buf, err = c.Read()
		if err != nil {
			fmt.Println("miner read request from server failed")
			return
		}
		var request bitcoin.Message
		json.Unmarshal(buf, &request)
		var minValue uint64 = math.MaxUint64
		var minIndex uint64
		for i := request.Lower; i <= request.Upper; i++ {
			current := bitcoin.Hash(request.Data, i)
			if current < minValue {
				minValue = current
				minIndex = i
			}
		}
		result := bitcoin.NewResult(minValue, minIndex)
		buf, _ = json.Marshal(result)
		err = c.Write(buf)
		if err != nil {
			fmt.Println("miner write result to server failed")
			return
		}
	}
}
