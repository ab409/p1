package main

import (
	"fmt"
	"os"
	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
	"strconv"
	"encoding/json"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}

	// TODO: implement this!
	c, err := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err != nil {
		printDisconnected()
		return
	}
	defer c.Close()
	max, _ := strconv.ParseUint(os.Args[3], 10, 64)
	request := bitcoin.NewRequest(os.Args[2], 0, max)
	buf, _ := json.Marshal(request)
	err = c.Write(buf)
	if err != nil {
		printDisconnected()
		return
	}
	buf, err = c.Read()
	if err != nil {
		printDisconnected()
		return
	}
	var result bitcoin.Message
	json.Unmarshal(buf, &result)
	printResult(strconv.FormatUint(result.Hash, 10), strconv.FormatUint(result.Nonce, 10))
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
