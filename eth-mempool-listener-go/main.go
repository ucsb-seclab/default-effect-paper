package main

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	//	"github.com/ethereum/go-ethereum/core/types"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

var (
	NodeEndpoint = ""
)

func init() {
	NodeEndpoint = os.Getenv("NODE_ENDPOINT")
}

func main() {
	ctx := context.Background()
	txnsHash := make(chan common.Hash)

	baseClient, err := rpc.Dial(NodeEndpoint)
	if err != nil {
		log.Fatalln(err)
	}

	ethClient, err := ethclient.Dial(NodeEndpoint)
	if err != nil {
		log.Fatalln(err)
	}

	//chainID, err := ethClient.NetworkID(ctx)
	if err != nil {
		log.Fatal(err)
	}

	subscriber := gethclient.New(baseClient)
	_, err = subscriber.SubscribePendingTransactions(ctx, txnsHash)

	if err != nil {
		log.Fatalln(err)
	}

	// signer := types.NewLondonSigner(chainID)

	defer func() {
		baseClient.Close()
		ethClient.Close()
	}()

	t := time.NewTimer(time.Duration(1) * time.Second)

	for true {
		select {
		case txnHash := <-txnsHash:
			t.Reset(time.Duration(1) * time.Second)
			fmt.Println(txnHash.String())
		case <-t.C:
			return
		}
	}
}
