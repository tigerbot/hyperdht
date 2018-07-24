package cmdUtils

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht"
)

// Verbose determines whether to print out periodic messages about bootstrapping and announcing.
var Verbose bool

func randDuration() time.Duration {
	part := time.Duration(rand.Int63n(int64(time.Minute)))
	return 4*time.Minute + part
}

func shortID(dht *hyperdht.HyperDHT) string {
	id := dht.ID()
	return fmt.Sprintf("%04x...%04x", id[:2], id[len(id)-2:])
}

// Bootstrap will periodically call the Bootstrap function on the DHT.
func Bootstrap(ctx context.Context, dht *hyperdht.HyperDHT) {
	id := shortID(dht)
	timer := time.NewTimer(randDuration())
	for {
		if err := dht.Bootstrap(ctx); err != nil {
			log.Printf("%s encountered error bootstrapping: %v\n", id, err)
		} else if Verbose {
			log.Printf("%s bootstrapped...\n", id)
		}

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(randDuration())
		}
	}
}

// Announce will use the provided DHT to announce on the network using the provided key.
func Announce(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT, key []byte) {
	if wait != nil {
		defer wait.Done()
	}
	timer := time.NewTimer(randDuration())
	defer timer.Stop()

	id := shortID(dht)
	defer func() {
		// Our main context is already finished, but we still want to unannounce so we create
		// a new context that has a limited lifetime.
		freshCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := dht.Unannounce(freshCtx, key, nil); err != nil {
			log.Printf("%s encountered error unannouncing %x: %v\n", id, key, err)
		} else if Verbose {
			log.Printf("%s unannounced %x\n", id, key)
		}
	}()

	for {
		if err := dht.AnnounceDiscard(ctx, key, nil); err != nil {
			log.Printf("%s encountered error announcing %x: %v\n", id, key, err)
		} else if Verbose {
			log.Printf("%s announced %x...\n", id, key)
		}

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(randDuration())
		}
	}
}

// Query will use the provided DHT to lookup and print the addresses for other nodes that
// have announced using the provided key.
func Query(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT, key []byte) {
	if wait != nil {
		defer wait.Done()
	}

	id := shortID(dht)
	stream := dht.Lookup(ctx, key, nil)
	var respCnt int
	for resp := range stream.ResponseChan() {
		log.Printf("%x from %s\n", key, resp.Node)
		log.Printf("\t%v\n", resp.Peers)
		respCnt++
	}
	if err := <-stream.ErrorChan(); err != nil {
		log.Printf("%s errored querying for %x: %v\n", id, key, err)
	} else if respCnt == 0 {
		log.Printf("%s received no responses querying for %x\n", id, key)
	}
}
