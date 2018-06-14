package main

import (
	"context"
	"encoding/hex"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"gitlab.daplie.com/core-sdk/hyperdht"
	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
)

var quiet bool

type handler func(context.Context, *sync.WaitGroup, *hyperdht.HyperDHT, []byte)

func interruptCtx() context.Context {
	ctx, done := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt)

	go func() {
		<-sigChan
		log.Println("interrupt detected, shutting down (interrupt again to force close)")
		done()
		signal.Stop(sigChan)
	}()

	return ctx
}
func randDuration() time.Duration {
	part := time.Duration(rand.Int63n(int64(time.Minute)))
	return 4*time.Minute + part
}

func normal(ctx context.Context, dht *hyperdht.HyperDHT) {
	for {
		if err := dht.Bootstrap(ctx); err != nil {
			log.Println("encountered error bootstrapping:", err)
		} else if !quiet {
			log.Println("bootstrapped...")
		}

		timer := time.NewTimer(randDuration())
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
func announce(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT, key []byte) {
	if wait != nil {
		defer wait.Done()
	}
	timer := time.NewTimer(randDuration())
	defer timer.Stop()

	run := func() {
		if err := dht.AnnounceDiscard(ctx, key, nil); err != nil {
			log.Printf("encountered error announcing %x: %v\n", key, err)
		} else if !quiet {
			log.Printf("announced %x...\n", key)
		}
	}
	unrun := func() {
		// Our main context is already finished, but we still want to unannounce so we create
		// a new context that has a limited lifetime.
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := dht.Unannounce(ctx, key, nil); err != nil {
			log.Printf("encountered error unannouncing %x: %v\n", key, err)
		} else if !quiet {
			log.Printf("unannounced %x\n", key)
		}
	}

	run()
	defer unrun()
	for {
		select {
		case <-ctx.Done():
			return

		case <-timer.C:
			run()
			timer.Reset(randDuration())
		}
	}
}
func query(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT, key []byte) {
	if wait != nil {
		defer wait.Done()
	}

	stream := dht.Lookup(ctx, key, nil)
	var respCnt int
	for resp := range stream.ResponseChan() {
		log.Printf("%x from %s\n", key, resp.Node)
		log.Printf("\t%v\n", resp.Peers)
		respCnt++
	}
	if err := <-stream.ErrorChan(); err != nil {
		log.Printf("error querying for %x: %v\n", key, err)
	} else if respCnt == 0 {
		log.Printf("received no responses querying for %x\n", key)
	}
}

func iterateKeys(ctx context.Context, dht *hyperdht.HyperDHT, args []string, f handler) {
	keyList := make([][]byte, 0, len(args))
	for _, hexKey := range args {
		if key, err := hex.DecodeString(hexKey); err != nil {
			log.Fatalf("invalid hex key %q: %v", hexKey, err)
		} else {
			keyList = append(keyList, key)
		}
	}

	if len(keyList) == 1 {
		f(ctx, nil, dht, keyList[0])
	} else {
		var wait sync.WaitGroup
		wait.Add(len(keyList))
		for _, key := range keyList {
			go f(ctx, &wait, dht, key)
		}
		wait.Wait()
	}
}

func main() {
	var cfg config
	var ourKeys, queryKeys []string

	pflag.VarP(&cfg, "config", "c", "the location of the config file to read")
	pflag.IntVarP(&cfg.Port, "port", "p", 0, "the port to listen on")
	pflag.BoolVar(&cfg.Ephemeral, "ephemeral", false, "don't inform other peers of our ID")
	pflag.StringSliceVarP(&cfg.BootStrap, "bootstrap", "b", nil, "the list of servers to contact initially")

	pflag.BoolVar(&quiet, "quiet", false, "don't print the periodic messages")
	pflag.StringSliceVarP(&ourKeys, "announce", "a", nil, "the list of keys to announce on")
	pflag.StringSliceVarP(&queryKeys, "query", "q", nil, "the list of keys to query (incompatible with announce)")
	pflag.Parse()

	if len(ourKeys) > 0 && len(queryKeys) > 0 {
		log.Fatal("announce and query cannot be run in the same command")
	}

	dhtCfg := dhtRpc.Config{
		ID:          cfg.ID,
		BootStrap:   cfg.parseBootstrap(),
		Port:        cfg.Port,
		Ephemeral:   cfg.Ephemeral || len(queryKeys) > 0,
		Concurrency: cfg.Concurrency,
	}
	dht, err := hyperdht.New(&dhtCfg)
	if err != nil {
		log.Fatalln("error creating hyperdht node", err)
	}
	defer dht.Close()
	log.Println("hyperdht listening on", dht.Addr())

	ctx := interruptCtx()
	if len(queryKeys) > 0 {
		iterateKeys(ctx, dht, queryKeys, query)
	} else if len(ourKeys) > 0 {
		iterateKeys(ctx, dht, ourKeys, announce)
	} else {
		normal(ctx, dht)
	}
}
