// Hyperdht provides a command line interface to interact with a hyperdht network. It can be
// used to create bootstrap nodes, announce on the network, or query on the network.
package main

import (
	"context"
	"encoding/hex"
	"log"
	"sync"

	"github.com/spf13/pflag"

	"github.com/tigerbot/hyperdht"
	"github.com/tigerbot/hyperdht/cmd/internal/cmdutils"
	"github.com/tigerbot/hyperdht/dhtrpc"
)

var quiet bool

type handler func(context.Context, *sync.WaitGroup, *hyperdht.HyperDHT, []byte)

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
	pflag.StringSliceVarP(&cfg.Bootstrap, "bootstrap", "b", nil, "the list of servers to contact initially")

	pflag.BoolVar(&quiet, "quiet", false, "don't print the periodic messages")
	pflag.StringSliceVarP(&ourKeys, "announce", "a", nil, "the list of keys to announce on")
	pflag.StringSliceVarP(&queryKeys, "query", "q", nil, "the list of keys to query (incompatible with announce)")
	pflag.Parse()

	if len(ourKeys) > 0 && len(queryKeys) > 0 {
		log.Fatal("announce and query cannot be run in the same command")
	}

	cmdutils.Verbose = !quiet
	dhtCfg := dhtrpc.Config{
		ID:          cfg.ID,
		BootStrap:   cmdutils.ParseAddrs(cfg.Bootstrap),
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

	ctx := cmdutils.InterruptCtx()
	if len(queryKeys) > 0 {
		iterateKeys(ctx, dht, queryKeys, cmdutils.Query)
	} else if len(ourKeys) > 0 {
		iterateKeys(ctx, dht, ourKeys, cmdutils.Announce)
	} else {
		cmdutils.Bootstrap(ctx, dht)
	}
}
