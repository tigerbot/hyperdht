package main

import (
	"context"
	"encoding/hex"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"gitlab.daplie.com/core-sdk/hyperdht"
	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"
)

type handler func(context.Context, *sync.WaitGroup, *hyperdht.HyperDHT, []byte)

type addrList []net.Addr

func (l *addrList) Type() string {
	return "host:port"
}
func (l *addrList) String() string {
	if l == nil {
		return ""
	}

	str := make([]string, 0, len(*l))
	for _, a := range *l {
		str = append(str, a.String())
	}
	return strings.Join(str, ",")
}
func (l *addrList) Set(input string) error {
	for _, str := range strings.Split(input, ",") {
		hostStr, portStr, err := net.SplitHostPort(str)
		if err != nil {
			return errors.Errorf("%q is not a valid network address", str)
		}

		host := net.ParseIP(hostStr)
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil || host == nil || port == 0 {
			return errors.Errorf("%q is not a valid network address", str)
		}

		*l = append(*l, &net.UDPAddr{IP: host, Port: int(port)})
	}

	return nil
}

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
	timer := time.NewTimer(randDuration())
	defer timer.Stop()

	run := func() {
		if err := dht.Bootstrap(ctx); err != nil {
			log.Println("encountered error bootstrapping:", err)
		} else {
			log.Println("bootstrapped...")
		}
	}

	run()
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
func announce(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT, key []byte) {
	if wait != nil {
		defer wait.Done()
	}
	timer := time.NewTimer(randDuration())
	defer timer.Stop()

	run := func() {
		stream := dht.Announce(ctx, key)
		for _ = range stream.ResponseChan() {
		}
		if err := <-stream.ErrorChan(); err != nil {
			log.Printf("encountered error announcing %x: %v\n", key, err)
		} else {
			log.Printf("announced %x...\n", key)
		}
	}
	unrun := func() {
		// Our main context is already finished, but we still want to unannounce so we create
		// a new context that has a limited lifetime.
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := dht.Unannounce(ctx, key); err != nil {
			log.Printf("encountered error unannouncing %x: %v\n", key, err)
		} else {
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

	stream := dht.Lookup(ctx, key)
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
	for _, arg := range args {
		for _, hexKey := range strings.Split(arg, ",") {
			if key, err := hex.DecodeString(hexKey); err != nil {
				log.Fatalf("invalid hex key %q: %v", hexKey, err)
			} else {
				keyList = append(keyList, key)
			}
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
	var bootstrap addrList
	var ourKeys, queryKeys []string
	var port int
	var ephemeral bool

	pflag.IntVarP(&port, "port", "p", 0, "the port to listen on")
	pflag.VarP(&bootstrap, "bootstrap", "b", "the list of servers to contact initially")
	pflag.BoolVar(&ephemeral, "ephemeral", false, "don't inform other peers of our ID")
	pflag.StringArrayVarP(&ourKeys, "announce", "a", nil, "the list of keys to announce on")
	pflag.StringArrayVarP(&queryKeys, "query", "q", nil, "the list of keys to query (incompatible with announce)")
	pflag.Parse()

	if len(ourKeys) > 0 && len(queryKeys) > 0 {
		log.Fatal("announce and query cannot be run in the same command")
	}

	cfg := dhtRpc.Config{
		BootStrap: bootstrap,
		Port:      port,
		Ephemeral: ephemeral || len(queryKeys) > 0,
	}
	dht, err := hyperdht.New(&cfg)
	if err != nil {
		log.Fatalln("error creating hyperdht node", err)
	}
	defer dht.Close()
	log.Println("hyperdht listening on", dht.Addr())

	ctx := interruptCtx()
	if len(queryKeys) > 0 {
		iterateKeys(ctx, dht, queryKeys, query)
		return
	}

	if len(ourKeys) > 0 {
		iterateKeys(ctx, dht, ourKeys, announce)
	} else {
		normal(ctx, dht)
	}
}
