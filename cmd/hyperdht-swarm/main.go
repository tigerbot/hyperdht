// Hyperdht-swarm spawns multiple hyperdht nodes. It is intended to be used to increase the
// network size, either for testing or to make the network more robust.
package main

import (
	"context"
	"crypto/sha256"
	"log"
	"sync"

	"github.com/spf13/pflag"

	"github.com/tigerbot/hyperdht"
	"github.com/tigerbot/hyperdht/cmd/internal/cmdUtils"
	"github.com/tigerbot/hyperdht/dhtRpc"
)

func normal(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT) {
	defer wait.Done()
	defer dht.Close()
	cmdUtils.Bootstrap(ctx, dht)
}
func announce(ctx context.Context, wait *sync.WaitGroup, dht *hyperdht.HyperDHT) {
	defer dht.Close()
	key := sha256.Sum256(dht.ID())
	cmdUtils.Announce(ctx, wait, dht, key[:])
}

func main() {
	var bootstrap []string
	var count int
	var active, verbose bool

	pflag.IntVarP(&count, "count", "c", 1, "the number of hyperdht instances to spawn")
	pflag.StringSliceVarP(&bootstrap, "bootstrap", "b", nil, "the list of servers to contact initially")
	pflag.BoolVarP(&active, "announce", "a", false, "each node will announce the hash of its ID")
	pflag.BoolVarP(&verbose, "verbose", "v", false, "print the periodic messages about bootstrapping and announcing")
	pflag.Parse()

	cmdUtils.Verbose = verbose
	dhtCfg := dhtRpc.Config{
		BootStrap: cmdUtils.ParseAddrs(bootstrap),
	}
	h := normal
	if active {
		h = announce
	}

	ctx := cmdUtils.InterruptCtx()
	var wait sync.WaitGroup
	wait.Add(count)
	for i := 0; i < count; i++ {
		if dht, err := hyperdht.New(&dhtCfg); err != nil {
			log.Fatalln("error creating hyperdht node", err)
		} else {
			if verbose {
				log.Printf("hyperdht %x listening on %s\n", dht.ID(), dht.Addr())
			}
			go h(ctx, &wait, dht)
		}
	}
	wait.Wait()
}
