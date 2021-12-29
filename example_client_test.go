package hyperdht_test

import (
	"context"
	"crypto/sha256"
	"log"
	"net"
	"sync"
	"time"

	"github.com/anacrolix/utp"

	"github.com/tigerbot/hyperdht"
	"github.com/tigerbot/hyperdht/dhtRpc"
)

func HandleConn(wait *sync.WaitGroup, c net.Conn) {
	defer wait.Done()
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))

	buf := make([]byte, 128)
	if _, err := c.Write([]byte("Hello world!")); err != nil {
		log.Printf("errored writing to %s: %v\n", c.RemoteAddr(), err)
	} else if n, err := c.Read(buf); err != nil {
		log.Printf("errored reading from %s: %v\n", c.RemoteAddr(), err)
	} else {
		log.Printf("server %s told us %q\n", c.RemoteAddr(), buf[:n])
	}
}

func ExampleHyperDHT_client() {
	rawBootstrap := []string{
		"dht1.daplie.com:49737",
		"dht2.daplie.com:49737",
		"dht3.daplie.com:49737",
		"dht4.daplie.com:49737",
	}
	bootstrap := make([]net.Addr, len(rawBootstrap))
	for i, str := range rawBootstrap {
		if addr, err := net.ResolveUDPAddr("udp4", str); err != nil {
			log.Fatalf("invalid address %q: %v", str, err)
		} else {
			bootstrap[i] = addr
		}
	}

	socket, err := utp.NewSocket("udp", "")
	if err != nil {
		log.Fatalln("failed to create new UTP socket:", err)
	}

	// We make this node ephemeral because it will make a single query and then disappear, so
	// it's not worth making the other nodes store this one.
	dht, err := hyperdht.New(&dhtRpc.Config{BootStrap: bootstrap, Socket: socket, Ephemeral: true})
	if err != nil {
		socket.Close()
		log.Fatalln("failed to create new DHT:", err)
	}
	defer dht.Close()
	log.Printf("DHT listening on %s\n", dht.Addr())

	ctx, done := context.WithTimeout(context.Background(), 40*time.Second)
	defer done()

	key := sha256.Sum256([]byte("hyperdht example"))
	stream := dht.Lookup(ctx, key[:], nil)
	var wait sync.WaitGroup

	// Because we will potentially receive the same address from multiple peers we need to make
	// sure that we only connect to each one once. Note that it is better to set the flags after
	// successful connection rather than when the address is received as there might be problems
	// holepunching with some peers but not others.
	connected := make(map[string]bool, 1)
	for resp := range stream.ResponseChan() {
		log.Printf("node %s gave us %v\n", resp.Node, resp.Peers)

		for _, peer := range resp.Peers {
			if connected[peer.String()] {
				continue
			}

			// Send a quick ping to see if the peer is behind a firewall, and if we don't get
			// a response try to punch through it using the referring node as an intermediary.
			if _, err1 := dht.Ping(ctx, peer); err1 != nil {
				if err2 := dht.Holepunch(ctx, peer, resp.Node); err2 != nil {
					log.Printf("connecting to %s failed:\n\tdirect ping: %v\n\tholepunch:   %v\n", peer, err1, err2)
					continue
				}
			}

			if conn, err := socket.DialContext(ctx, peer.Network(), peer.String()); err == nil {
				connected[peer.String()] = true
				wait.Add(1)
				go HandleConn(&wait, conn)
			}
		}
	}

	if err := <-stream.ErrorChan(); err != nil {
		log.Fatalln("lookup query errored:", err)
	}
	wait.Wait()
}
