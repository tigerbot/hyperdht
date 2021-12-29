package hyperdht_test

import (
	"context"
	"crypto/sha256"
	"io"
	"log"
	"net"
	"time"

	"github.com/anacrolix/utp"

	"github.com/tigerbot/hyperdht"
	"github.com/tigerbot/hyperdht/dhtRpc"
)

func EchoConn(c net.Conn) {
	log.Println("received connection from", c.RemoteAddr())
	defer c.Close()
	io.Copy(c, c)
}

func AnnounceLoop(ctx context.Context, dht *hyperdht.HyperDHT, key []byte) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		if err := dht.AnnounceDiscard(ctx, key, nil); err != nil {
			log.Fatalln("failed to announce on the DHT:", err)
		} else {
			log.Printf("announced on %x...%x\n", key[:4], key[len(key)-4:])
		}

		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
		}
	}
}

func ExampleHyperDHT_server() {
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

	dht, err := hyperdht.New(&dhtRpc.Config{BootStrap: bootstrap, Socket: socket})
	if err != nil {
		socket.Close()
		log.Fatalln("failed to create new DHT:", err)
	}
	defer dht.Close()
	log.Printf("DHT listening on %s using ID %x\n", dht.Addr(), dht.ID())

	ctx, done := context.WithCancel(context.Background())
	defer done()

	key := sha256.Sum256([]byte("hyperdht example"))
	go AnnounceLoop(ctx, dht, key[:])

	for {
		if conn, err := socket.Accept(); err == nil {
			go EchoConn(conn)
		} else {
			log.Fatal("failed to accept incoming connection:", err)
		}
	}
}
