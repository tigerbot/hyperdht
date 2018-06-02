package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"gitlab.daplie.com/core-sdk/hyperdht/dhtRpc"

	"github.com/pkg/errors"
)

type addrList []net.Addr

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

func main() {
	var bootstrap addrList
	var port int

	flag.Var(&bootstrap, "bootstrap", "a comma separated list of network addresses")
	flag.IntVar(&port, "port", 0, "the port to listen on")
	flag.Parse()

	cfg := dhtRpc.Config{
		BootStrap: bootstrap,
		Port:      port,
	}
	dht, err := dhtRpc.New(&cfg)
	if err != nil {
		panic(err)
	}
	fmt.Println("hyperdht listening on", dht.Addr())

	ctx := context.Background()
	for {
		if err := dht.Bootstrap(ctx); err != nil {
			fmt.Println("encountered error bootstrapping:", err)
		} else {
			fmt.Println("bootstrapped...")
		}

		randDur := rand.Int63n(int64(time.Minute))
		time.Sleep(4*time.Minute + time.Duration(randDur))
	}
}
