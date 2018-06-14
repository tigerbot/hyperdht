package main

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"net"
	"path/filepath"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

var parsers = map[string]func(data []byte, v interface{}) error{
	".json": json.Unmarshal,
	".yaml": yaml.Unmarshal,
	".yml":  yaml.Unmarshal,
}

type hexID []byte

func (b *hexID) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(*b)), nil
}
func (b *hexID) UnmarshalText(src []byte) error {
	buf := make([]byte, hex.DecodedLen(len(src)))
	if _, err := hex.Decode(buf, src); err != nil {
		return err
	}
	*b = buf
	return nil
}

type config struct {
	ID          hexID    `json:"id"`
	Ephemeral   bool     `json:"ephemeral"`
	Concurrency int      `json:"concurrency"`
	BootStrap   []string `json:"bootstrap"`
	Port        int      `json:"port"`
}

func (c *config) Type() string { return "file" }
func (c *config) String() string {
	if buf, err := json.Marshal(c); err != nil {
		panic(err)
	} else {
		return string(buf)
	}
}
func (c *config) Set(filename string) error {
	parse := parsers[filepath.Ext(filename)]
	if parse == nil {
		return errors.Errorf("can't parse %q: unknown file extension", filename)
	}

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrapf(err, "failed to read %q", filename)
	}

	err = parse(file, c)
	return errors.WithMessage(err, "failed to parse config file")
}

func (c *config) parseBootstrap() []net.Addr {
	result := make([]net.Addr, 0, len(c.BootStrap))
	for _, input := range c.BootStrap {
		// The node code defaulted to port 49737 for bootstrap nodes, and so will we. Both
		// SplitHostPort and ResolveUDPAddr will error if there isn't a port specified in the
		// string, so we just assume if the split fails it's because there wasn't a port and
		// we add one. If the assumption is wrong the resolve will still fail anyway.
		if _, _, err := net.SplitHostPort(input); err != nil {
			input = net.JoinHostPort(input, "49737")
		}
		if addr, err := net.ResolveUDPAddr("udp4", input); err != nil {
			panic(errors.Errorf("invalid address %q: %v", input, err))
		} else {
			result = append(result, addr)
		}
	}
	return result
}
