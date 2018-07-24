package main

import (
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
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
	Bootstrap   []string `json:"bootstrap"`
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
