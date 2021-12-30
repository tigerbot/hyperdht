package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var tmpDir string

func TestJSONParsing(t *testing.T) {
	filename := filepath.Join(tmpDir, "config.json")
	content := `{
	"id": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
	"port": 49737,
	"bootstrap": [
		"dht1.daplie.com",
		"other.example.com:12345"
	]
}`
	ioutil.WriteFile(filename, []byte(content), 0o600) //nolint:errcheck

	var cfg config
	if err := cfg.Set(filename); err != nil {
		t.Fatal(err)
	}

	expected := config{
		Port: 49737,
		Bootstrap: []string{
			"dht1.daplie.com",
			"other.example.com:12345",
		},
		ID: hexID{
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
		},
	}
	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf("config was %+v, expected %+v", cfg, expected)
	}
}

func TestYAMLParsing(t *testing.T) {
	filename := filepath.Join(tmpDir, "config.yaml")
	content := `
id: "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
port: 49737
bootstrap:
  - dht1.daplie.com
  - other.example.com:12345`
	ioutil.WriteFile(filename, []byte(content), 0o600) //nolint:errcheck

	var cfg config
	if err := cfg.Set(filename); err != nil {
		t.Fatal("parsing config failed:", err)
	}

	expected := config{
		Port: 49737,
		Bootstrap: []string{
			"dht1.daplie.com",
			"other.example.com:12345",
		},
		ID: hexID{
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
			0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef,
		},
	}
	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf("config was %+v, expected %+v", cfg, expected)
	}
}

func TestMain(m *testing.M) {
	var err error
	if tmpDir, err = ioutil.TempDir("", "hyperdht-test"); err != nil {
		panic(err)
	}
	defer os.Remove(tmpDir)

	os.Exit(m.Run())
}
