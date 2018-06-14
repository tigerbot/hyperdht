package main

import (
	"io/ioutil"
	"net"
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
	ioutil.WriteFile(filename, []byte(content), 0600)

	var cfg config
	if err := cfg.Set(filename); err != nil {
		t.Fatal(err)
	}

	expected := config{
		Port: 49737,
		BootStrap: []string{
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
	ioutil.WriteFile(filename, []byte(content), 0600)

	var cfg config
	if err := cfg.Set(filename); err != nil {
		t.Fatal("parsing config failed:", err)
	}

	expected := config{
		Port: 49737,
		BootStrap: []string{
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

func TestBootstrapParsing(t *testing.T) {
	cfg := config{
		BootStrap: []string{
			"dht1.daplie.com",
			"192.168.25.1:12345",
		},
	}

	parsed := cfg.parseBootstrap()
	expected := []net.Addr{
		&net.UDPAddr{IP: net.IPv4(45, 55, 220, 157), Port: 49737},
		&net.UDPAddr{IP: net.IPv4(192, 168, 25, 1), Port: 12345},
	}
	if !reflect.DeepEqual(parsed, expected) {
		t.Errorf("parsed was %+v, expected %+v", parsed, expected)
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
