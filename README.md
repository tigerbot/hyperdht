# hyperdht

[![GoDoc](https://godoc.org/gitlab.daplie.com/core-sdk/hyperdht?status.svg)](https://godoc.org/gitlab.daplie.com/core-sdk/hyperdht)
[![Go Report Card](https://goreportcard.com/badge/gitlab.daplie.com/core-sdk/hyperdht)](https://goreportcard.com/report/gitlab.daplie.com/core-sdk/hyperdht)
[![Build Status](https://gitlab.daplie.com/core-sdk/hyperdht/badges/master/build.svg)](https://gitlab.daplie.com/core-sdk/hyperdht/commits/master)
[![Coverage Report](https://gitlab.daplie.com/core-sdk/hyperdht/badges/master/coverage.svg)](https://gitlab.daplie.com/core-sdk/hyperdht/commits/master)

Hyperdht is a golang implementation of a Kademlia based DHT that supports peer
discovery and distributed hole punching. It uses a DHT that can make RPC calls
to allows nodes to announce that they are listening on a key, and any other
nodes can call a lookup on that keys and get that node's public facing address.
It can also get the node's local address if both nodes specified local addresses
that are in the same subnet in their queries. If the node is behind a NAT the
node that ran the lookup can request that the node storing the information act
as a STUN server to establish a connection.

Implementation is based on and intended to work with
[mafintosh's `hyperdht` written for nodejs](https://github.com/mafintosh/hyperdht).
It is built on the following packages also contained in this repository.

## kbucket

Package kbucket implements a Kademlia DHT K-Bucket using a binary tree.
It is intended to be as minimal as possible and makes as few assumptions as
possible about what is being stored. It also makes no assumptions about how an
ID is generated or how long it must be. It also allows for contacts whose ID
length differs from that of the bucket.

Implementation is based on [tristanls's `k-bucket` written for nodejs](https://github.com/tristanls/k-bucket).

## udpRequest

Package udpRequest allows requests and responses over a net.PacketConn.
Each message that is sent is prepended with a short header that indicates whether
a packet is a request or a response as well as an ID to match responses to their
cooresponding request.

In addition to normal requests and responses, udpRequest also allows forwarding
requests using the same header that was received and forwarding responses to
hosts other than the one the request was received from. However the logic for
when this would be appropriate/necessary does not exist in this layer.

Implementation is based on [mafintosh's `upd-request` written for nodejs](https://github.com/mafintosh/udp-request).

## ipEncoding

Package ipEncoding converts between IP addresses (with ports) and binary buffers.
It also handles encoding/decoding "nodes", which are just IDs associated with an
IP address. Both IPv4 and IPv6 encodings are supported.

Encoding is originally based on [mafintosh's `ipv4-peers` written for nodejs](https://github.com/mafintosh/ipv4-peers).

## dhtRpc

Package dhtRpc makes calls over a Kademlia based DHT.
The DHT uses the [`udpRequest` package](#udpRequest) to send [protobuf](https://developers.google.com/protocol-buffers/)
encoded messages between peers. Anytime a message containing a valid ID is
received the node is stored using the [`kbucket` package](#kbucket), and any
responses that are sent will include its stored nodes closest to the target of
the request, encoded using the [`ipEncoding` package](#ipEncoding) (excluding
responses to the special `ping` query). This allows a DHT node making queries to
progressively find and make requests to the nodes closest to the query's target.

Implementation is based on [mafintosh's `dht-rpc` written for nodejs](https://github.com/mafintosh/dht-rpc).
