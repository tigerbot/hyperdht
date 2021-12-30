package protoschemas

import (
	// go generate commands depend on utility installed through this package
	_ "google.golang.org/protobuf/proto"
)

//go:generate protoc --go_out=. peer-schema.proto
//go:generate protoc --go_out=. rpc-schema.proto
