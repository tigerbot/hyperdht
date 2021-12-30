//go:build race
// +build race

package dhtrpc

import (
	"time"
)

func init() {
	stdTimeout = 5 * time.Second
	swarmBootstrapTimeout = time.Second
	rateCheckInterval = 2 * time.Millisecond
	rateTestResponseDelay = 5 * time.Millisecond
}
