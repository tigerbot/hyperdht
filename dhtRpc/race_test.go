// +build race

package dhtRpc

import (
	"time"
)

func init() {
	raceDetector = true

	stdTimeout = 5 * time.Second
	swarmBootstrapTimeout = time.Minute
	rateCheckInterval = 2 * time.Millisecond
	rateTestResponsDelay = 5 * time.Millisecond
}
