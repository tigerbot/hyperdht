// +build race

package dhtRpc

func init() {
	raceDetector = true
}
