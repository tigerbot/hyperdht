package cmdUtils

import (
	"context"
	"log"
	"os"
	"os/signal"
)

// InterruptCtx returns a context that will be canceled when the program receives an interrupt
// or kill signal.
func InterruptCtx() context.Context {
	ctx, done := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	go func() {
		<-sigChan
		log.Println("interrupt detected, shutting down (interrupt again to force close)")
		done()
		signal.Stop(sigChan)
	}()

	return ctx
}
