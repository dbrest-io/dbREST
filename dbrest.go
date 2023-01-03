package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/flarco/g"
)

var ctx = g.NewContext(context.Background())
var interrupted = false

func main() {

	exitCode := 11
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	kill := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	signal.Notify(kill, syscall.SIGTERM)

	go func() {
		defer close(done)
		exitCode = cliInit()
	}()

	select {
	case <-done:
		os.Exit(exitCode)
	case <-kill:
		println("\nkilling process...")
		os.Exit(111)
	case <-interrupt:
		if cliServe.Sc.Used {
			println("\ninterrupting...")
			interrupted = true

			ctx.Cancel()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
		os.Exit(exitCode)
		return
	}
}
