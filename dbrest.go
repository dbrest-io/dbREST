package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
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

func cliInit() int {
	// init CLI
	flaggy.SetName("dbrest")
	flaggy.SetDescription("Spin up a REST API for any Major Database | https://github.com/dbrest-io/dbREST")
	flaggy.SetVersion(state.Version)
	flaggy.DefaultParser.ShowHelpOnUnexpected = true
	flaggy.DefaultParser.AdditionalHelpPrepend = "Version " + state.Version

	// make CLI sub-commands
	cliConns.Make().Add()
	cliServe.Make().Add()
	cliToken.Make().Add()

	for _, cli := range g.CliArr {
		flaggy.AttachSubcommand(cli.Sc, 1)
	}

	flaggy.ShowHelpOnUnexpectedDisable()
	flaggy.Parse()

	ok, err := g.CliProcess()
	if err != nil {
		g.LogFatal(err)
	} else if !ok {
		flaggy.ShowHelp("")
	}

	return 0
}
