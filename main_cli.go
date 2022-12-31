package main

import (
	"strings"

	"github.com/dbrest-io/dbrest/env"
	"github.com/dbrest-io/dbrest/server"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/spf13/cast"
)

var cliServe = &g.CliSC{
	Name:        "serve",
	Description: "launch the dbREST API endpoint",
	ExecProcess: serve,
}

var cliConns = &g.CliSC{
	Name:        "conns",
	Singular:    "local connection",
	Description: "Manage local connections in the dbREST env file",
	SubComs: []*g.CliSC{
		{
			Name:        "discover",
			Description: "list available streams in connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
			Flags: []g.Flag{
				{
					Name:        "filter",
					ShortName:   "f",
					Type:        "string",
					Description: "filter stream name by pattern (e.g. account_*)",
				},
				{
					Name:        "folder",
					Type:        "string",
					Description: "discover streams in a specific folder (for file connections)",
				},
				{
					Name:        "schema",
					Type:        "string",
					Description: "discover streams in a specific schema (for database connections)",
				},
			},
		},
		{
			Name:        "list",
			Description: "list local connections detected",
		},
		{
			Name:        "test",
			Description: "test a local connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
		},
		{
			Name:        "unset",
			Description: "remove a connection from the dbREST env file",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to remove",
				},
			},
		},
		{
			Name:        "set",
			Description: "set a connection in the dbREST env file",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to set",
				},
				{
					Name:        "key=value properties...",
					ShortName:   "",
					Type:        "string",
					Description: "The key=value properties to set. See https://docs.dbrest.io/",
				},
			},
		},
	},
	ExecProcess: conns,
}

var cliToken = &g.CliSC{
	Name:        "token",
	Description: "manage access tokens & roles",
	ExecProcess: token,
}

func serve(c *g.CliSC) (ok bool, err error) {

	s := server.NewServer()
	defer s.Close()

	go s.Start()

	<-ctx.Ctx.Done()

	return true, nil
}

func conns(c *g.CliSC) (ok bool, err error) {
	ok = true

	ef := env.LoadDbRestEnvFile()
	ec := connection.EnvConns{EnvFile: &ef}

	switch c.UsedSC() {
	case "unset":
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))
		if name == "" {
			flaggy.ShowHelp("")
			return ok, nil
		}

		err := ec.Unset(name)
		if err != nil {
			return ok, g.Error(err, "could not unset %s", name)
		}
		g.Info("connection `%s` has been removed from %s", name, ec.EnvFile.Path)
	case "set":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return ok, nil
		}

		kvArr := []string{cast.ToString(c.Vals["value properties..."])}
		kvMap := map[string]interface{}{}
		for k, v := range g.KVArrToMap(append(kvArr, flaggy.TrailingArguments...)...) {
			k = strings.ToLower(k)
			kvMap[k] = v
		}
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))

		err := ec.Set(name, kvMap)
		if err != nil {
			return ok, g.Error(err, "could not unset %s", name)
		}
		g.Info("connection `%s` has been set in %s. Please test with `dbrest conns test %s`", name, ec.EnvFile.Path, name)

	case "list":
		println(ec.List())

	case "test":
		name := cast.ToString(c.Vals["name"])
		ok, err = ec.Test(name)
		if err != nil {
			return ok, g.Error(err, "could not test %s", name)
		} else if ok {
			g.Info("success!") // successfully connected
		}
	case "discover":
		name := cast.ToString(c.Vals["name"])
		opt := connection.DiscoverOptions{
			Schema: cast.ToString(c.Vals["schema"]),
			Folder: cast.ToString(c.Vals["folder"]),
			Filter: cast.ToString(c.Vals["filter"]),
		}

		streamNames, err := ec.Discover(name, opt)
		if err != nil {
			return ok, g.Error(err, "could not discover %s", name)
		}

		g.Info("Found %d streams:", len(streamNames))
		for _, sn := range streamNames {
			println(g.F(" - %s", sn))
		}

	case "":
		return false, nil
	}
	return ok, nil
}

func token(c *g.CliSC) (ok bool, err error) { return }
