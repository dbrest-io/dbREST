package main

import (
	"sort"
	"strings"

	"github.com/dbrest-io/dbrest/env"
	"github.com/dbrest-io/dbrest/server"
	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/jedib0t/go-pretty/table"
	"github.com/samber/lo"
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

var cliTokens = &g.CliSC{
	Name:        "tokens",
	Description: "manage access tokens & roles",
	SubComs: []*g.CliSC{
		{
			Name:        "issue",
			Description: "create or replace a token. If it exists, add --regenerate to regenerate it.",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the token",
				},
			},
			Flags: []g.Flag{
				{
					Name:        "roles",
					Type:        "string",
					Description: "The roles to attach the token to",
				},
				{
					Name:        "regenerate",
					Type:        "string",
					Description: "Whether to regenerate the token (it is exists)",
				},
			},
		},
		{
			Name:        "revoke",
			Description: "delete an existing token. The token will no longer have access to the API",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the token",
				},
			},
		},
		{
			Name:        "toggle",
			Description: "Enable/Disable a token",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the token",
				},
			},
		},
		{
			Name:        "list",
			Description: "List all existing tokens",
		},
		{
			Name:        "roles",
			Description: "List all roles detected",
		},
	},
	ExecProcess: tokens,
}

func serve(c *g.CliSC) (ok bool, err error) {
	if len(state.Connections) == 0 {
		g.Warn("No connections have been defined. Please create some with command `dbrest conns` or put a URL in an environment variable. See https://docs.dbrest.io for more details.")
		return true, g.Error("No connections have been defined. Please create file %s", env.HomeDirEnvFile)
	} else if !g.PathExists(env.HomeDirRolesFile) {
		g.Warn("No roles have been defined. See https://docs.dbrest.io for more details.")
		return true, g.Error("No roles have been defined. Please create file %s", env.HomeDirRolesFile)
	} else if !g.PathExists(env.HomeDirTokenFile) {
		g.Warn("No tokens have been issued. Please issue with command `dbrest token`. See https://docs.dbrest.io for more details.")
	}

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
		name := strings.ToLower(cast.ToString(c.Vals["name"]))
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
		name := strings.ToLower(cast.ToString(c.Vals["name"]))

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

	default:
		return false, nil
	}
	return ok, nil
}

func tokens(c *g.CliSC) (ok bool, err error) {
	ok = true
	name := strings.ToLower(cast.ToString(c.Vals["name"]))
	roles := strings.Split(cast.ToString(c.Vals["roles"]), ",")

	switch c.UsedSC() {
	case "issue":
		if name == "" {
			return false, nil
		} else if len(roles) == 0 || roles[0] == "" {
			g.Warn("Must provide roles with --roles")
			return false, nil
		}

		_, regenerate := c.Vals["regenerate"]
		token := state.NewToken(roles)
		if oldToken, ok := state.Tokens[name]; ok {
			if !regenerate {
				token.Token = oldToken.Token
			}
		}

		err = state.Tokens.Add(name, token)
		if err != nil {
			return ok, g.Error(err, "could not issue token")
		}
		g.Info("Successfully added token `%s`", name)
	case "revoke":
		if name == "" {
			return false, nil
		}
		err = state.Tokens.Remove(name)
		if err != nil {
			return ok, g.Error(err, "could not revoke token")
		}
		g.Info("Successfully removed token `%s`", name)
	case "toggle":
		if name == "" {
			return false, nil
		}
		disabled, err := state.Tokens.Toggle(name)
		if err != nil {
			return ok, g.Error(err, "could not toggle token")
		}
		g.Info("token `%s` is now %s", lo.Ternary(disabled, "disabled", "enabled"))
	case "list":
		tokens := lo.Keys(state.Tokens)
		sort.Strings(tokens)
		T := table.NewWriter()
		T.AppendHeader(table.Row{"Token Name", "Enabled", "Roles"})
		for _, name := range tokens {
			token := state.Tokens[name]
			T.AppendRow(
				table.Row{name, cast.ToString(!token.Disabled), strings.Join(token.Roles, ",")},
			)
		}
		println(T.Render())
	case "roles":
		columns := iop.Columns{
			{Name: "Role", Type: iop.StringType},
			{Name: "Connection", Type: iop.StringType},
			{Name: "Grant", Type: iop.StringType},
			{Name: "Object", Type: iop.StringType},
		}
		data := iop.NewDataset(columns)
		for roleName, role := range state.Roles {
			for connName, grant := range role {
				for _, object := range grant.AllowRead {
					data.Append([]any{roleName, connName, "AllowRead", object})
				}

				for _, object := range grant.AllowWrite {
					data.Append([]any{roleName, connName, "AllowWrite", object})
				}

				if string(grant.AllowSQL) != "" {
					data.Append([]any{roleName, connName, "AllowSQL", string(grant.AllowSQL)})
				}
			}
		}

		data.Sort(0, 1, 2)
		data.Print(0)
	default:
		return false, nil
	}
	return
}
