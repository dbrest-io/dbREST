package env

import (
	"os"

	env "github.com/flarco/dbio/env"
)

var (
	HomeDir = os.Getenv("DBREST_HOME_DIR")
	Env     = &env.EnvFile{}
)

func init() {

	HomeDir = env.SetHomeDir("dbrest")

	// other sources of creds
	env.SetHomeDir("sling")  // https://github.com/slingdata-io/sling
	env.SetHomeDir("dbrest") // https://github.com/dbrest-io/dbrest
}

func LoadDbRestEnvFile(envFile string) (ef env.EnvFile) {
	ef = env.LoadEnvFile(envFile)
	Env = &ef
	Env.TopComment = "# Environment Credentials for dbREST\n# See https://docs.dbrest.io/\n"
	return
}

func LoadEnvFile(envFile string) (ef env.EnvFile) {
	return env.LoadEnvFile(envFile)
}
