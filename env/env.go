package env

import (
	"os"
	"path"

	env "github.com/flarco/dbio/env"
)

var (
	HomeDir          = os.Getenv("DBREST_HOME_DIR")
	HomeDirEnvFile   = ""
	HomeDirTokenFile = ""
	HomeDirRolesFile = ""
	Env              = &env.EnvFile{}
)

func init() {

	HomeDir = env.SetHomeDir("dbrest")
	HomeDirEnvFile = env.GetEnvFilePath(HomeDir)
	HomeDirTokenFile = path.Join(HomeDir, ".tokens") // TODO: use proper salting for tokens
	HomeDirRolesFile = path.Join(HomeDir, "roles.yaml")

	// other sources of creds
	env.SetHomeDir("sling")  // https://github.com/slingdata-io/sling
	env.SetHomeDir("dbrest") // https://github.com/dbrest-io/dbrest
}

func LoadDbRestEnvFile() (ef env.EnvFile) {
	ef = env.LoadEnvFile(HomeDirEnvFile)
	Env = &ef
	Env.TopComment = "# Environment Credentials for dbREST\n# See https://docs.dbrest.io/\n"
	return
}
