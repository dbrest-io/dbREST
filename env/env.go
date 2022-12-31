package env

import (
	"embed"
	"io/ioutil"
	"os"

	env "github.com/flarco/dbio/env"
	"github.com/flarco/g"
)

var (
	HomeDir        = os.Getenv("DBREST_HOME_DIR")
	HomeDirEnvFile = ""
	Env            = &env.EnvFile{}
)

//go:embed *
var envFolder embed.FS

func init() {

	HomeDir = env.SetHomeDir("dbrest")
	HomeDirEnvFile = env.GetEnvFilePath(HomeDir)

	// create env file if not exists
	os.MkdirAll(HomeDir, 0755)
	if HomeDir != "" && !g.PathExists(HomeDirEnvFile) {
		defaultEnvBytes, _ := envFolder.ReadFile("default.env.yaml")
		ioutil.WriteFile(HomeDirEnvFile, defaultEnvBytes, 0644)
	}

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
