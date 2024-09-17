package env

import (
	"os"

	env "github.com/slingdata-io/sling-cli/core/env"
)

var (
	HomeDir        = os.Getenv("DBREST_HOME_DIR")
	HomeDirEnvFile = ""
	Env            = &env.EnvFile{}
)

func init() {

	HomeDir = env.SetHomeDir("dbrest")
	HomeDirEnvFile = env.GetEnvFilePath(HomeDir)

	if content := os.Getenv("DBREST_ENV_YAML"); content != "" {
		os.Setenv("ENV_YAML", content)
	}

	// other sources of creds
	env.SetHomeDir("sling") // https://github.com/slingdata-io/sling
	env.SetHomeDir("dbnet") // https://github.com/dbnet-io/dbnet

	// create env file if not exists
	os.MkdirAll(HomeDir, 0755)
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
