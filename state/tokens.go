package state

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dbrest-io/dbrest/env"
	"github.com/flarco/g"
	"github.com/samber/lo"
)

func init() {
	LoadTokens(true)
}

var tmMux sync.Mutex
var lastLoadedTokens time.Time

// TokenMap is map of string to token
type TokenMap map[string]Token

type Token struct {
	Token    string    `json:"token"`
	Roles    []string  `json:"roles"`
	Disabled bool      `json:"disabled"`
	IssuedAt time.Time `json:"issued_at"`
}

func LoadTokens(force bool) (err error) {
	if !(force || time.Since(lastLoadedTokens) > (5*time.Second)) {
		return
	}

	if !g.PathExists(env.HomeDirTokenFile) {
		os.WriteFile(env.HomeDirTokenFile, []byte("{}"), 0644)
	}

	bytes, _ := os.ReadFile(env.HomeDirTokenFile)
	err = g.JSONUnmarshal(bytes, &Tokens)
	if err != nil {
		err = g.Error(err, "could not unmarshal token map")
	}

	// populate token values map
	for _, token := range Tokens {
		TokenValues[token.Token] = token
	}

	lastLoadedTokens = time.Now()

	return
}

func ResolveToken(value string) (token Token, ok bool) {
	tmMux.Lock()
	token, ok = TokenValues[value]
	tmMux.Unlock()
	return
}

func (tm TokenMap) Get(name string, token Token) (err error) {
	tmMux.Lock()
	tm[name] = token
	tmMux.Unlock()

	err = tm.Save()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}
	return
}

func (tm TokenMap) Add(name string, token Token) (err error) {
	// check roles
	roles := lo.Keys(Roles)
	if len(roles) == 0 {
		g.Warn("No roles have been defined. See https://docs.dbrest.io")
		return g.Error("No roles have been defined. Please create file %s", env.HomeDirRolesFile)
	}

	for _, role := range token.Roles {
		role = strings.ToUpper(role)
		if !lo.Contains(roles, role) {
			return g.Error("invalid role: %s. Available roles: %s", role, strings.Join(roles, ","))
		}
	}

	tmMux.Lock()
	tm[name] = token
	TokenValues[token.Token] = token
	tmMux.Unlock()

	err = tm.Save()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}

	return
}

func (tm TokenMap) Toggle(name string) (disabled bool, err error) {
	tmMux.Lock()
	token, ok := tm[name]
	if !ok {
		return disabled, g.Error("token %s does not exist", name)
	}

	token.Disabled = !token.Disabled
	disabled = token.Disabled
	tm[name] = token
	tmMux.Unlock()

	err = tm.Save()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}

	return
}

func (tm TokenMap) Remove(name string) (err error) {
	tmMux.Lock()
	token, ok := tm[name]
	if !ok {
		return g.Error("token %s does not exist", name)
	}

	delete(tm, name)
	delete(TokenValues, token.Token)
	tmMux.Unlock()

	err = tm.Save()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}
	return
}

func (tm *TokenMap) Save() (err error) {
	tmMux.Lock()
	defer tmMux.Unlock()
	err = os.WriteFile(env.HomeDirTokenFile, []byte(g.Marshal(tm)), 0644)
	if err != nil {
		err = g.Error(err, "could not write token map")
	}
	return
}

func NewToken(roles []string) Token {
	return Token{
		Token:    g.RandString(g.AplhanumericRunes, 64),
		Roles:    roles,
		Disabled: false,
		IssuedAt: time.Now(),
	}
}
