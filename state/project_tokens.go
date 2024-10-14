package state

import (
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
)

// TokenMap is map of string to token
type TokenMap map[string]Token

type Token struct {
	Token    string    `json:"token"`
	Roles    []string  `json:"roles"`
	Disabled bool      `json:"disabled"`
	IssuedAt time.Time `json:"issued_at"`
}

func (p *Project) LoadTokens(force bool) (err error) {
	if !(force || time.Since(p.lastLoadedTokens) > (5*time.Second)) {
		return
	}

	if !g.PathExists(p.TokenFile) {
		os.WriteFile(p.TokenFile, []byte("{}"), 0644)
	}

	bytes, _ := os.ReadFile(p.TokenFile)
	err = g.JSONUnmarshal(bytes, &p.Tokens)
	if err != nil {
		err = g.Error(err, "could not unmarshal token map")
	}

	// populate token values map
	for _, token := range p.Tokens {
		p.TokenValues[token.Token] = token
	}

	p.lastLoadedTokens = time.Now()

	return
}

func (p *Project) ResolveToken(value string) (token Token, ok bool) {
	p.mux.Lock()
	token, ok = p.TokenValues[value]
	p.mux.Unlock()
	return
}

func (p *Project) TokenGet(name string, token Token) (err error) {
	p.mux.Lock()
	p.Tokens[name] = token
	p.mux.Unlock()

	err = p.TokenSave()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}
	return
}

func (p *Project) TokenAdd(name string, token Token) (err error) {
	// check roles
	roles := lo.Keys(p.Roles)
	if len(roles) == 0 {
		g.Warn("No roles have been defined. See https://docs.dbrest.io")
		return g.Error("No roles have been defined. Please create file %s", p.RolesFile)
	}

	for _, role := range token.Roles {
		role = strings.ToLower(role)
		if !lo.Contains(roles, role) {
			return g.Error("invalid role: %s. Available roles: %s", role, strings.Join(roles, ","))
		}
	}

	p.mux.Lock()
	p.Tokens[name] = token
	p.TokenValues[token.Token] = token
	p.mux.Unlock()

	err = p.TokenSave()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}

	return
}

func (p *Project) TokenToggle(name string) (disabled bool, err error) {
	p.mux.Lock()
	token, ok := p.Tokens[name]
	if !ok {
		return disabled, g.Error("token %s does not exist", name)
	}

	token.Disabled = !token.Disabled
	disabled = token.Disabled
	p.Tokens[name] = token
	p.TokenValues[token.Token] = token
	p.mux.Unlock()

	err = p.TokenSave()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}

	return
}

func (p *Project) TokenRemove(name string) (err error) {
	p.mux.Lock()
	token, ok := p.Tokens[name]
	if !ok {
		return g.Error("token %s does not exist", name)
	}

	delete(p.Tokens, name)
	delete(p.TokenValues, token.Token)
	p.mux.Unlock()

	err = p.TokenSave()
	if err != nil {
		err = g.Error(err, "could not update token map")
	}
	return
}

func (p *Project) TokenSave() (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	err = os.WriteFile(p.TokenFile, []byte(g.Marshal(p.Tokens)), 0644)
	if err != nil {
		err = g.Error(err, "could not write token map")
	}
	return
}

func NewToken(roles []string) Token {
	return Token{
		Token:    g.RandString(g.AlphaNumericRunes, 64),
		Roles:    roles,
		Disabled: false,
		IssuedAt: time.Now(),
	}
}
