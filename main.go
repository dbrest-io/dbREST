package main

import "github.com/flarco/dbrest/server"

func main() {
	s := server.NewServer()
	s.Start()
}
