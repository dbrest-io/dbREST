package main

import "github.com/dbrest-io/dbrest/server"

func main() {
	s := server.NewServer()
	s.Start()
}
