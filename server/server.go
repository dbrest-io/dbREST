package server

import (
	"net/http"
	"os"
	"time"

	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

// Server is the main server
type Server struct {
	Port       string
	EchoServer *echo.Echo
	StartTime  time.Time
	Routes     []echo.Route
}

func NewServer() (s *Server) {
	s = &Server{EchoServer: echo.New(), Port: "1323"}
	if port := os.Getenv("PORT"); port != "" {
		s.Port = port
	}

	// add routes
	for _, route := range standardRoutes {
		route.Middlewares = append(route.Middlewares, middleware.Logger())
		route.Middlewares = append(route.Middlewares, middleware.Recover())
		s.EchoServer.AddRoute(route)
	}
	return
}

func (s *Server) Start() {
	s.StartTime = time.Now()
	if err := s.EchoServer.Start(":" + s.Port); err != http.ErrServerClosed {
		g.LogFatal(g.Error(err, "could not start server"))
	}
}

func (s *Server) Hostname() string {
	return g.F("http://localhost:%s", s.Port)
}
