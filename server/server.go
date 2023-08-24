package server

import (
	"net/http"
	"os"
	"time"

	"github.com/dbrest-io/dbrest/state"
	"github.com/flarco/g"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

// Server is the main server
type Server struct {
	Port       string
	EchoServer *echo.Echo
	StartTime  time.Time
}

func NewServer() (s *Server) {
	s = &Server{EchoServer: echo.New(), Port: "1323"}
	if port := os.Getenv("PORT"); port != "" {
		s.Port = port
	}

	// add routes
	for _, route := range StandardRoutes {
		route.Middlewares = append(route.Middlewares, middleware.Logger())
		route.Middlewares = append(route.Middlewares, middleware.Recover())
		s.EchoServer.AddRoute(route)
	}

	// cors
	s.EchoServer.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		// AllowOrigins: []string{"http://localhost:1323"},
		// AllowCredentials: true,
		// AllowHeaders: []string{"*"},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, "X-Request-ID", "X-Request-Columns", "X-Request-Continue", "X-Project-ID", "access-control-allow-origin", "access-control-allow-headers"},
		AllowOriginFunc: func(origin string) (bool, error) {
			return true, nil
		},
	}))

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

func (s *Server) Close() {
	state.CloseConnections()
}
