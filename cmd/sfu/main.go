package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	// Embed the timezone database so Europe/Stockholm (used for
	// peak_sessions_today's daily reset) resolves on the tzdata-less alpine
	// runtime image without an extra apk package.
	_ "time/tzdata"

	"github.com/mollohq/m3llo-sfu/internal/server"
)

func main() {
	cfg := server.ConfigFromEnv()

	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	go srv.Run()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	srv.Logger.Info("server_stopping", nil)
	srv.Shutdown()
}
