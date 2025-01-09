package main

import (
	"context"
	"fmt"
	"godis/interface/tcp"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"godis/lib/logger"
)

func ListenAndServe(listener net.Listener, handler tcp.Handler, closechan <-chan struct{}) {
	go func() {
		<-closechan
		logger.Info("shutting down ...")
		_ = listener.Close()
		_ = handler.Close()
	}()
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()
	ctx := context.Background()
	var waitDown sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		logger.Info("accept link..")
		waitDown.Add(1)
		go func() {
			defer func() {
				waitDown.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}
	waitDown.Wait()
}

type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func main() {
	// ListenAndServe(":8000")
}
