package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	. "github.com/ninedraft/tgbotstream/internal/withtmt"

	colorlog "github.com/charmbracelet/log"
	"github.com/coder/websocket"
	"github.com/ninedraft/tgbotstream/secret"
)

func main() {
	lg := slog.New(colorlog.New(os.Stderr))
	slog.SetDefault(lg)

	slog.SetLogLoggerLevel(slog.LevelDebug)

	secretFile := cmp.Or("./auth_secret", os.Getenv("AUTH_SECRET_FILE"))

	serverAddr := "ws://localhost:4201/ws"

	flag.StringVar(&serverAddr, "addr", serverAddr, "server address to dial")

	n := 1
	flag.IntVar(&n, "n", n, "number of concurrent clients to run")
	flag.Parse()

	scrt, err := secret.FromFile(secretFile)
	if err != nil {
		panic("loading secret from " + secretFile + ": " + err.Error())
	}

	wg := &sync.WaitGroup{}

	for range n {
		wg.Go(func() {
			runClient(scrt, serverAddr)
		})
	}

	wg.Wait()
}

func runClient(scrt *secret.Secret, addr string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := pickName()
	log := slog.With("name", name)

	header := http.Header{}
	header.Set("Authorization", "Basic "+scrt.Encode())

	log.Debug("header", "value", header)

	conn, err := TimeoutValues(ctx, 10*time.Second, func(ctx context.Context) (*websocket.Conn, error) {
		conn, _, err := websocket.Dial(ctx, addr, &websocket.DialOptions{
			CompressionMode: websocket.CompressionContextTakeover,
			HTTPHeader:      header,
		})

		return conn, err
	})
	if err != nil {
		log.ErrorContext(ctx, "unable to connect", "error", err)
		return
	}
	defer conn.Close(websocket.StatusGoingAway, "bye")

	log.InfoContext(ctx, "connected")

	go func() {
		const timeout = 5 * time.Second
		ticker := time.NewTicker(timeout)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := TimeoutValue(ctx, timeout, conn.Ping); err != nil {
					err = errors.Join(err, context.Cause(ctx))
					log.ErrorContext(ctx, "ping", "error", err)
				}
			}
		}
	}()

	for {
		msgType, msg, err := conn.Read(ctx)
		if err != nil {
			err = errors.Join(err, context.Cause(ctx))
			log.ErrorContext(ctx, "reading message", "error", err)
			return
		}

		switch msgType {
		case websocket.MessageText:
			fmt.Println(string(msg))
		case websocket.MessageBinary:
			fmt.Println("<binary>")
		}
	}
}
