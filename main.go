package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/ninedraft/tgbotstream/internal/withtmt"
	"github.com/ninedraft/tgbotstream/secret"

	telegram "github.com/OvyFlash/telegram-bot-api"
	colorlog "github.com/charmbracelet/log"
	"github.com/coder/websocket"
)

func run() (int, error) {
	colorlog.Default().SetLevel(colorlog.DebugLevel)
	lg := slog.New(colorlog.Default())

	slog.SetDefault(lg)
	slog.SetLogLoggerLevel(slog.LevelDebug)

	telegramTokenFile := cmp.Or(os.Getenv("TELEGRAM_TOKEN_FILE"), "./token")

	authSecretFile := cmp.Or(os.Getenv("AUTH_SECRET_FILE"), "./auth_secret")

	getUpdatesTimeout := time.Minute
	flag.DurationVar(&getUpdatesTimeout, "updates-timeout", getUpdatesTimeout, "Timeout for getting updates from Telegram API")

	retrySleep := 5 * time.Second
	flag.DurationVar(&retrySleep, "retry-sleep", retrySleep, "Sleep duration before retrying after a failed publish")

	listenAddr := "localhost:4201"

	connCap := 16
	flag.IntVar(&connCap, "conncap", connCap, "maximum concurrent websocket connections")

	flag.Parse()

	slog.Info("using telegram token from file", "token_file", telegramTokenFile)
	slog.Info("using auth secret from file", "auth_secret_file", authSecretFile)

	tokenData, err := os.ReadFile(telegramTokenFile)
	token := strings.TrimSpace(string(tokenData))
	if err != nil {
		return 1, fmt.Errorf("reading telegram token from %q: %w", telegramTokenFile, err)
	}

	authSecret, err := secret.FromFile(authSecretFile)
	if err != nil {
		return 1, fmt.Errorf("reading auth secret from %q: %w", authSecretFile, err)
	}

	ctx := context.Background()

	botApi, err := telegram.NewBotAPI(token)
	if err != nil {
		return 1, fmt.Errorf("creating telegram bot API: %w", err)
	}

	offset := &atomic.Int64{}

	mux := http.NewServeMux()

	srv := newService(authSecret, connCap)

	mux.Handle("/ws", srv)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		onFailSleep := retrySleep

		for {
			updates, err := botApi.GetUpdatesWithContext(ctx, telegram.UpdateConfig{
				Offset:  int(offset.Load()),
				Limit:   100,
				Timeout: max(1, int(getUpdatesTimeout/time.Second)),
			})

			if err != nil {
				onFailSleep = min(2*onFailSleep, 5*time.Minute)
				slog.ErrorContext(ctx, "getting updates", "error", err, "retry_after", onFailSleep)

				if !sleep(ctx, onFailSleep) {
					return
				}

				continue
			}
			onFailSleep = retrySleep

			slog.InfoContext(ctx, "got updates", "n_updates", len(updates))

			if len(updates) > 0 {
				offset.Store(int64(updates[len(updates)-1].UpdateID + 1))
			}

			for _, update := range updates {
				err := srv.Publish(ctx, update)
				if err != nil {
					onFailSleep = min(2*onFailSleep, 5*time.Minute)
					slog.ErrorContext(ctx, "publishing update", "error", err, "retry_after", onFailSleep)

					if !sleep(ctx, onFailSleep) {
						return
					}

					continue
				}
				onFailSleep = retrySleep
			}
		}
	}()

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	defer context.AfterFunc(ctx, func() {
		err := TimeoutValue(context.Background(), 5*time.Second, server.Shutdown)
		if err != nil {
			panic("unable to shutdown server gracefully: " + err.Error())
		}
	})()

	slog.Info("listening", "address", listenAddr)
	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return 1, errors.Join(err, context.Cause(ctx))
	}

	return 0, nil
}

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n\nERROR: %v\n\n", err)
	}

	if code != 0 {
		os.Exit(code)
	}
}

type service struct {
	secret      *secret.Secret
	connCounter atomic.Int64
	connTickets chan struct{}

	mu   sync.RWMutex
	subs map[*websocket.Conn]*subscriber
}

type subscriber struct {
	msgs      chan []byte
	slownesss atomic.Int64
}

func newService(scrt *secret.Secret, connCap int) *service {
	return &service{
		secret:      scrt,
		connTickets: make(chan struct{}, connCap),
		subs:        map[*websocket.Conn]*subscriber{},
	}
}

func (srv *service) Publish(ctx context.Context, msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	srv.mu.RLock()
	defer srv.mu.RUnlock()

	slog.DebugContext(ctx, "broadcasting messages", "n_subs", len(srv.subs))
	defer slog.DebugContext(ctx, "done broadcasting messages", "n_subs", len(srv.subs))

	send := func(sub *subscriber) {
		Timeout(ctx, 100*time.Millisecond, func(ctx context.Context) {
			select {
			case sub.msgs <- data:
				sub.slownesss.Store(0)
			case <-ctx.Done():
				sub.slownesss.Add(1)
				return
			}
		})
	}

	wg := &sync.WaitGroup{}

	for _, sub := range srv.subs {
		wg.Go(func() {
			send(sub)
		})
	}

	wg.Wait()

	return nil
}

func (srv *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := slog.With("remote_addr", r.RemoteAddr)

	username, pass, ok := r.BasicAuth()
	if !ok ||
		!secret.New([]byte(username), []byte(pass)).Equal(srv.secret) {

		log.WarnContext(ctx, "unauthorized subscriber attempt", "remote_addr", r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	select {
	case <-ctx.Done():
		log.WarnContext(ctx, "context was cancelled while connectection was waiting for a ticket",
			"error", context.Cause(ctx))
		return
	case <-time.After(time.Second):
		log.WarnContext(ctx, "dropping subscription request: subscription cap is reached, server load at max")
		const status = http.StatusTooManyRequests
		http.Error(w, http.StatusText(status), status)

		return
	case srv.connTickets <- struct{}{}:
		defer func() {
			<-srv.connTickets
		}()
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})

	if err != nil {
		log.WarnContext(ctx, "failed to upgrade connection", "error", err)
		http.Error(w, "failed to upgrade connection: "+err.Error(), http.StatusBadRequest)
		return
	}

	id := srv.connCounter.Add(1)

	log = log.With("conn_id", id)

	log.InfoContext(ctx, "new subscriber")

	msgs := make(chan []byte, 1)
	sub := &subscriber{msgs: msgs}

	srv.mu.Lock()
	srv.subs[conn] = sub
	srv.mu.Unlock()

	closeStatus := websocket.StatusNormalClosure

	defer func() {
		log.DebugContext(ctx, "closing connection", "status", closeStatus)
		conn.Close(closeStatus, closeStatus.String())

		srv.mu.Lock()
		defer srv.mu.Unlock()

		delete(srv.subs, conn)
	}()

	ctx = conn.CloseRead(ctx)

	const pingInterval = 5 * time.Second
	timer := time.NewTimer(pingInterval)
	defer timer.Stop()

	const maxFails = 3
	for fails := 0; fails < maxFails; {
		select {
		case <-ctx.Done():
			log.DebugContext(ctx, "connection context canceled")
			return
		case <-timer.C:
			timer.Reset(pingInterval + rand.N(pingInterval/4))
			log.DebugContext(ctx, "sending ping")
			if err = TimeoutValue(ctx, 2*pingInterval, conn.Ping); err != nil {
				err = errors.Join(err, context.Cause(ctx))
				log.WarnContext(ctx, "pinging client", "error", err)
				fails++
				continue
			}

			fails = int(sub.slownesss.Load())
		case msg := <-msgs:
			if err = conn.Write(ctx, websocket.MessageText, msg); err != nil {
				err = errors.Join(err, context.Cause(ctx))
				log.WarnContext(ctx, "writing message", "error", err)
				fails++
				continue
			}
			fails = 0
		}
	}

	closeStatus = websocket.StatusGoingAway
}

func sleep(ctx context.Context, dt time.Duration) bool {
	select {
	case <-time.After(dt):
		return true
	case <-ctx.Done():
		return false
	}
}
