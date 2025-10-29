package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/ninedraft/tgbotstream/internal/withtmt"
	"github.com/ninedraft/tgbotstream/secret"
)

type Service struct {
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

func New(scrt *secret.Secret, connCap int) *Service {
	return &Service{
		secret:      scrt,
		connTickets: make(chan struct{}, connCap),
		subs:        map[*websocket.Conn]*subscriber{},
	}
}

func (srv *Service) Publish(ctx context.Context, msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}

	srv.mu.RLock()
	defer srv.mu.RUnlock()

	slog.DebugContext(ctx, "broadcasting messages", "n_subs", len(srv.subs))
	defer slog.DebugContext(ctx, "done broadcasting messages", "n_subs", len(srv.subs))

	send := func(sub *subscriber) {
		withtmt.Timeout(ctx, 100*time.Millisecond, func(ctx context.Context) {
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
		sub := sub

		wg.Go(func() {
			send(sub)
		})
	}

	wg.Wait()

	return nil
}

func (srv *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		log.InfoContext(ctx, "closing connection", "status", closeStatus)
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
			log.InfoContext(ctx, "connection context canceled")
			return
		case <-timer.C:
			timer.Reset(pingInterval + rand.N(pingInterval/4))
			log.DebugContext(ctx, "sending ping")
			if err = withtmt.TimeoutValue(ctx, 2*pingInterval, conn.Ping); err != nil {
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
