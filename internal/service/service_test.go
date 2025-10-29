package service_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/ninedraft/tgbotstream/internal/service"
	"github.com/ninedraft/tgbotstream/secret"
)

func TestServicePublishDeliversMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := service.New(secret.New([]byte("user"), []byte("pass")), 2)
		server, listener := startServer(t, srv)
		defer shutdownServer(server, listener)

		serverConn, clientConn := net.Pipe()
		defer clientConn.Close()

		listener.push(serverConn)

		tr := &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return clientConn, nil
			},
			ForceAttemptHTTP2: false,
		}
		defer tr.CloseIdleConnections()

		client := &http.Client{Transport: tr}

		ctx := t.Context()

		conn, resp, err := websocket.Dial(ctx, "ws://user:pass@local/ws", &websocket.DialOptions{
			HTTPClient: client,
		})
		if err != nil {
			t.Fatalf("dial: %v (resp=%v)", err, statusOf(resp))
		}
		defer conn.Close(websocket.StatusNormalClosure, "")

		synctest.Wait()

		msg := map[string]string{"text": "hello"}
		if err := srv.Publish(ctx, msg); err != nil {
			t.Fatalf("publish: %v", err)
		}

		synctest.Wait()

		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		typ, payload, err := conn.Read(readCtx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if typ != websocket.MessageText {
			t.Fatalf("unexpected message type: %v", typ)
		}
		if string(payload) != `{"text":"hello"}` {
			t.Fatalf("unexpected payload: %s", payload)
		}
	})
}

func TestServiceUnauthorized(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := service.New(secret.New([]byte("user"), []byte("pass")), 1)
		server, listener := startServer(t, srv)
		defer shutdownServer(server, listener)

		serverConn, clientConn := net.Pipe()
		defer clientConn.Close()

		listener.push(serverConn)

		tr := &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return clientConn, nil
			},
			ForceAttemptHTTP2: false,
		}
		defer tr.CloseIdleConnections()

		client := &http.Client{Transport: tr}

		_, resp, err := websocket.Dial(t.Context(), "ws://local/ws", &websocket.DialOptions{
			HTTPClient: client,
		})
		if err == nil {
			t.Fatalf("expected unauthorized error")
		}
		if resp == nil {
			t.Fatalf("expected response")
		}
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unexpected status: %d", resp.StatusCode)
		}
	})
}

func TestServiceTooManyRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := service.New(secret.New([]byte("user"), []byte("pass")), 1)
		server, listener := startServer(t, srv)
		defer shutdownServer(server, listener)

		firstSrvConn, firstClientConn := net.Pipe()
		listener.push(firstSrvConn)

		tr1 := &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return firstClientConn, nil
			},
			ForceAttemptHTTP2: false,
		}
		defer tr1.CloseIdleConnections()

		client1 := &http.Client{Transport: tr1}

		ctx := t.Context()

		conn1, resp, err := websocket.Dial(ctx, "ws://user:pass@local/ws", &websocket.DialOptions{
			HTTPClient: client1,
		})
		if err != nil {
			t.Fatalf("first dial: %v (resp=%v)", err, statusOf(resp))
		}
		defer conn1.Close(websocket.StatusNormalClosure, "")

		secondSrvConn, secondClientConn := net.Pipe()
		listener.push(secondSrvConn)

		tr2 := &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return secondClientConn, nil
			},
			ForceAttemptHTTP2: false,
		}
		defer tr2.CloseIdleConnections()

		client2 := &http.Client{Transport: tr2}

		type dialResult struct {
			conn *websocket.Conn
			resp *http.Response
			err  error
		}

		resCh := make(chan dialResult, 1)

		go func() {
			conn, resp, err := websocket.Dial(ctx, "ws://user:pass@local/ws", &websocket.DialOptions{
				HTTPClient: client2,
			})
			resCh <- dialResult{conn: conn, resp: resp, err: err}
		}()

		time.Sleep(time.Second)
		synctest.Wait()

		res := <-resCh
		if res.err == nil {
			t.Fatalf("expected error due to capacity")
		}
		if res.resp == nil {
			t.Fatalf("expected response from capacity check")
		}
		if res.resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("unexpected status: %d", res.resp.StatusCode)
		}
		if res.conn != nil {
			res.conn.Close(websocket.StatusGoingAway, "")
		}

		conn1.Close(websocket.StatusNormalClosure, "")
	})
}

func startServer(t *testing.T, srv *service.Service) (*http.Server, *pipeListener) {
	t.Helper()

	listener := newPipeListener()
	server := &http.Server{
		Handler: http.HandlerFunc(srv.ServeHTTP),
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		err := server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			t.Errorf("serve: %v", err)
		}
	}()

	t.Cleanup(func() {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("server did not stop")
		}
	})

	return server, listener
}

func shutdownServer(server *http.Server, listener *pipeListener) {
	listener.Close()
	server.Close()
}

func statusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

type pipeListener struct {
	conns     chan net.Conn
	closeOnce *sync.Once
	done      chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns:     make(chan net.Conn),
		closeOnce: &sync.Once{},
		done:      make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case <-l.done:
		return nil, net.ErrClosed
	case conn, ok := <-l.conns:
		if !ok {
			return nil, net.ErrClosed
		}
		return conn, nil
	}
}

func (l *pipeListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.done)
		close(l.conns)
	})
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return fakeAddr("pipe")
}

func (l *pipeListener) push(conn net.Conn) {
	select {
	case <-l.done:
		conn.Close()
	case l.conns <- conn:
	}
}

type fakeAddr string

func (addr fakeAddr) Network() string {
	return string(addr)
}

func (addr fakeAddr) String() string {
	return string(addr)
}
