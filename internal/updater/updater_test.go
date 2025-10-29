package updater_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	telegram "github.com/OvyFlash/telegram-bot-api"
	"github.com/ninedraft/tgbotstream/internal/updater"
)

func TestRunRetriesGetUpdates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		offset := &atomic.Int64{}

		bot := &fakeBot{
			responses: []botResponse{
				{err: errors.New("fail 1")},
				{err: errors.New("fail 2")},
				{after: cancel},
			},
		}

		pub := &fakePublisher{}

		var (
			mu     sync.Mutex
			sleeps []time.Duration
		)

		sleep := func(ctx context.Context, dt time.Duration) bool {
			if ctx.Err() != nil {
				return false
			}
			mu.Lock()
			sleeps = append(sleeps, dt)
			mu.Unlock()
			return updater.Sleep(ctx, dt)
		}

		done := make(chan struct{})
		go func() {
			updater.Run(ctx, updater.Config{
				Bot:               bot,
				Publisher:         pub,
				Offset:            offset,
				GetUpdatesTimeout: time.Second,
				RetrySleep:        5 * time.Second,
				Sleep:             sleep,
			})
			close(done)
		}()

		synctest.Wait()
		synctest.Wait()
		<-done

		mu.Lock()
		defer mu.Unlock()

		want := []time.Duration{10 * time.Second, 20 * time.Second}
		if !slices.Equal(sleeps, want) {
			t.Fatalf("sleep durations = %v, want %v", sleeps, want)
		}
	})
}

func TestRunRetriesPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		offset := &atomic.Int64{}

		update1 := telegram.Update{UpdateID: 1}
		update2 := telegram.Update{UpdateID: 2}

		bot := &fakeBot{
			responses: []botResponse{
				{updates: []telegram.Update{update1}},
				{updates: []telegram.Update{update2}, after: cancel},
			},
		}

		pub := &fakePublisher{
			errs: []error{
				errors.New("publish fail"),
				nil,
			},
		}

		var (
			mu     sync.Mutex
			sleeps []time.Duration
		)

		sleep := func(ctx context.Context, dt time.Duration) bool {
			if ctx.Err() != nil {
				return false
			}
			mu.Lock()
			sleeps = append(sleeps, dt)
			mu.Unlock()
			return updater.Sleep(ctx, dt)
		}

		done := make(chan struct{})
		go func() {
			updater.Run(ctx, updater.Config{
				Bot:               bot,
				Publisher:         pub,
				Offset:            offset,
				GetUpdatesTimeout: time.Second,
				RetrySleep:        5 * time.Second,
				Sleep:             sleep,
			})
			close(done)
		}()

		synctest.Wait()
		synctest.Wait()
		<-done

		if got := pub.calls.Load(); got != 2 {
			t.Fatalf("publish calls = %d, want 2", got)
		}

		mu.Lock()
		defer mu.Unlock()

		want := []time.Duration{10 * time.Second}
		if !slices.Equal(sleeps, want) {
			t.Fatalf("sleep durations = %v, want %v", sleeps, want)
		}
	})
}

type botResponse struct {
	updates []telegram.Update
	err     error
	after   func()
}

type fakeBot struct {
	mu        sync.Mutex
	responses []botResponse
	index     int
}

func (bot *fakeBot) GetUpdatesWithContext(ctx context.Context, config telegram.UpdateConfig) ([]telegram.Update, error) {
	bot.mu.Lock()
	defer bot.mu.Unlock()

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if bot.index >= len(bot.responses) {
		return nil, context.Canceled
	}

	resp := bot.responses[bot.index]
	bot.index++

	if resp.after != nil {
		resp.after()
	}

	return resp.updates, resp.err
}

type fakePublisher struct {
	mu    sync.Mutex
	errs  []error
	index int
	calls atomic.Int64
}

func (pub *fakePublisher) Publish(context.Context, any) error {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	pub.calls.Add(1)

	if pub.index >= len(pub.errs) {
		return nil
	}
	err := pub.errs[pub.index]
	pub.index++
	return err
}
