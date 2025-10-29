package updater

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"sync/atomic"
	"time"

	telegram "github.com/OvyFlash/telegram-bot-api"
	"github.com/ninedraft/tgbotstream/internal/withtmt"
)

type Bot interface {
	GetUpdatesWithContext(ctx context.Context, config telegram.UpdateConfig) ([]telegram.Update, error)
}

type Publisher interface {
	Publish(ctx context.Context, msg any) error
}

type SleepFunc func(context.Context, time.Duration) bool

type Config struct {
	Bot               Bot
	Publisher         Publisher
	Offset            *atomic.Int64
	GetUpdatesTimeout time.Duration
	RetrySleep        time.Duration
	Sleep             SleepFunc
}

const retryCap = 5 * time.Minute

func Run(ctx context.Context, cfg Config) {
	sleepFn := cfg.Sleep
	if sleepFn == nil {
		sleepFn = Sleep
	}

	onFailSleep := cfg.RetrySleep

	for {
		updates, err := withtmt.TimeoutValues(ctx, 2*cfg.GetUpdatesTimeout,
			func(ctx context.Context) ([]telegram.Update, error) {
				return cfg.Bot.GetUpdatesWithContext(ctx, telegram.UpdateConfig{
					Offset:  int(cfg.Offset.Load()),
					Limit:   100,
					Timeout: max(1, int(cfg.GetUpdatesTimeout/time.Second)),
				})
			})

		if err != nil {
			err = stripHTTPErr(err)
			onFailSleep = min(2*onFailSleep, retryCap)
			slog.ErrorContext(ctx, "getting updates", "error", err, "retry_after", onFailSleep)

			if !sleepFn(ctx, onFailSleep) {
				return
			}

			continue
		}
		onFailSleep = cfg.RetrySleep

		slog.InfoContext(ctx, "got updates", "n_updates", len(updates))

		if len(updates) > 0 {
			cfg.Offset.Store(int64(updates[len(updates)-1].UpdateID + 1))
		}

		for _, update := range updates {
			err := cfg.Publisher.Publish(ctx, update)
			if err != nil {
				onFailSleep = min(2*onFailSleep, retryCap)
				slog.ErrorContext(ctx, "publishing update", "error", err, "retry_after", onFailSleep)

				if !sleepFn(ctx, onFailSleep) {
					return
				}

				continue
			}
			onFailSleep = cfg.RetrySleep
		}
	}
}

func Sleep(ctx context.Context, dt time.Duration) bool {
	select {
	case <-time.After(dt):
		return true
	case <-ctx.Done():
		return false
	}
}

func stripHTTPErr(err error) error {
	var ue *url.Error
	if !errors.As(err, &ue) {
		return err
	}

	// removing URL info - telegram token is passed as part of URL path, so we dropping it
	return ue.Unwrap()
}
