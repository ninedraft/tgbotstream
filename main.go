package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ninedraft/tgbotstream/internal/service"
	"github.com/ninedraft/tgbotstream/internal/updater"
	. "github.com/ninedraft/tgbotstream/internal/withtmt"
	"github.com/ninedraft/tgbotstream/secret"

	telegram "github.com/OvyFlash/telegram-bot-api"
	"github.com/charmbracelet/log"
	colorlog "github.com/charmbracelet/log"
	istty "github.com/cristalhq/istty"
)

func run() (int, error) {
	telegramTokenFile := cmp.Or(os.Getenv("TELEGRAM_TOKEN_FILE"), "./token")

	authSecretFile := cmp.Or(os.Getenv("AUTH_SECRET_FILE"), "./auth_secret")

	getUpdatesTimeout := 5 * time.Minute
	flag.DurationVar(&getUpdatesTimeout, "updates-timeout", getUpdatesTimeout, "Timeout for getting updates from Telegram API")

	retrySleep := 5 * time.Second
	flag.DurationVar(&retrySleep, "retry-sleep", retrySleep, "Sleep duration before retrying after a failed publish")

	listenAddr := "localhost:4201"

	connCap := 16
	flag.IntVar(&connCap, "conncap", connCap, "maximum concurrent websocket connections")

	logLevelEnv := os.Getenv("LOG_LEVEL")
	logLevelDefault := cmp.Or(logLevelEnv, "info")
	logLevel := flag.String("log-level", logLevelDefault, "log level")

	logFormat := cmp.Or(strings.TrimSpace(os.Getenv("LOG_FORMAT")), "auto")
	flag.StringVar(&logFormat, "log-format", logFormat, "log format (text, text-color, auto, json)")

	flag.Parse()

	level, err := colorlog.ParseLevel(*logLevel)
	if err != nil {
		return 1, fmt.Errorf("parsing log level %q: %w", *logLevel, err)
	}
	slogLevel := slog.Level(int(level))

	lg := logHandler(logFormat, slogLevel)
	slog.SetDefault(slog.New(lg))
	slog.SetLogLoggerLevel(slogLevel)

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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	botApi, err := telegram.NewBotAPI(token)
	if err != nil {
		return 1, fmt.Errorf("creating telegram bot API: %w", err)
	}

	offset := &atomic.Int64{}

	mux := http.NewServeMux()

	srv := service.New(authSecret, connCap)

	mux.Handle("/ws", srv)

	go func() {
		defer cancel()

		updater.Run(ctx, updater.Config{
			Bot:               botApi,
			Publisher:         srv,
			Offset:            offset,
			GetUpdatesTimeout: getUpdatesTimeout,
			RetrySleep:        retrySleep,
		})
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

func logHandler(cfgFormat string, level slog.Leveler) slog.Handler {
	cfgFormat = strings.ToLower(strings.TrimSpace(cfgFormat))

	switch {
	case slices.Contains([]string{"", "auto"}, cfgFormat) && istty.IsTerminal(os.Stderr.Fd()), cfgFormat == "text-color":
		return colorlog.NewWithOptions(os.Stdout, colorlog.Options{
			Level:     colorLevels[level.Level()],
			Formatter: colorlog.TextFormatter,
		})

	case cfgFormat == "", cfgFormat == "text":
		return slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
		})
	case cfgFormat == "json":
		return slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
		})
	default:
		log.Printf("WARN: unsupported log format, fallback to text")
		return logHandler("text", level)
	}
}

var colorLevels = map[slog.Level]colorlog.Level{
	slog.LevelDebug: colorlog.DebugLevel,
	slog.LevelInfo:  colorlog.InfoLevel,
	slog.LevelWarn:  colorlog.WarnLevel,
	slog.LevelError: colorlog.ErrorLevel,
}
