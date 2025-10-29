package withtmt

import (
	"context"
	"time"
)

func Timeout(ctx context.Context, timeout time.Duration, fn func(ctx context.Context)) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fn(ctx)
}

func TimeoutValue[E any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) E) E {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return fn(ctx)
}

func TimeoutValues[A, B any](ctx context.Context, timeout time.Duration, fn func(ctx context.Context) (A, B)) (A, B) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return fn(ctx)
}
