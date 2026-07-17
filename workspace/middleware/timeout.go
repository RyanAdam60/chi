package middleware

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// Timeout is a middleware that cancels ctx after dev duration and provides
// a timeout response.
func Timeout(timeout time.Duration) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			r = r.WithContext(ctx)
			if r.Body != nil {
				r.Body = &contextReader{ctx: ctx, rc: r.Body}
			}

			done := make(chan struct{})
			panicChan := make(chan interface{}, 1)

			go func() {
				defer func() {
					if p := recover(); p != nil {
						panicChan <- p
					}
				}()
				next.ServeHTTP(w, r)
				close(done)
			}()

			select {
			case p := <-panicChan:
				panic(p)
			case <-done:
			case <-ctx.Done():
				w.WriteHeader(http.StatusGatewayTimeout)
				w.Write([]byte(http.StatusText(http.StatusGatewayTimeout)))
				if r.Body != nil {
					r.Body.Close()
				}
			}
		}
		return http.HandlerFunc(fn)
	}
}

type contextReader struct {
	ctx  context.Context
	rc   io.ReadCloser
	once sync.Once
	err  error
}

func (cr *contextReader) Read(p []byte) (n int, err error) {
	if err := cr.ctx.Err(); err != nil {
		return 0, err
	}
	n, err = cr.rc.Read(p)
	if err != nil {
		if ctxErr := cr.ctx.Err(); ctxErr != nil {
			return n, ctxErr
		}
		return n, err
	}
	if err := cr.ctx.Err(); err != nil {
		return n, err
	}
	return n, nil
}

func (cr *contextReader) Close() error {
	cr.once.Do(func() {
		cr.err = cr.rc.Close()
	})
	return cr.err
}
