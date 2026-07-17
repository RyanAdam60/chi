package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type blockingReader struct {
	closeChan chan struct{}
	once      sync.Once
}

func newBlockingReader() *blockingReader {
	return &blockingReader{
		closeChan: make(chan struct{}),
	}
}

func (r *blockingReader) Read(p []byte) (n int, err error) {
	<-r.closeChan
	return 0, io.EOF
}

func (r *blockingReader) Close() error {
	r.once.Do(func() {
		close(r.closeChan)
	})
	return nil
}

func TestTimeoutBlockedReader(t *testing.T) {
	t.Parallel()

	handler := Timeout(50 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if err == nil {
			t.Error("expected error reading body, got nil")
		}
	}))

	req, err := http.NewRequest("POST", "/", newBlockingReader())
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("test timed out; handler goroutine is likely leaked/blocked")
	case <-done:
	}

	if rec.Code != http.StatusGatewayTimeout {
		t.Errorf("expected status %d, got %d", http.StatusGatewayTimeout, rec.Code)
	}
}
