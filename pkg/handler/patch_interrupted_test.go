package handler_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	. "github.com/tus/tusd/pkg/handler"
)

type interruptedReader struct{ err error }

func (r interruptedReader) Read([]byte) (int, error) { return 0, r.err }

func TestPatchUnexpectedEOF(t *testing.T) {
	for _, tc := range []struct {
		name, data, offset string
		bodyErr, storeErr  error
		written            int64
		status             int
	}{
		{"partial", "hello", "8", io.ErrUnexpectedEOF, nil, 5, http.StatusNoContent},
		{"empty", "", "3", io.ErrUnexpectedEOF, nil, 0, http.StatusNoContent},
		{"wrapped", "hello", "8", fmt.Errorf("request body: %w", io.ErrUnexpectedEOF), nil, 5, http.StatusNoContent},
		{"storage error", "hello", "", io.ErrUnexpectedEOF, errors.New("S3 unavailable"), 0, http.StatusInternalServerError},
		{"storage EOF", "hello", "", io.ErrUnexpectedEOF, io.ErrUnexpectedEOF, 0, http.StatusInternalServerError},
		{"short storage write", "hello", "", io.ErrUnexpectedEOF, nil, 2, http.StatusInternalServerError},
		{"other body error", "hello", "", errors.New("read failed"), nil, 5, http.StatusInternalServerError},
	} {
		SubTest(t, tc.name, func(t *testing.T, store *MockFullDataStore, composer *StoreComposer) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			upload := NewMockFullUpload(ctrl)
			store.EXPECT().GetUpload(context.Background(), "upload").Return(upload, nil)
			upload.EXPECT().GetInfo(context.Background()).Return(FileInfo{ID: "upload", Size: 20, Offset: 3}, nil)
			upload.EXPECT().WriteChunk(context.Background(), int64(3), NewReaderMatcher(tc.data)).Return(tc.written, tc.storeErr)

			var logs bytes.Buffer
			h, err := NewUnroutedHandler(Config{StoreComposer: composer, Logger: log.New(&logs, "", 0)})
			require.NoError(t, err)
			body := io.MultiReader(strings.NewReader(tc.data), interruptedReader{tc.bodyErr})
			r := httptest.NewRequest("PATCH", "/upload", body)
			r.ContentLength = 10
			r.Header.Set("Content-Type", "application/offset+octet-stream")
			r.Header.Set("Upload-Offset", "3")
			r.Header.Set("X-Request-ID", "interrupted-request")
			w := httptest.NewRecorder()
			h.PatchFile(w, r)

			require.Equal(t, tc.status, w.Code)
			require.Equal(t, tc.offset, w.Header().Get("Upload-Offset"))
			if tc.storeErr != nil {
				require.Contains(t, w.Body.String(), tc.storeErr.Error())
			}
			var bodyLog string
			for _, line := range strings.Split(logs.String(), "\n") {
				if strings.Contains(line, `event="BodyReadError"`) {
					bodyLog = line
				}
			}
			require.Contains(t, bodyLog, `requestId="interrupted-request"`)
			require.Contains(t, bodyLog, `contentLength="10"`)
			require.Contains(t, bodyLog, fmt.Sprintf(`bytesRead="%d"`, len(tc.data)))
			require.Contains(t, bodyLog, fmt.Sprintf(`bytesWritten="%d"`, tc.written))
		})
	}
}
