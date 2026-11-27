package s3store

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/tus/tusd/pkg/handler"
)

func TestInterruptedPatchResume(t *testing.T) {
	// Exercise the real HTTP body parser, handler and S3 store. Only the S3 API
	// is replaced, so the final object can be compared without a live bucket.
	payload := bytes.Repeat([]byte("0123456789abcdef"), 15*1024*1024/16)
	for _, received := range []int{0, 65536, 6 * 1024 * 1024} {
		t.Run(strconv.Itoa(received), func(t *testing.T) {
			backend := &resumeS3{
				objects: map[string][]byte{"upload.info": []byte(fmt.Sprintf(`{"ID":"upload+multipart","Size":%d}`, len(payload)))},
			}
			store := New("bucket", backend)
			store.TemporaryDirectory = t.TempDir()
			composer := handler.NewStoreComposer()
			store.UseIn(composer)
			finished := 0
			h, err := handler.NewUnroutedHandler(handler.Config{
				StoreComposer:         composer,
				NotifyCompleteUploads: true,
				PreFinishResponseCallback: func(event *handler.CallbackEvent) error {
					finished++
					return nil
				},
			})
			require.NoError(t, err)
			h.CompleteUploads = make(chan handler.HookEvent, 1)

			// The declared Content-Length exceeds the bytes sent. net/http returns
			// io.ErrUnexpectedEOF just as it does for a disconnected HTTP client.
			headers := fmt.Sprintf("PATCH /upload+multipart HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/offset+octet-stream\r\nUpload-Offset: 0\r\nContent-Length: %d\r\n\r\n", len(payload))
			r, err := http.ReadRequest(bufio.NewReader(io.MultiReader(strings.NewReader(headers), bytes.NewReader(payload[:received]))))
			require.NoError(t, err)
			w := httptest.NewRecorder()
			h.PatchFile(w, r)
			require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
			require.Equal(t, strconv.Itoa(received), w.Header().Get("Upload-Offset"))
			require.Equal(t, 0, finished)
			require.Empty(t, h.CompleteUploads)
			require.False(t, backend.completed)

			w = httptest.NewRecorder()
			h.HeadFile(w, httptest.NewRequest("HEAD", "/upload+multipart", nil))
			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, strconv.Itoa(received), w.Header().Get("Upload-Offset"))

			if received > 0 {
				r = httptest.NewRequest("PATCH", "/upload+multipart", bytes.NewReader(payload))
				r.Header.Set("Content-Type", "application/offset+octet-stream")
				r.Header.Set("Upload-Offset", "0")
				w = httptest.NewRecorder()
				h.PatchFile(w, r)
				require.Equal(t, http.StatusConflict, w.Code)
			}

			r = httptest.NewRequest("PATCH", "/upload+multipart", bytes.NewReader(payload[received:]))
			r.Header.Set("Content-Type", "application/offset+octet-stream")
			r.Header.Set("Upload-Offset", strconv.Itoa(received))
			w = httptest.NewRecorder()
			h.PatchFile(w, r)
			require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
			require.Equal(t, strconv.Itoa(len(payload)), w.Header().Get("Upload-Offset"))
			require.Equal(t, 1, finished)
			require.Len(t, h.CompleteUploads, 1)
			require.EqualValues(t, len(payload), (<-h.CompleteUploads).Upload.Offset)
			require.True(t, backend.completed)
			require.Equal(t, sha256.Sum256(payload), sha256.Sum256(backend.objects["upload"]))
		})
	}
}

// In-memory S3 responses retain the bytes written by S3Store across requests.
// Unexpected API calls fail through the embedded nil interface.
type resumeS3 struct {
	S3API
	objects   map[string][]byte
	parts     [][]byte
	completed bool
}

func (s *resumeS3) GetObjectWithContext(_ context.Context, in *s3.GetObjectInput, _ ...request.Option) (*s3.GetObjectOutput, error) {
	data, ok := s.objects[*in.Key]
	if !ok {
		return nil, awserr.New("NoSuchKey", "missing", nil)
	}
	return &s3.GetObjectOutput{Body: ioutil.NopCloser(bytes.NewReader(data)), ContentLength: aws.Int64(int64(len(data)))}, nil
}

func (s *resumeS3) PutObjectWithContext(_ context.Context, in *s3.PutObjectInput, _ ...request.Option) (*s3.PutObjectOutput, error) {
	data, err := ioutil.ReadAll(in.Body)
	s.objects[*in.Key] = data
	return &s3.PutObjectOutput{}, err
}

func (s *resumeS3) DeleteObjectWithContext(_ context.Context, in *s3.DeleteObjectInput, _ ...request.Option) (*s3.DeleteObjectOutput, error) {
	delete(s.objects, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (s *resumeS3) ListPartsWithContext(_ context.Context, _ *s3.ListPartsInput, _ ...request.Option) (*s3.ListPartsOutput, error) {
	if s.completed {
		return nil, awserr.New("NoSuchUpload", "completed", nil)
	}
	out := &s3.ListPartsOutput{}
	for i, data := range s.parts {
		out.Parts = append(out.Parts, &s3.Part{PartNumber: aws.Int64(int64(i + 1)), Size: aws.Int64(int64(len(data))), ETag: aws.String(strconv.Itoa(i + 1))})
	}
	return out, nil
}

func (s *resumeS3) UploadPartWithContext(_ context.Context, in *s3.UploadPartInput, _ ...request.Option) (*s3.UploadPartOutput, error) {
	data, err := ioutil.ReadAll(in.Body)
	if int(*in.PartNumber) != len(s.parts)+1 {
		return nil, fmt.Errorf("unexpected part number %d", *in.PartNumber)
	}
	s.parts = append(s.parts, data)
	return &s3.UploadPartOutput{ETag: aws.String(strconv.Itoa(len(s.parts)))}, err
}

func (s *resumeS3) CompleteMultipartUploadWithContext(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...request.Option) (*s3.CompleteMultipartUploadOutput, error) {
	var data []byte
	for i, part := range in.MultipartUpload.Parts {
		chunk := s.parts[*part.PartNumber-1]
		if i < len(in.MultipartUpload.Parts)-1 && len(chunk) < 5*1024*1024 {
			return nil, fmt.Errorf("non-final part smaller than S3 minimum")
		}
		data = append(data, chunk...)
	}
	s.objects[*in.Key] = data
	s.completed = true
	return &s3.CompleteMultipartUploadOutput{}, nil
}
