package panel

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/vmihailenco/msgpack/v5"
)

func TestGetUserListEmptySnapshot(t *testing.T) {
	msgpackBody, err := msgpack.Marshal(UserListBody{Users: []UserInfo{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, contentType, body string
	}{
		{"JSON", "application/json", `{"users":[]}`},
		{"MessagePack", "application/x-msgpack", string(msgpackBody)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const previousETag = `"previous"`
			const emptyETag = `"empty"`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch tag := r.Header.Get("If-None-Match"); tag {
				case previousETag:
					w.Header().Set("Content-Type", tc.contentType)
					w.Header().Set("ETag", emptyETag)
					_, _ = w.Write([]byte(tc.body))
				case emptyETag:
					w.WriteHeader(http.StatusNotModified)
				default:
					t.Errorf("unexpected If-None-Match %q", tag)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client := &Client{client: resty.New().SetBaseURL(server.URL)}

			// Before applying the snapshot, retries must keep using the previous ETag.
			for range 2 {
				users, etag, err := client.GetUserList(t.Context(), previousETag)
				if err != nil || users == nil || len(users) != 0 || etag != emptyETag {
					t.Fatalf("wanted an empty snapshot and its ETag, got %v, %q, %v", users, etag, err)
				}
			}
			users, _, err := client.GetUserList(t.Context(), emptyETag)
			if err != nil || users != nil {
				t.Fatalf("wanted an unchanged result, got %v, %v", users, err)
			}
		})
	}
}

func TestGetUserListRejectsTruncatedSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"users":[]`))
	}))
	defer server.Close()
	client := &Client{client: resty.New().SetBaseURL(server.URL)}
	if _, _, err := client.GetUserList(t.Context(), ""); err == nil {
		t.Fatal("accepted an incomplete user list")
	}
}
