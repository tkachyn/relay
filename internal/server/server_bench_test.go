package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func BenchmarkHTTPJobSubmission(b *testing.B) {
	testServer := httptest.NewServer(New().Handler())
	defer testServer.Close()
	payload := []byte(`{"type":"command","payload":"echo benchmark","priority":1}`)
	client := testServer.Client()

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		request, err := http.NewRequest(http.MethodPost, testServer.URL+"/v1/jobs", bytes.NewReader(payload))
		if err != nil {
			b.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			b.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			b.Fatalf("got status %d", response.StatusCode)
		}
	}
}
