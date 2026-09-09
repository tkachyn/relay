package dashboard

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesEmbeddedDashboard(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/dashboard/", nil)
	response := httptest.NewRecorder()
	Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("got status %d, expected %d", response.Code, http.StatusOK)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "Relay Dashboard") {
		t.Fatalf("dashboard title missing from response: %s", body)
	}
}
