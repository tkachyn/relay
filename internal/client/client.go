package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/job"
)

type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) Submit(request api.CreateJobRequest) (*job.Job, error) {
	var result job.Job
	err := c.do(http.MethodPost, "/v1/jobs", request, http.StatusCreated, &result)
	return &result, err
}

func (c *Client) ListJobs() ([]*job.Job, error) {
	var response api.JobListResponse
	if err := c.do(http.MethodGet, "/v1/jobs", nil, http.StatusOK, &response); err != nil {
		return nil, err
	}
	return response.Jobs, nil
}

func (c *Client) GetJob(id string) (*job.Job, error) {
	var result job.Job
	if err := c.do(http.MethodGet, "/v1/jobs/"+id, nil, http.StatusOK, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) CancelJob(id string) (*job.Job, error) {
	var result job.Job
	if err := c.do(http.MethodPost, "/v1/jobs/"+id+"/cancel", nil, http.StatusOK, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) RegisterWorker(id string) (*api.Worker, error) {
	var result api.Worker
	err := c.do(http.MethodPost, "/v1/workers/register", api.RegisterWorkerRequest{ID: id}, http.StatusOK, &result)
	return &result, err
}

func (c *Client) ClaimJob(id string) (*job.Job, bool, error) {
	request, err := http.NewRequest(http.MethodPost, c.BaseURL+"/v1/workers/"+id+"/claim", nil)
	if err != nil {
		return nil, false, err
	}
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, false, err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNoContent {
		return nil, false, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, false, c.responseError(response)
	}

	var result job.Job
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, false, err
	}
	return &result, true, nil
}

func (c *Client) ReportResult(id string, request api.ResultRequest) (*job.Job, error) {
	var result job.Job
	err := c.do(http.MethodPost, "/v1/jobs/"+id+"/result", request, http.StatusOK, &result)
	return &result, err
}

func (c *Client) ListWorkers() ([]api.Worker, error) {
	var response api.WorkerListResponse
	if err := c.do(http.MethodGet, "/v1/workers", nil, http.StatusOK, &response); err != nil {
		return nil, err
	}
	return response.Workers, nil
}

func (c *Client) do(method, path string, body any, expectedStatus int, result any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, c.BaseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		return c.responseError(response)
	}
	if result == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(result)
}

func (c *Client) responseError(response *http.Response) error {
	var message api.ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&message); err != nil {
		return fmt.Errorf("server returned %s", response.Status)
	}
	return fmt.Errorf("server returned %s: %s", response.Status, message.Error)
}
