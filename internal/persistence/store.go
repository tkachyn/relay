package persistence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/job"
)

type State struct {
	Jobs    []*job.Job   `json:"jobs"`
	Workers []api.Worker `json:"workers"`
}

type Store struct {
	path string
}

func New(path string) *Store {
	return &Store{path: path}
}

func (s *Store) Load() (State, error) {
	file, err := os.Open(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	defer file.Close()

	var state State
	if err := json.NewDecoder(file).Decode(&state); err != nil {
		return State{}, err
	}
	return state, nil
}

func (s *Store) Save(state State) error {
	dir := filepath.Dir(s.path)
	temp, err := os.CreateTemp(dir, ".relay-state-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)

	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, s.path); err == nil {
		return nil
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(tempPath, s.path)
}
