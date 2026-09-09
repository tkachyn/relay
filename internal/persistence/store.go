package persistence

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/tkachyn/relay/internal/api"
	"github.com/tkachyn/relay/internal/job"
)

// state is the complete snapshot needed to restart a server
type State struct {
	Jobs    []*job.Job   `json:"jobs"`
	Workers []api.Worker `json:"workers"`
}

// store writes snapshots beside the target before replacing it
type Store struct {
	path string
}

// new creates a file-backed state store
func New(path string) *Store {
	return &Store{path: path}
}

// load reads the last complete snapshot or returns an empty state
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

// save flushes a complete snapshot before replacing the previous file
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
