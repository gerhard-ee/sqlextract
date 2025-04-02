package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStateManager implements the Manager interface using file-based storage
type FileStateManager struct {
	baseDir string
	mu      sync.RWMutex
}

// NewFileStateManager creates a new file-based state manager
func NewFileStateManager(baseDir string) Manager {
	return &FileStateManager{
		baseDir: baseDir,
	}
}

func (m *FileStateManager) GetState(table string) (*State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stateFile := filepath.Join(m.baseDir, fmt.Sprintf("%s.state", table))
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read state file: %v", err)
	}

	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %v", err)
	}

	return &state, nil
}

func (m *FileStateManager) UpdateState(table string, processedRows int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.GetState(table)
	if err != nil {
		return err
	}
	if state == nil {
		state = &State{
			Table:       table,
			LastUpdated: time.Now(),
			Status:      "running",
		}
	}

	state.ProcessedRows = processedRows
	state.LastUpdated = time.Now()

	return m.saveState(state)
}

func (m *FileStateManager) CreateState(state *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existingState, err := m.GetState(state.Table)
	if err != nil {
		return err
	}
	if existingState != nil {
		return fmt.Errorf("state already exists for table: %s", state.Table)
	}

	return m.saveState(state)
}

func (m *FileStateManager) DeleteState(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	states, err := m.ListStates()
	if err != nil {
		return err
	}

	for _, state := range states {
		if state.JobID == jobID {
			stateFile := filepath.Join(m.baseDir, fmt.Sprintf("%s.state", state.Table))
			if err := os.Remove(stateFile); err != nil {
				return fmt.Errorf("failed to delete state file: %v", err)
			}
			return nil
		}
	}

	return fmt.Errorf("state not found for job ID: %s", jobID)
}

func (m *FileStateManager) ListStates() ([]*State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read state directory: %v", err)
	}

	var states []*State
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".state" {
			data, err := os.ReadFile(filepath.Join(m.baseDir, entry.Name()))
			if err != nil {
				continue
			}

			var state State
			if err := json.Unmarshal(data, &state); err != nil {
				continue
			}
			states = append(states, &state)
		}
	}

	return states, nil
}

func (m *FileStateManager) LockState(jobID string, duration time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	lockFile := filepath.Join(m.baseDir, fmt.Sprintf("%s.lock", jobID))
	if _, err := os.Stat(lockFile); err == nil {
		// Lock file exists, check if it's expired
		data, err := os.ReadFile(lockFile)
		if err != nil {
			return false, fmt.Errorf("failed to read lock file: %v", err)
		}

		var lockTime time.Time
		if err := json.Unmarshal(data, &lockTime); err != nil {
			return false, fmt.Errorf("failed to unmarshal lock time: %v", err)
		}

		if lockTime.After(time.Now()) {
			return false, nil
		}
	}

	// Create or update lock file
	lockTime := time.Now().Add(duration)
	data, err := json.Marshal(lockTime)
	if err != nil {
		return false, fmt.Errorf("failed to marshal lock time: %v", err)
	}

	if err := os.WriteFile(lockFile, data, 0644); err != nil {
		return false, fmt.Errorf("failed to write lock file: %v", err)
	}

	return true, nil
}

func (m *FileStateManager) UnlockState(jobID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	lockFile := filepath.Join(m.baseDir, fmt.Sprintf("%s.lock", jobID))
	if _, err := os.Stat(lockFile); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no lock found for job ID: %s", jobID)
		}
		return fmt.Errorf("failed to check lock file: %v", err)
	}

	if err := os.Remove(lockFile); err != nil {
		return fmt.Errorf("failed to remove lock file: %v", err)
	}

	return nil
}

func (m *FileStateManager) saveState(state *State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	stateFile := filepath.Join(m.baseDir, fmt.Sprintf("%s.state", state.Table))
	if err := os.WriteFile(stateFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %v", err)
	}

	return nil
}
