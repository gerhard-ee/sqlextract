package state

import (
	"fmt"
	"sync"
	"time"
)

// State represents the current state of a data extraction job
type State struct {
	JobID         string
	Table         string
	LastOffset    int64
	LastValues    []interface{}
	LastUpdated   time.Time
	TotalRows     int64
	ProcessedRows int64
	Status        string // "running", "completed", "failed"
	Error         string
}

// Manager defines the interface for state management
type Manager interface {
	GetState(table string) (*State, error)
	UpdateState(table string, processedRows int64) error
	CreateState(state *State) error
	DeleteState(jobID string) error
	ListStates() ([]*State, error)
	LockState(jobID string, duration time.Duration) (bool, error)
	UnlockState(jobID string) error
}

// MemoryManager implements the Manager interface using in-memory storage
// This is useful for testing and single-instance deployments
type MemoryManager struct {
	states map[string]*State
	locks  map[string]time.Time
	mu     sync.RWMutex
}

// NewMemoryManager creates a new in-memory state manager
func NewMemoryManager() Manager {
	return &MemoryManager{
		states: make(map[string]*State),
		locks:  make(map[string]time.Time),
	}
}

func (m *MemoryManager) GetState(table string) (*State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.states[table]
	if !exists {
		return nil, fmt.Errorf("state not found for table: %s", table)
	}

	return state, nil
}

func (m *MemoryManager) UpdateState(table string, processedRows int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.states[table]
	if !exists {
		return fmt.Errorf("state not found for table: %s", table)
	}

	// Check if state is locked
	if lockTime, exists := m.locks[table]; exists && lockTime.After(time.Now()) {
		return fmt.Errorf("state is locked for table: %s", table)
	}

	state.ProcessedRows = processedRows
	state.LastUpdated = time.Now()
	return nil
}

func (m *MemoryManager) CreateState(state *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.states[state.Table]; exists {
		return fmt.Errorf("state already exists for table: %s", state.Table)
	}

	m.states[state.Table] = state
	return nil
}

func (m *MemoryManager) DeleteState(table string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.states[table]; !exists {
		return fmt.Errorf("state not found for table: %s", table)
	}

	delete(m.states, table)
	delete(m.locks, table)
	return nil
}

func (m *MemoryManager) ListStates() ([]*State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]*State, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}

	return states, nil
}

func (m *MemoryManager) LockState(table string, duration time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.states[table]; !exists {
		return false, fmt.Errorf("state not found for table: %s", table)
	}

	now := time.Now()
	if lockTime, exists := m.locks[table]; exists && lockTime.After(now) {
		return false, nil
	}

	m.locks[table] = now.Add(duration)
	return true, nil
}

func (m *MemoryManager) UnlockState(table string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.locks[table]; !exists {
		return fmt.Errorf("no lock found for table: %s", table)
	}

	delete(m.locks, table)
	return nil
}
