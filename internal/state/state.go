package state

import (
	"context"
	"fmt"
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
	// GetState retrieves the current state for a job
	GetState(ctx context.Context, jobID string) (*State, error)

	// UpdateState updates the state for a job
	UpdateState(ctx context.Context, state *State) error

	// CreateState creates a new state for a job
	CreateState(ctx context.Context, state *State) error

	// DeleteState removes the state for a job
	DeleteState(ctx context.Context, jobID string) error

	// ListStates retrieves all states for a given table
	ListStates(ctx context.Context, table string) ([]*State, error)

	// LockState acquires a lock on a state for a job
	LockState(ctx context.Context, jobID string, ttl time.Duration) (bool, error)

	// UnlockState releases a lock on a state for a job
	UnlockState(ctx context.Context, jobID string) error
}

// MemoryManager implements the Manager interface using in-memory storage
// This is useful for testing and single-instance deployments
type MemoryManager struct {
	states map[string]*State
	locks  map[string]time.Time
}

// NewMemoryManager creates a new in-memory state manager
func NewMemoryManager() *MemoryManager {
	return &MemoryManager{
		states: make(map[string]*State),
		locks:  make(map[string]time.Time),
	}
}

func (m *MemoryManager) GetState(ctx context.Context, jobID string) (*State, error) {
	if state, exists := m.states[jobID]; exists {
		return state, nil
	}
	return nil, nil
}

func (m *MemoryManager) UpdateState(ctx context.Context, state *State) error {
	m.states[state.JobID] = state
	return nil
}

func (m *MemoryManager) CreateState(ctx context.Context, state *State) error {
	if _, exists := m.states[state.JobID]; exists {
		return fmt.Errorf("state already exists for job %s", state.JobID)
	}
	m.states[state.JobID] = state
	return nil
}

func (m *MemoryManager) DeleteState(ctx context.Context, jobID string) error {
	delete(m.states, jobID)
	delete(m.locks, jobID)
	return nil
}

func (m *MemoryManager) ListStates(ctx context.Context, table string) ([]*State, error) {
	var states []*State
	for _, state := range m.states {
		if state.Table == table {
			states = append(states, state)
		}
	}
	return states, nil
}

func (m *MemoryManager) LockState(ctx context.Context, jobID string, ttl time.Duration) (bool, error) {
	if lockTime, exists := m.locks[jobID]; exists {
		if time.Now().Before(lockTime) {
			return false, nil
		}
	}
	m.locks[jobID] = time.Now().Add(ttl)
	return true, nil
}

func (m *MemoryManager) UnlockState(ctx context.Context, jobID string) error {
	delete(m.locks, jobID)
	return nil
}
