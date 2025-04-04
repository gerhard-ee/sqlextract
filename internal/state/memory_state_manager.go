package state

import (
	"sync"
	"time"
)

// MemoryStateManager is an in-memory implementation of the StateManager interface
type MemoryStateManager struct {
	states map[string]*State
	locks  map[string]time.Time
	mu     sync.RWMutex
}

// NewMemoryStateManager creates a new memory-based state manager
func NewMemoryStateManager() *MemoryStateManager {
	return &MemoryStateManager{
		states: make(map[string]*State),
		locks:  make(map[string]time.Time),
	}
}

// GetState retrieves the state for a given table
func (m *MemoryStateManager) GetState(table string) (*State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if state, exists := m.states[table]; exists {
		return state, nil
	}
	return nil, nil
}

// CreateState creates a new state for a table
func (m *MemoryStateManager) CreateState(state *State) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.states[state.Table] = state
	return nil
}

// UpdateState updates the state for a table
func (m *MemoryStateManager) UpdateState(table string, processedRows int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state, exists := m.states[table]; exists {
		state.LastUpdated = time.Now()
		state.ProcessedRows = processedRows
		return nil
	}
	return nil
}

// DeleteState deletes the state for a table
func (m *MemoryStateManager) DeleteState(table string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.states, table)
	return nil
}

// ListStates returns a list of all states
func (m *MemoryStateManager) ListStates() ([]*State, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]*State, 0, len(m.states))
	for _, state := range m.states {
		states = append(states, state)
	}
	return states, nil
}

// LockState attempts to lock a state for a specified duration
func (m *MemoryStateManager) LockState(table string, duration time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if state is already locked
	if lockTime, exists := m.locks[table]; exists && lockTime.After(time.Now()) {
		return false, nil
	}

	// Lock the state
	m.locks[table] = time.Now().Add(duration)
	return true, nil
}

// UnlockState unlocks a state
func (m *MemoryStateManager) UnlockState(table string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.locks, table)
	return nil
}
