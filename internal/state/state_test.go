package state

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryManager(t *testing.T) {
	manager := NewMemoryManager()

	// Test basic state operations
	t.Run("Basic Operations", func(t *testing.T) {
		// Test CreateState
		state := &State{
			Table:       "test_table",
			LastUpdated: time.Now(),
			Status:      "running",
		}
		err := manager.CreateState(state)
		if err != nil {
			t.Errorf("Failed to create state: %v", err)
		}

		// Test GetState
		got, err := manager.GetState("test_table")
		if err != nil {
			t.Errorf("Failed to get state: %v", err)
		}
		if got.Table != state.Table {
			t.Errorf("Expected table %s, got %s", state.Table, got.Table)
		}

		// Test UpdateState
		err = manager.UpdateState("test_table", 100)
		if err != nil {
			t.Errorf("Failed to update state: %v", err)
		}
		got, err = manager.GetState("test_table")
		if err != nil {
			t.Errorf("Failed to get updated state: %v", err)
		}
		if got.ProcessedRows != 100 {
			t.Errorf("Expected 100 processed rows, got %d", got.ProcessedRows)
		}

		// Test DeleteState
		err = manager.DeleteState("test_table")
		if err != nil {
			t.Errorf("Failed to delete state: %v", err)
		}
		_, err = manager.GetState("test_table")
		if err == nil {
			t.Error("Expected error when getting deleted state")
		}
	})

	// Test concurrent operations
	t.Run("Concurrent Operations", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 10
		numOperations := 100

		// Create state first
		state := &State{
			Table:       "concurrent_table",
			LastUpdated: time.Now(),
			Status:      "running",
		}
		err := manager.CreateState(state)
		if err != nil {
			t.Fatalf("Failed to create state: %v", err)
		}

		// Test concurrent state updates
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(routineID int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					err := manager.UpdateState("concurrent_table", int64(j+1))
					if err != nil {
						t.Errorf("Failed to update state in goroutine %d: %v", routineID, err)
					}
				}
			}(i)
		}
		wg.Wait()

		// Verify final state
		got, err := manager.GetState("concurrent_table")
		if err != nil {
			t.Errorf("Failed to get final state: %v", err)
		}
		if got.ProcessedRows < int64(numOperations) {
			t.Errorf("Expected at least %d processed rows, got %d", numOperations, got.ProcessedRows)
		}
	})

	// Test locking mechanism
	t.Run("Locking Mechanism", func(t *testing.T) {
		// Create state first
		state := &State{
			Table:       "test_table",
			LastUpdated: time.Now(),
			Status:      "running",
		}
		err := manager.CreateState(state)
		if err != nil {
			t.Fatalf("Failed to create state: %v", err)
		}

		// Test LockState
		locked, err := manager.LockState("test_table", 5*time.Second)
		if err != nil {
			t.Errorf("Failed to lock state: %v", err)
		}
		if !locked {
			t.Error("Expected state to be locked")
		}

		// Test concurrent access to locked state
		lockedChan := make(chan bool)
		go func() {
			err := manager.UpdateState("test_table", 200)
			if err == nil {
				lockedChan <- false
			} else {
				lockedChan <- true
			}
		}()

		select {
		case isLocked := <-lockedChan:
			if !isLocked {
				t.Error("Expected state to be locked")
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Timeout waiting for lock test")
		}

		// Test UnlockState
		err = manager.UnlockState("test_table")
		if err != nil {
			t.Errorf("Failed to unlock state: %v", err)
		}

		// Verify state can be updated after unlock
		err = manager.UpdateState("test_table", 300)
		if err != nil {
			t.Errorf("Failed to update state after unlock: %v", err)
		}
	})

	// Test error cases
	t.Run("Error Cases", func(t *testing.T) {
		// Test getting non-existent state
		_, err := manager.GetState("non_existent")
		if err == nil {
			t.Error("Expected error when getting non-existent state")
		}

		// Test updating non-existent state
		err = manager.UpdateState("non_existent", 100)
		if err == nil {
			t.Error("Expected error when updating non-existent state")
		}

		// Test deleting non-existent state
		err = manager.DeleteState("non_existent")
		if err == nil {
			t.Error("Expected error when deleting non-existent state")
		}

		// Test locking non-existent state
		var locked bool
		locked, err = manager.LockState("non_existent", 5*time.Second)
		if err == nil {
			t.Error("Expected error when locking non-existent state")
		}
		if locked {
			t.Error("Expected state to not be locked")
		}
	})
}
