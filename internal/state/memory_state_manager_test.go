package state

import (
	"testing"
	"time"
)

func TestMemoryStateManager(t *testing.T) {
	manager := NewMemoryStateManager()

	// Test creating a new state
	state := &State{
		Table:       "test_table",
		LastUpdated: time.Now(),
		Status:      "running",
	}
	err := manager.CreateState(state)
	if err != nil {
		t.Errorf("CreateState failed: %v", err)
	}

	// Test retrieving the state
	retrievedState, err := manager.GetState("test_table")
	if err != nil {
		t.Errorf("GetState failed: %v", err)
	}
	if retrievedState == nil {
		t.Error("Expected to retrieve state, got nil")
	}
	if retrievedState.Table != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", retrievedState.Table)
	}

	// Test updating the state
	err = manager.UpdateState("test_table", 100)
	if err != nil {
		t.Errorf("UpdateState failed: %v", err)
	}

	// Verify the update
	updatedState, err := manager.GetState("test_table")
	if err != nil {
		t.Errorf("GetState after update failed: %v", err)
	}
	if updatedState.ProcessedRows != 100 {
		t.Errorf("Expected processed rows 100, got %d", updatedState.ProcessedRows)
	}

	// Test deleting the state
	err = manager.DeleteState("test_table")
	if err != nil {
		t.Errorf("DeleteState failed: %v", err)
	}

	// Verify the deletion
	deletedState, err := manager.GetState("test_table")
	if err != nil {
		t.Errorf("GetState after delete failed: %v", err)
	}
	if deletedState != nil {
		t.Error("Expected state to be deleted, but it still exists")
	}
}

func TestMemoryStateManager_Concurrent(t *testing.T) {
	manager := NewMemoryStateManager()
	done := make(chan bool)

	// Start multiple goroutines to test concurrent access
	for i := 0; i < 10; i++ {
		go func(id int) {
			table := "test_table"
			state := &State{
				Table:       table,
				LastUpdated: time.Now(),
				Status:      "running",
			}

			// Create state
			err := manager.CreateState(state)
			if err != nil {
				t.Errorf("CreateState failed in goroutine %d: %v", id, err)
			}

			// Update state multiple times
			for j := 0; j < 100; j++ {
				err = manager.UpdateState(table, int64(j))
				if err != nil {
					t.Errorf("UpdateState failed in goroutine %d: %v", id, err)
				}
			}

			// Get state
			_, err = manager.GetState(table)
			if err != nil {
				t.Errorf("GetState failed in goroutine %d: %v", id, err)
			}

			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
