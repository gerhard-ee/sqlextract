package state

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// KubernetesManager implements the Manager interface using Kubernetes ConfigMaps
type KubernetesManager struct {
	client    *kubernetes.Clientset
	namespace string
}

func NewKubernetesManager(namespace string) (*KubernetesManager, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %v", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %v", err)
	}

	return &KubernetesManager{
		client:    client,
		namespace: namespace,
	}, nil
}

func (k *KubernetesManager) GetState(table string) (*State, error) {
	ctx := context.Background()
	cm, err := k.client.CoreV1().ConfigMaps(k.namespace).Get(ctx, fmt.Sprintf("sqlextract-state-%s", table), metav1.GetOptions{})
	if err != nil {
		return nil, nil // Return nil if ConfigMap doesn't exist
	}

	var state State
	if err := json.Unmarshal([]byte(cm.Data["state"]), &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state: %v", err)
	}

	return &state, nil
}

func (k *KubernetesManager) UpdateState(table string, processedRows int64) error {
	ctx := context.Background()
	state, err := k.GetState(table)
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

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("sqlextract-state-%s", table),
		},
		Data: map[string]string{
			"state": string(data),
		},
	}

	_, err = k.client.CoreV1().ConfigMaps(k.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		// If update fails because ConfigMap doesn't exist, create it
		_, err = k.client.CoreV1().ConfigMaps(k.namespace).Create(ctx, cm, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create ConfigMap: %v", err)
		}
	}

	return nil
}

func (k *KubernetesManager) CreateState(state *State) error {
	ctx := context.Background()
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("sqlextract-state-%s", state.Table),
		},
		Data: map[string]string{
			"state": string(data),
		},
	}

	_, err = k.client.CoreV1().ConfigMaps(k.namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create ConfigMap: %v", err)
	}

	return nil
}

func (k *KubernetesManager) DeleteState(jobID string) error {
	ctx := context.Background()
	err := k.client.CoreV1().ConfigMaps(k.namespace).Delete(ctx, fmt.Sprintf("sqlextract-state-%s", jobID), metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete ConfigMap: %v", err)
	}

	return nil
}

func (k *KubernetesManager) ListStates() ([]*State, error) {
	ctx := context.Background()
	list, err := k.client.CoreV1().ConfigMaps(k.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=sqlextract",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list ConfigMaps: %v", err)
	}

	var states []*State
	for _, cm := range list.Items {
		var state State
		if err := json.Unmarshal([]byte(cm.Data["state"]), &state); err != nil {
			continue // Skip invalid states
		}
		states = append(states, &state)
	}

	return states, nil
}

func (k *KubernetesManager) LockState(jobID string, duration time.Duration) (bool, error) {
	ctx := context.Background()
	lockKey := fmt.Sprintf("sqlextract-lock-%s", jobID)
	lock := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: lockKey,
			Annotations: map[string]string{
				"expires": time.Now().Add(duration).Format(time.RFC3339),
			},
		},
		Data: map[string]string{
			"locked_at": time.Now().Format(time.RFC3339),
		},
	}

	_, err := k.client.CoreV1().ConfigMaps(k.namespace).Create(ctx, lock, metav1.CreateOptions{})
	if err != nil {
		return false, nil // Lock already exists
	}

	return true, nil
}

func (k *KubernetesManager) UnlockState(jobID string) error {
	ctx := context.Background()
	lockKey := fmt.Sprintf("sqlextract-lock-%s", jobID)
	err := k.client.CoreV1().ConfigMaps(k.namespace).Delete(ctx, lockKey, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete lock: %v", err)
	}

	return nil
}
