# Deploying SQLExtract to Kubernetes

This directory contains Kubernetes manifests for deploying SQLExtract in a Kubernetes cluster.

## Prerequisites

- Kubernetes cluster (1.16+)
- `kubectl` configured to communicate with your cluster
- Docker registry access (if using private images)

## Configuration

### 1. Create Database Credentials Secret

First, create a secret containing your database credentials. Replace the placeholders with your base64-encoded values:

```bash
# Encode your values
echo -n "your-host" | base64
echo -n "your-port" | base64
echo -n "your-username" | base64
echo -n "your-password" | base64
echo -n "your-database" | base64
echo -n "your-schema" | base64

# Update the values in secret.yaml and apply
kubectl apply -f secret.yaml
```

### 2. Configure the Application

Update the ConfigMap in `configmap.yaml` with your desired settings:

- `DB_TYPE`: Database type (mssql, postgres, mysql, duckdb, bigquery, snowflake)
- `TABLE_NAME`: Name of the table to extract
- `OUTPUT_FILE`: Path to save the extracted data
- `BATCH_SIZE`: Number of rows to process in each batch
- `LOG_LEVEL`: Logging level (debug, info, warn, error)
- `MAX_RETRIES`: Maximum number of retry attempts
- `RETRY_DELAY`: Delay between retries
- `TIMEOUT`: Maximum time for extraction
- `CHECKPOINT_INTERVAL`: Interval for saving progress

Apply the ConfigMap:

```bash
kubectl apply -f configmap.yaml
```

### 3. Set Up RBAC

Create the necessary service account and RBAC rules:

```bash
kubectl apply -f rbac.yaml
```

### 4. Create Storage

The application requires persistent storage for output files. Apply the PVC:

```bash
kubectl apply -f deployment.yaml
```

### 5. Deploy the Application

Deploy the application:

```bash
kubectl apply -f deployment.yaml
```

## Monitoring

Check the deployment status:

```bash
kubectl get deployments
kubectl get pods
```

View application logs:

```bash
kubectl logs -l app=sqlextract
```

## Scaling

The application is designed to handle distributed state using Kubernetes resources. You can scale the deployment:

```bash
kubectl scale deployment sqlextract --replicas=3
```

## Troubleshooting

1. Check pod status:
   ```bash
   kubectl describe pod -l app=sqlextract
   ```

2. Check application logs:
   ```bash
   kubectl logs -l app=sqlextract
   ```

3. Verify ConfigMap and Secret:
   ```bash
   kubectl describe configmap sqlextract-config
   kubectl describe secret db-credentials
   ```

4. Check PVC status:
   ```bash
   kubectl describe pvc sqlextract-data-pvc
   ```

## Cleanup

To remove all resources:

```bash
kubectl delete -f deployment.yaml
kubectl delete -f configmap.yaml
kubectl delete -f rbac.yaml
``` 