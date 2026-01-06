#!/bin/bash

# Local Database Runner for Postgres and Elasticsearch
# This script starts both databases locally and provides connection information

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
CONFIG_FILE="config/benchmark_config.json"
SCALE=$(python3 scripts/config_reader.py "$CONFIG_FILE" "benchmark.scale" "small")

# Define scale-prefixed directories
DATA_DIR="data"
RESULTS_DIR="results"

# Load resource defaults from config
CPU=$(python3 scripts/config_reader.py "$CONFIG_FILE" "resources.postgres.cpu_request")
MEMORY=$(python3 scripts/config_reader.py "$CONFIG_FILE" "resources.postgres.memory_request")
JVM_OPTS=$(python3 scripts/config_reader.py "$CONFIG_FILE" "resources.elasticsearch.jvm_opts")

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."

    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl not found - required for local database setup"
        exit 1
    fi

    # Build benchmark runner image
    print_info "Building benchmark runner Docker image..."
    if docker build -t benchmark-runner:latest . > /dev/null; then
        print_success "Benchmark runner image built successfully"
    else
        print_error "Failed to build benchmark runner image"
        exit 1
    fi

    print_success "Prerequisites check passed"
}

# Function to clean database data
cleanup_database_data() {
    print_info "Cleaning up database data..."
    kubectl delete pvc postgres-pvc --ignore-not-found=true
    kubectl delete pvc elasticsearch-pvc --ignore-not-found=true
    # Wait a moment to ensure deletion propagates
    sleep 2
}

# Function to generate data on host
generate_data_on_host() {
    print_info "Generating data on host for scale: $SCALE"
    
    mkdir -p "$DATA_DIR"
    
    if [[ ! -f "$DATA_DIR/documents_${SCALE}.json" ]]; then
        python3 scripts/generate_synthetic_data.py "$SCALE" > "$DATA_DIR/documents_${SCALE}.json"
        print_success "Data generated and saved to $DATA_DIR/documents_${SCALE}.json"
    else
        print_info "Data file $DATA_DIR/documents_${SCALE}.json already exists"
    fi

    if [[ ! -f "$DATA_DIR/documents_child_${SCALE}.json" ]]; then
        python3 scripts/generate_synthetic_data.py "$SCALE" --mode child > "$DATA_DIR/documents_child_${SCALE}.json"
        print_success "Child data generated and saved to $DATA_DIR/documents_child_${SCALE}.json"
    else
        print_info "Child data file $DATA_DIR/documents_child_${SCALE}.json already exists"
    fi
}

# Function to update Kubernetes YAML files with resource specifications
update_k8s_resources() {
    print_info "Updating Kubernetes resource specifications..."

    # Copy templates to deployment files
    cp k8s/benchmark-runner-deployment-template.yaml k8s/benchmark-runner-deployment.yaml
    cp k8s/postgres-deployment-template.yaml k8s/postgres-deployment.yaml
    cp k8s/elasticsearch-deployment-template.yaml k8s/elasticsearch-deployment.yaml

    # Compute absolute paths relative to current directory
    DATA_PATH="$(pwd)/$DATA_DIR"
    SCRIPT_PATH="$(pwd)/scripts"
    CONFIG_PATH="$(pwd)/config"
    RESULTS_PATH="$(pwd)/$RESULTS_DIR"

    # Ensure results directory exists (required for hostPath mounts)
    if [[ ! -d "$RESULTS_PATH" ]]; then
        print_info "Creating results directory: $RESULTS_PATH"
        mkdir -p "$RESULTS_PATH"
    fi

    # Verify directories exist
    if [[ ! -d "$DATA_PATH" ]]; then
        print_error "Data directory not found: $DATA_PATH"
        exit 1
    fi
    if [[ ! -d "$SCRIPT_PATH" ]]; then
        print_error "Scripts directory not found: $SCRIPT_PATH"
        exit 1
    fi
    if [[ ! -d "$CONFIG_PATH" ]]; then
        print_error "Config directory not found: $CONFIG_PATH"
        exit 1
    fi

    # Replace path placeholders in benchmark-runner deployment
    sed -i '' "s|DATA_PATH_PLACEHOLDER|$DATA_PATH|g" k8s/benchmark-runner-deployment.yaml
    sed -i '' "s|SCRIPT_PATH_PLACEHOLDER|$SCRIPT_PATH|g" k8s/benchmark-runner-deployment.yaml
    sed -i '' "s|CONFIG_PATH_PLACEHOLDER|$CONFIG_PATH|g" k8s/benchmark-runner-deployment.yaml
    sed -i '' "s|RESULTS_PATH_PLACEHOLDER|$RESULTS_PATH|g" k8s/benchmark-runner-deployment.yaml

    # Replace path placeholders in both deployments
    sed -i '' "s|DATA_PATH_PLACEHOLDER|$DATA_PATH|g" k8s/postgres-deployment.yaml
    sed -i '' "s|SCRIPT_PATH_PLACEHOLDER|$SCRIPT_PATH|g" k8s/postgres-deployment.yaml
    sed -i '' "s|CONFIG_PATH_PLACEHOLDER|$CONFIG_PATH|g" k8s/postgres-deployment.yaml
    sed -i '' "s|RESULTS_PATH_PLACEHOLDER|$RESULTS_PATH|g" k8s/postgres-deployment.yaml
    sed -i '' "s|DATA_PATH_PLACEHOLDER|$DATA_PATH|g" k8s/elasticsearch-deployment.yaml
    sed -i '' "s|SCRIPT_PATH_PLACEHOLDER|$SCRIPT_PATH|g" k8s/elasticsearch-deployment.yaml
    sed -i '' "s|CONFIG_PATH_PLACEHOLDER|$CONFIG_PATH|g" k8s/elasticsearch-deployment.yaml

    # Replace placeholders in Postgres deployment
    sed -i '' "s/CPU_REQUEST_PLACEHOLDER/$CPU/g" k8s/postgres-deployment.yaml
    sed -i '' "s/CPU_LIMIT_PLACEHOLDER/$CPU/g" k8s/postgres-deployment.yaml
    sed -i '' "s/MEMORY_REQUEST_PLACEHOLDER/$MEMORY/g" k8s/postgres-deployment.yaml
    sed -i '' "s/MEMORY_LIMIT_PLACEHOLDER/$MEMORY/g" k8s/postgres-deployment.yaml

    # Replace placeholders in Elasticsearch deployment
    sed -i '' "s/CPU_REQUEST_PLACEHOLDER/$CPU/g" k8s/elasticsearch-deployment.yaml
    sed -i '' "s/CPU_LIMIT_PLACEHOLDER/$CPU/g" k8s/elasticsearch-deployment.yaml
    sed -i '' "s/MEMORY_REQUEST_PLACEHOLDER/$MEMORY/g" k8s/elasticsearch-deployment.yaml
    sed -i '' "s/MEMORY_LIMIT_PLACEHOLDER/$MEMORY/g" k8s/elasticsearch-deployment.yaml
    sed -i '' "s/JVM_OPTS_PLACEHOLDER/$JVM_OPTS/g" k8s/elasticsearch-deployment.yaml

    print_success "Kubernetes resource specifications updated"
}

# Function to delete existing deployments
delete_deployments() {
    print_info "Deleting existing deployments..."
    kubectl delete deployments --all --ignore-not-found=true
    kubectl wait --for=delete pod --all --timeout=15s || true
    print_success "Existing deployments deleted"
}

# Function to setup a specific database
setup_database() {
    local db=$1
    print_info "Setting up $db database..."

    # Create namespace if it doesn't exist
    kubectl apply -f k8s/namespace.yaml

    if [[ "$db" == "postgres" ]]; then
        print_info "Deploying Postgres..."
        kubectl apply -f k8s/postgres-deployment.yaml
        kubectl rollout status deployment/postgres --timeout=300s
    elif [[ "$db" == "elasticsearch" ]]; then
        print_info "Deploying Elasticsearch..."
        kubectl apply -f k8s/elasticsearch-deployment.yaml
        kubectl rollout status deployment/elasticsearch --timeout=300s
    fi

    print_success "$db deployment completed"
}

# Function to display connection information
display_connection_info() {
    echo
    print_success "Databases are now running!"
    echo
    echo "Port-forward commands:"
    echo "  Postgres:    kubectl port-forward svc/postgres-service 5432:5432"
    echo "  Elasticsearch: kubectl port-forward svc/elasticsearch-service 9200:9200"
    echo
    echo "Connection strings:"
    echo "  Postgres (psql):    psql 'postgresql://benchmark_user:benchmark_password_123@localhost:5432/benchmark_db'"
    echo "  Postgres (Python):  postgresql://benchmark_user:benchmark_password_123@localhost:5432/benchmark_db"
    echo "  Elasticsearch:      http://localhost:9200"
    echo
    echo "To stop the databases, run: kubectl delete deployments --all"
    echo "To clean up data: kubectl delete pvc postgres-pvc elasticsearch-pvc"
}

# Main execution
print_info "Starting local databases for Postgres and Elasticsearch"

# Validate config file
if [[ ! -f "$CONFIG_FILE" ]]; then
    print_error "Configuration file not found: $CONFIG_FILE"
    exit 1
fi

check_prerequisites
generate_data_on_host
update_k8s_resources

# Start both databases
setup_database "postgres"
setup_database "elasticsearch"

display_connection_info

print_success "Local databases setup completed!"