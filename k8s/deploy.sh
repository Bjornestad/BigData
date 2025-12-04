#!/bin/bash

# Kubernetes Deployment Script for Data Fetching
# This script deploys jobs to fetch weather and energy data in parallel

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Data Fetching Kubernetes Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Error: kubectl is not installed${NC}"
    exit 1
fi

# Check if connected to a cluster
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}Error: Not connected to a Kubernetes cluster${NC}"
    echo "Run 'kubectl config get-contexts' to see available clusters"
    exit 1
fi

echo -e "${GREEN}✓ Connected to Kubernetes cluster${NC}"
CURRENT_CONTEXT=$(kubectl config current-context)
echo -e "  Context: ${YELLOW}${CURRENT_CONTEXT}${NC}"
echo ""

# Ask for confirmation
read -p "Deploy to cluster '${CURRENT_CONTEXT}'? (y/n) " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled"
    exit 0
fi

# Check if using Minikube
if [[ $CURRENT_CONTEXT == *"minikube"* ]]; then
    echo -e "${YELLOW}Detected Minikube - Building image in Minikube's Docker${NC}"
    eval $(minikube docker-env)
    docker build -t bigdata-fetcher:latest ..
    echo -e "${GREEN}✓ Image built in Minikube${NC}"
else
    echo -e "${YELLOW}Building Docker image...${NC}"
    docker build -t bigdata-fetcher:latest ..
    echo -e "${GREEN}✓ Image built${NC}"

    # Check if we need to push to a registry
    read -p "Do you need to push to a Docker registry? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        read -p "Enter registry URL (e.g., your-registry/bigdata-fetcher:latest): " REGISTRY_URL
        docker tag bigdata-fetcher:latest $REGISTRY_URL
        docker push $REGISTRY_URL
        echo -e "${GREEN}✓ Image pushed to registry${NC}"

        # Update image in YAML
        sed -i.bak "s|image: bigdata-fetcher:latest|image: $REGISTRY_URL|g" data-fetch-jobs.yaml
        sed -i.bak "s|imagePullPolicy: Never|imagePullPolicy: Always|g" data-fetch-jobs.yaml
        echo -e "${YELLOW}Updated YAML to use registry image${NC}"
    fi
fi
echo ""

# Deploy to Kubernetes
echo -e "${YELLOW}Deploying to Kubernetes...${NC}"
kubectl apply -f data-fetch-jobs.yaml

echo ""
echo -e "${GREEN}✓ Deployment successful!${NC}"
echo ""

# Show status
echo -e "${YELLOW}Job Status:${NC}"
kubectl get jobs -l app=data-fetch

echo ""
echo -e "${YELLOW}Pod Status:${NC}"
kubectl get pods -l app=data-fetch

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Deployment Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Monitor jobs with:"
echo "  kubectl get jobs -l app=data-fetch -w"
echo ""
echo "View logs for a specific year:"
echo "  kubectl logs -l year=2021 -f"
echo ""
echo "View all logs:"
echo "  kubectl logs -l app=data-fetch --all-containers=true"
echo ""
echo "Check completion:"
echo "  kubectl get jobs -l app=data-fetch"
echo ""
