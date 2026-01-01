#!/bin/bash

# Kafka Connection Fix - Quick Deploy Script
# This script applies the Kafka init container fixes

set -e

NAMESPACE="bd-bd-gr-08"
RELEASE_NAME="energy-platform"
CHART_PATH="./energy-platform"

echo "=========================================="
echo "Kafka Connection Fix Deployment"
echo "=========================================="
echo ""
echo "This will:"
echo "  1. Add init containers to wait for Kafka"
echo "  2. Add startup probes for better health checks"
echo "  3. Upgrade your existing deployment"
echo ""

# Check if namespace exists
if ! kubectl get namespace "$NAMESPACE" &> /dev/null; then
    echo "❌ Namespace $NAMESPACE does not exist!"
    echo "Creating namespace..."
    kubectl create namespace "$NAMESPACE"
fi

# Check if release exists
if helm list -n "$NAMESPACE" | grep -q "$RELEASE_NAME"; then
    echo "✅ Found existing release: $RELEASE_NAME"
    echo ""
    echo "Current pods:"
    kubectl get pods -n "$NAMESPACE" | grep -E "backend|database-writer"
    echo ""
    
    read -p "Upgrade existing release? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "🚀 Upgrading release with Kafka fixes..."
        helm upgrade "$RELEASE_NAME" "$CHART_PATH" \
            --namespace "$NAMESPACE" \
            --wait \
            --timeout 10m
        
        echo ""
        echo "✅ Upgrade complete!"
    else
        echo "Cancelled."
        exit 0
    fi
else
    echo "📦 Installing new release: $RELEASE_NAME"
    echo ""
    
    read -p "Install fresh deployment? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "🚀 Installing release with Kafka fixes..."
        helm install "$RELEASE_NAME" "$CHART_PATH" \
            --namespace "$NAMESPACE" \
            --create-namespace \
            --wait \
            --timeout 10m
        
        echo ""
        echo "✅ Installation complete!"
    else
        echo "Cancelled."
        exit 0
    fi
fi

echo ""
echo "=========================================="
echo "Verifying Deployment"
echo "=========================================="
echo ""

# Wait a moment for pods to start
sleep 5

echo "📊 Current pod status:"
kubectl get pods -n "$NAMESPACE" | grep -E "NAME|backend|database-writer|kafka"

echo ""
echo "🔍 Checking init containers..."

# Get pod names
BACKEND_POD=$(kubectl get pods -n "$NAMESPACE" -l app=backend -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
DB_WRITER_POD=$(kubectl get pods -n "$NAMESPACE" -l app=database-writer -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "$BACKEND_POD" ]; then
    echo ""
    echo "Backend init container logs:"
    kubectl logs -n "$NAMESPACE" "$BACKEND_POD" -c wait-for-kafka 2>/dev/null || echo "Init container already completed"
fi

if [ -n "$DB_WRITER_POD" ]; then
    echo ""
    echo "Database Writer init container logs:"
    kubectl logs -n "$NAMESPACE" "$DB_WRITER_POD" -c wait-for-kafka 2>/dev/null || echo "Init container already completed"
fi

echo ""
echo "=========================================="
echo "Next Steps"
echo "=========================================="
echo ""
echo "1. Check backend logs (should be clean!):"
echo "   kubectl logs -n $NAMESPACE -l app=backend --tail=50"
echo ""
echo "2. Check database writer logs:"
echo "   kubectl logs -n $NAMESPACE -l app=database-writer --tail=50"
echo ""
echo "3. Verify Kafka connection:"
echo "   kubectl logs -n $NAMESPACE -l app=backend | grep -i 'connected to kafka'"
echo ""
echo "4. Watch for errors:"
echo "   kubectl logs -n $NAMESPACE -l app=backend -f | grep -i error"
echo ""
echo "✅ Deployment complete! Your system should now start cleanly."
