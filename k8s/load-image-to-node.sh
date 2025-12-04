#!/bin/bash

# Script to load Docker image onto Kubernetes node without registry

set -e

echo "🚀 Loading Docker image to Kubernetes node"
echo ""

# Configuration
IMAGE_NAME="bigdata-fetcher:latest"
TAR_FILE="../bigdata-fetcher.tar"
NODE_NAME="leftover.tek.sdu.dk"

# Check if tar file exists
if [ ! -f "$TAR_FILE" ]; then
    echo "Error: $TAR_FILE not found"
    echo "Creating tar file..."
    cd ..
    docker save $IMAGE_NAME -o bigdata-fetcher.tar
    cd k8s
fi

echo "✓ Docker image tar file ready: $TAR_FILE"
echo ""

# Method 1: Copy tar to node and import with ctr
echo "Method 1: Import via SSH to node"
echo "======================================"
echo ""
echo "Run these commands to load the image on the node:"
echo ""
echo "# 1. Copy the tar file to the node"
echo "scp $TAR_FILE $NODE_NAME:/tmp/"
echo ""
echo "# 2. SSH to the node"
echo "ssh $NODE_NAME"
echo ""
echo "# 3. Import to containerd (on the node)"
echo "sudo ctr -n k8s.io images import /tmp/bigdata-fetcher.tar"
echo ""
echo "# 4. Verify the image"
echo "sudo crictl images | grep bigdata-fetcher"
echo ""
echo "# 5. Clean up"
echo "rm /tmp/bigdata-fetcher.tar"
echo "exit"
echo ""

# Method 2: Use kubectl to create a loader pod
echo "Method 2: Use a DaemonSet to load image"
echo "======================================"
echo ""
echo "This method creates a privileged pod that loads the image."
echo "WARNING: Requires privileged access"
echo ""

cat > /tmp/image-loader.yaml <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: docker-image
  namespace: bd-bd-gr-08
binaryData:
  image.tar: |
    # This would contain the base64 encoded tar file
    # But ConfigMap has size limits (1MB), so use Method 1 or 3
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: image-loader
  namespace: bd-bd-gr-08
spec:
  selector:
    matchLabels:
      app: image-loader
  template:
    metadata:
      labels:
        app: image-loader
    spec:
      hostNetwork: true
      hostPID: true
      containers:
      - name: loader
        image: alpine:latest
        command:
          - /bin/sh
          - -c
          - |
            echo "Image loader pod ready"
            echo "To load image, use:"
            echo "  kubectl cp bigdata-fetcher.tar POD_NAME:/tmp/"
            echo "  kubectl exec POD_NAME -- ctr -n k8s.io images import /tmp/bigdata-fetcher.tar"
            sleep infinity
        securityContext:
          privileged: true
        volumeMounts:
        - name: containerd
          mountPath: /run/containerd
      volumes:
      - name: containerd
        hostPath:
          path: /run/containerd
EOF

echo "DaemonSet config created: /tmp/image-loader.yaml"
echo ""

# Method 3: Update YAML to use IfNotPresent and pre-load
echo "Method 3: Pre-load and use imagePullPolicy: IfNotPresent"
echo "======================================"
echo ""
echo "After loading the image to the node with Method 1:"
echo ""
echo "# Update the Job manifests"
echo "sed -i 's|imagePullPolicy: Never|imagePullPolicy: IfNotPresent|g' data-fetch-jobs.yaml"
echo ""
echo "# Deploy"
echo "kubectl apply -f data-fetch-jobs.yaml"
echo ""

echo "======================================"
echo "RECOMMENDED APPROACH"
echo "======================================"
echo ""
echo "Use Method 1 (SSH to node):"
echo ""
echo "1. Copy image:"
echo "   scp $TAR_FILE $NODE_NAME:/tmp/"
echo ""
echo "2. SSH and import:"
echo "   ssh $NODE_NAME"
echo "   sudo ctr -n k8s.io images import /tmp/bigdata-fetcher.tar"
echo "   sudo crictl images | grep bigdata-fetcher"
echo ""
echo "3. Update and deploy:"
echo "   sed -i 's|imagePullPolicy: Never|imagePullPolicy: IfNotPresent|g' data-fetch-jobs.yaml"
echo "   kubectl apply -f data-fetch-jobs.yaml"
echo ""
