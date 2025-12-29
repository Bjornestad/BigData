#!/bin/bash

# Monitor Kubernetes data fetching jobs
# Shows real-time status and logs

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Data Fetching Job Monitor${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to get job status
get_job_status() {
    echo -e "${YELLOW}Job Status:${NC}"
    kubectl get jobs -l app=data-fetch -o custom-columns=\
NAME:.metadata.name,\
COMPLETIONS:.status.completions,\
DURATION:.status.completionTime,\
AGE:.metadata.creationTimestamp
    echo ""
}

# Function to get pod status
get_pod_status() {
    echo -e "${YELLOW}Pod Status:${NC}"
    kubectl get pods -l app=data-fetch -o custom-columns=\
NAME:.metadata.name,\
YEAR:.metadata.labels.year,\
STATUS:.status.phase,\
RESTARTS:.status.containerStatuses[0].restartCount,\
AGE:.metadata.creationTimestamp
    echo ""
}

# Function to show completion summary
show_summary() {
    echo -e "${YELLOW}Completion Summary:${NC}"

    TOTAL=$(kubectl get jobs -l app=data-fetch --no-headers | wc -l)
    COMPLETED=$(kubectl get jobs -l app=data-fetch -o jsonpath='{range .items[*]}{.status.succeeded}{"\n"}{end}' | grep -c "1" || echo "0")
    FAILED=$(kubectl get jobs -l app=data-fetch -o jsonpath='{range .items[*]}{.status.failed}{"\n"}{end}' | grep -c "1" || echo "0")
    RUNNING=$((TOTAL - COMPLETED - FAILED))

    echo -e "  Total: ${TOTAL}"
    echo -e "  ${GREEN}Completed: ${COMPLETED}${NC}"
    echo -e "  ${YELLOW}Running: ${RUNNING}${NC}"
    if [ $FAILED -gt 0 ]; then
        echo -e "  ${RED}Failed: ${FAILED}${NC}"
    fi
    echo ""
}

# Main menu
while true; do
    clear
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  Data Fetching Job Monitor${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""

    get_job_status
    get_pod_status
    show_summary

    echo -e "${YELLOW}Options:${NC}"
    echo "  1) Refresh status"
    echo "  2) View logs for 2021"
    echo "  3) View logs for 2022"
    echo "  4) View logs for 2023"
    echo "  5) View logs for 2024"
    echo "  6) View logs for 2025"
    echo "  7) View all logs"
    echo "  8) Auto-refresh (every 5s)"
    echo "  9) Check data files"
    echo "  q) Quit"
    echo ""
    read -p "Choose option: " -n 1 -r OPTION
    echo ""

    case $OPTION in
        1)
            continue
            ;;
        2)
            kubectl logs -l year=2021 -f --tail=100
            ;;
        3)
            kubectl logs -l year=2022 -f --tail=100
            ;;
        4)
            kubectl logs -l year=2023 -f --tail=100
            ;;
        5)
            kubectl logs -l year=2024 -f --tail=100
            ;;
        6)
            kubectl logs -l year=2025 -f --tail=100
            ;;
        7)
            kubectl logs -l app=data-fetch --all-containers=true --tail=50
            read -p "Press any key to continue..."
            ;;
        8)
            echo "Auto-refreshing every 5 seconds (Ctrl+C to stop)..."
            while true; do
                clear
                get_job_status
                get_pod_status
                show_summary
                sleep 5
            done
            ;;
        9)
            echo "Checking data files..."
            kubectl run data-check --image=busybox --rm -it --restart=Never \
              --overrides='
            {
              "spec": {
                "containers": [{
                  "name": "data-check",
                  "image": "busybox",
                  "command": ["sh", "-c", "ls -lh /data/ && du -sh /data/*"],
                  "volumeMounts": [{
                    "name": "data",
                    "mountPath": "/data"
                  }]
                }],
                "volumes": [{
                  "name": "data",
                  "persistentVolumeClaim": {
                    "claimName": "weather-data-pvc"
                  }
                }]
              }
            }'
            read -p "Press any key to continue..."
            ;;
        q|Q)
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo "Invalid option"
            sleep 1
            ;;
    esac
done
