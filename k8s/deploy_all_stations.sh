#!/bin/bash
# Deploy monthly weather fetch jobs for ALL Danish stations (05005-06197)
# This will collect MAXIMUM weather data from both SYNOP and PLUVIO stations

# Define months with their end dates
declare -A MONTHS=(
  ["2021-01"]="31" ["2021-02"]="28" ["2021-03"]="31" ["2021-04"]="30" ["2021-05"]="31" ["2021-06"]="30"
  ["2021-07"]="31" ["2021-08"]="31" ["2021-09"]="30" ["2021-10"]="31" ["2021-11"]="30" ["2021-12"]="31"
  ["2022-01"]="31" ["2022-02"]="28" ["2022-03"]="31" ["2022-04"]="30" ["2022-05"]="31" ["2022-06"]="30"
  ["2022-07"]="31" ["2022-08"]="31" ["2022-09"]="30" ["2022-10"]="31" ["2022-11"]="30" ["2022-12"]="31"
  ["2023-01"]="31" ["2023-02"]="28" ["2023-03"]="31" ["2023-04"]="30" ["2023-05"]="31" ["2023-06"]="30"
  ["2023-07"]="31" ["2023-08"]="31" ["2023-09"]="30" ["2023-10"]="31" ["2023-11"]="30" ["2023-12"]="31"
  ["2024-01"]="31" ["2024-02"]="29" ["2024-03"]="31" ["2024-04"]="30" ["2024-05"]="31" ["2024-06"]="30"
  ["2024-07"]="31" ["2024-08"]="31" ["2024-09"]="30" ["2024-10"]="31" ["2024-11"]="30" ["2024-12"]="31"
  ["2025-01"]="31" ["2025-02"]="28" ["2025-03"]="31" ["2025-04"]="30" ["2025-05"]="31" ["2025-06"]="30"
  ["2025-07"]="31" ["2025-08"]="31" ["2025-09"]="30" ["2025-10"]="31" ["2025-11"]="30"
)

# Ordered list of months
MONTH_ORDER=(
  "2021-01" "2021-02" "2021-03" "2021-04" "2021-05" "2021-06" "2021-07" "2021-08" "2021-09" "2021-10" "2021-11" "2021-12"
  "2022-01" "2022-02" "2022-03" "2022-04" "2022-05" "2022-06" "2022-07" "2022-08" "2022-09" "2022-10" "2022-11" "2022-12"
  "2023-01" "2023-02" "2023-03" "2023-04" "2023-05" "2023-06" "2023-07" "2023-08" "2023-09" "2023-10" "2023-11" "2023-12"
  "2024-01" "2024-02" "2024-03" "2024-04" "2024-05" "2024-06" "2024-07" "2024-08" "2024-09" "2024-10" "2024-11" "2024-12"
  "2025-01" "2025-02" "2025-03" "2025-04" "2025-05" "2025-06" "2025-07" "2025-08" "2025-09" "2025-10" "2025-11"
)

TEMPLATE_FILE="/home/akris19/BigData/k8s/monthly-fetch-all-stations-template.yaml"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  Deploying ALL Danish Weather Stations Fetch Jobs             ║"
echo "║  Station Range: 05005 - 06197 (SYNOP + PLUVIO)               ║"
echo "║  Total Months: ${#MONTH_ORDER[@]}                                            ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check if template exists
if [ ! -f "$TEMPLATE_FILE" ]; then
  echo "❌ Template file not found: $TEMPLATE_FILE"
  exit 1
fi

# Summary tracking
SUCCESSFUL=0
FAILED=0
START_TIME=$(date +%s)

for MONTH in "${MONTH_ORDER[@]}"; do
  END_DAY=${MONTHS[$MONTH]}
  JOB_NAME="fetch-all-$MONTH"

  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "📅 Month: $MONTH (end: $MONTH-$END_DAY)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Deploy job
  echo "  🚀 Deploying job $JOB_NAME..."
  cat "$TEMPLATE_FILE" | \
    sed "s/YYYY-MM-DD/${MONTH}-${END_DAY}/g" | \
    sed "s/YYYY-MM/${MONTH}/g" | \
    kubectl apply -f - > /dev/null

  if [ $? -ne 0 ]; then
    echo "  ❌ Failed to deploy job $JOB_NAME"
    ((FAILED++))
    continue
  fi

  # Wait for job to complete (20 minute timeout for large data collection)
  echo "  ⏳ Waiting for job to complete (max 20 minutes)..."
  kubectl wait --for=condition=complete --timeout=1200s job/$JOB_NAME -n bd-bd-gr-08 2>/dev/null

  STATUS=$(kubectl get job $JOB_NAME -n bd-bd-gr-08 -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)

  if [ "$STATUS" == "Complete" ]; then
    echo "  ✅ $JOB_NAME completed successfully"

    # Get summary from logs
    echo "  📊 Summary:"
    kubectl logs -n bd-bd-gr-08 job/$JOB_NAME 2>/dev/null | grep -E "Stations with data:|Total observations:|File size:|Saved to:" | sed 's/^/     /'

    ((SUCCESSFUL++))
  else
    echo "  ❌ $JOB_NAME failed or timed out (status: $STATUS)"
    echo "  📋 Last 15 log lines:"
    kubectl logs -n bd-bd-gr-08 job/$JOB_NAME --tail=15 2>/dev/null | sed 's/^/     /'
    ((FAILED++))
  fi

  echo ""
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                    DEPLOYMENT SUMMARY                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "  ✅ Successful: $SUCCESSFUL"
echo "  ❌ Failed:     $FAILED"
echo "  ⏱️  Duration:   ${HOURS}h ${MINUTES}m"
echo ""

if [ $SUCCESSFUL -gt 0 ]; then
  echo "  📁 Data saved to PVC: /data/weather_all_stations/"
  echo "  📋 Next steps:"
  echo "     1. Copy data from PVC to HDFS"
  echo "     2. Create/update Hive table to include new stations"
  echo "     3. Query the expanded dataset"
fi

echo ""
echo "🎉 All deployment jobs completed!"
