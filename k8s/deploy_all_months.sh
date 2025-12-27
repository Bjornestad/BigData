#!/bin/bash
# Deploy monthly weather fetch jobs sequentially for 2021-01 to 2025-11

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

echo "Deploying monthly weather fetch jobs for ${#MONTH_ORDER[@]} months"
echo "Starting from ${MONTH_ORDER[0]}, ending at ${MONTH_ORDER[-1]}"
echo ""

for MONTH in "${MONTH_ORDER[@]}"; do
  END_DAY=${MONTHS[$MONTH]}
  JOB_NAME="fetch-$MONTH"

  echo "[$MONTH] Deploying job $JOB_NAME (end: $MONTH-$END_DAY)..."

  # Deploy job
  cat /home/akris19/BigData/k8s/monthly-fetch-raw-template.yaml | \
    sed "s/YYYY-MM-DD/${MONTH}-${END_DAY}/g" | \
    sed "s/YYYY-MM/${MONTH}/g" | \
    kubectl apply -f - > /dev/null

  # Wait for job to complete
  echo "  Waiting for job to complete..."
  kubectl wait --for=condition=complete --timeout=600s job/$JOB_NAME -n bd-bd-gr-08 2>/dev/null

  STATUS=$(kubectl get job $JOB_NAME -n bd-bd-gr-08 -o jsonpath='{.status.conditions[0].type}' 2>/dev/null)

  if [ "$STATUS" == "Complete" ]; then
    echo "  ✓ $JOB_NAME completed successfully"
    # Get last few log lines
    kubectl logs -n bd-bd-gr-08 job/$JOB_NAME --tail=5 | grep "✅\|Records:"
  else
    echo "  ✗ $JOB_NAME failed or timed out (status: $STATUS)"
    kubectl logs -n bd-bd-gr-08 job/$JOB_NAME --tail=10
    echo "  Continuing to next month..."
  fi

  echo ""
done

echo "All monthly jobs deployed!"
