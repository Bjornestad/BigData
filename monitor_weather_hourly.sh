#!/bin/bash

# Script to monitor the weather_station_hourly table creation progress

echo "================================================"
echo "Weather Station Hourly Table - Progress Monitor"
echo "================================================"
echo ""

# Method 1: Check the log file
echo "1. Checking creation log..."
if [ -f /tmp/weather_hourly_creation.log ]; then
    echo "   Latest progress from log:"
    tail -50 /tmp/weather_hourly_creation.log | grep -E "VERTICES:|%|SUCCEEDED|completed" | tail -5
    echo ""
else
    echo "   Log file not found yet"
    echo ""
fi

# Method 2: Check if table exists
echo "2. Checking if table exists in Hive..."
kubectl exec -n bd-bd-gr-08 hiveserver2-854cb557b6-wh928 -- beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES LIKE 'weather_station_hourly';" 2>&1 | grep -A 3 "tab_name"

echo ""

# Method 3: If table exists, check record count
echo "3. If table exists, checking record count..."
kubectl exec -n bd-bd-gr-08 hiveserver2-854cb557b6-wh928 -- beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) as row_count FROM weather_station_hourly;" 2>&1 | grep -A 5 "row_count" | head -10

echo ""
echo "================================================"
echo "To continuously monitor, run:"
echo "  watch -n 10 ./monitor_weather_hourly.sh"
echo ""
echo "To check the full log:"
echo "  tail -f /tmp/weather_hourly_creation.log"
echo "================================================"
