#!/bin/bash

# Test script for signal handling functionality
# This script tests the Ctrl+C graceful shutdown feature

echo "=== Testing Signal Handling in memtier_benchmark ==="
echo ""

# Create a temporary JSON output file
JSON_FILE="/tmp/memtier_signal_test_$(date +%s).json"

echo "Starting memtier_benchmark with JSON output: $JSON_FILE"
echo "The benchmark will run for 30 seconds, but we'll interrupt it with Ctrl+C after 5 seconds"
echo ""

# Start memtier_benchmark in background with a long test time
./memtier_benchmark \
    --test-time=30 \
    --clients=2 \
    --threads=1 \
    --requests=0 \
    --json-out-file="$JSON_FILE" \
    --host=127.0.0.1 \
    --port=6379 &

MEMTIER_PID=$!

echo "memtier_benchmark started with PID: $MEMTIER_PID"
echo "Waiting 5 seconds before sending SIGINT..."

# Wait 5 seconds
sleep 5

echo ""
echo "Sending first SIGINT (Ctrl+C) to trigger graceful shutdown..."
kill -INT $MEMTIER_PID

# Wait for the process to finish gracefully
wait $MEMTIER_PID
EXIT_CODE=$?

echo ""
echo "Process finished with exit code: $EXIT_CODE"

# Check if JSON file was created and contains the expected fields
if [ -f "$JSON_FILE" ]; then
    echo ""
    echo "✓ JSON file was created: $JSON_FILE"
    
    # Check for the benchmark_completion section
    if grep -q "benchmark_completion" "$JSON_FILE"; then
        echo "✓ Found benchmark_completion section in JSON"
        
        # Extract and display the completion info
        echo ""
        echo "Benchmark completion information:"
        grep -A 10 "benchmark_completion" "$JSON_FILE" | head -10
        
        # Check specific fields
        if grep -q '"full_run": "false"' "$JSON_FILE"; then
            echo "✓ full_run correctly set to false"
        else
            echo "❌ full_run not set to false"
        fi
        
        if grep -q '"interrupted_by_signal": "true"' "$JSON_FILE"; then
            echo "✓ interrupted_by_signal correctly set to true"
        else
            echo "❌ interrupted_by_signal not set to true"
        fi
        
        if grep -q '"sigint_count": "1"' "$JSON_FILE"; then
            echo "✓ sigint_count correctly set to 1"
        else
            echo "❌ sigint_count not set to 1"
        fi
        
    else
        echo "❌ benchmark_completion section not found in JSON"
    fi
    
    # Clean up
    echo ""
    echo "Cleaning up JSON file: $JSON_FILE"
    rm -f "$JSON_FILE"
    
else
    echo "❌ JSON file was not created"
fi

echo ""
echo "=== Signal Handling Test Complete ==="
