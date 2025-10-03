#!/bin/bash

# Test script for duration display and logging functionality
# This script tests the new duration display and logging features

echo "=== Testing Duration Display and Logging in memtier_benchmark ==="
echo ""

# Create temporary files
JSON_FILE="/tmp/memtier_duration_test_$(date +%s).json"
OUTPUT_FILE="/tmp/memtier_output_test_$(date +%s).txt"

echo "Testing with JSON output: $JSON_FILE"
echo "Testing with text output: $OUTPUT_FILE"
echo ""

# Test 1: Normal completion with test-time
echo "Test 1: Normal completion with --test-time=3 seconds"
./memtier_benchmark \
    --test-time=3 \
    --clients=1 \
    --threads=1 \
    --requests=0 \
    --json-out-file="$JSON_FILE" \
    --out-file="$OUTPUT_FILE" \
    --host=127.0.0.1 \
    --port=6379 2>&1

echo ""
echo "=== Checking text output file ==="
if [ -f "$OUTPUT_FILE" ]; then
    echo "✓ Output file created"
    echo ""
    echo "Duration information from output file:"
    grep -A 10 "Actual benchmark duration" "$OUTPUT_FILE" || echo "Duration info not found"
    echo ""
    echo "Completion status:"
    grep "completion status" "$OUTPUT_FILE" || echo "Completion status not found"
else
    echo "❌ Output file not created"
fi

echo ""
echo "=== Checking JSON output file ==="
if [ -f "$JSON_FILE" ]; then
    echo "✓ JSON file created"
    echo ""
    echo "Benchmark completion section:"
    grep -A 10 "benchmark_completion" "$JSON_FILE" | head -15 || echo "Completion section not found"
    echo ""
    echo "Run information section:"
    grep -A 15 "run information" "$JSON_FILE" | head -20 || echo "Run information not found"
else
    echo "❌ JSON file not created"
fi

# Clean up
echo ""
echo "Cleaning up test files..."
rm -f "$JSON_FILE" "$OUTPUT_FILE"

echo ""
echo "=== Duration Display Test Complete ==="
