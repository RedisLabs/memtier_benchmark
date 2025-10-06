#!/bin/bash

# Test script for the new Redis-style logging format
echo "=== Testing Redis-style Logging Format ==="
echo ""

# Test with a short run that we'll interrupt
echo "Starting a 10-second benchmark that we'll interrupt after 3 seconds..."
echo "Expected format:"
echo "[RUN #1] DD MMM YYYY HH:MM:SS.mmm * Preparing benchmark client..."
echo "[RUN #1] DD MMM YYYY HH:MM:SS.mmm * Launching threads now..."
echo "^CPID:signal-handler (THREAD_ID) DD MMM YYYY HH:MM:SS.mmm * Received SIGINT scheduling shutdown..."
echo "PID:M DD MMM YYYY HH:MM:SS.mmm * User requested shutdown..."
echo "PID:M DD MMM YYYY HH:MM:SS.mmm * Saving benchmark results before exiting."
echo "[RUN #1] DD MMM YYYY HH:MM:SS.mmm * Benchmark run completed: INTERRUPTED (stopped before full run)"
echo ""

# Start the benchmark in background
./memtier_benchmark \
    --test-time=10 \
    --clients=1 \
    --threads=1 \
    --requests=0 \
    --host=127.0.0.1 \
    --port=6379 &

MEMTIER_PID=$!

# Wait 3 seconds then send SIGINT
sleep 3
echo ""
echo "Sending SIGINT to test graceful shutdown..."
kill -INT $MEMTIER_PID

# Wait for completion
wait $MEMTIER_PID

echo ""
echo "=== Test completed ==="
echo "Check the output above to verify the Redis-style timestamp format is working correctly."
