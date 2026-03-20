#!/bin/bash

# Configuration
LOG_FILE="aurora_full_benchmark.log"
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

# Clear old log and start fresh
echo "==========================================================" > $LOG_FILE
echo "AURORA FULL SYSTEM PERFORMANCE REPORT" >> $LOG_FILE
echo "Timestamp: $TIMESTAMP" >> $LOG_FILE
echo "==========================================================" >> $LOG_FILE
echo "" >> $LOG_FILE

# Helper function to run a test and log output
run_benchmark() {
    local test_name=$1
    local test_command=$2
    
    echo ">>> RUNNING: $test_name" | tee -a $LOG_FILE
    echo "----------------------------------------------------------" >> $LOG_FILE
    $test_command --release -- --nocapture 2>&1 | tee -a $LOG_FILE
    echo -e "\n" >> $LOG_FILE
}

echo "Starting full benchmark suite... this may take several minutes."

# 1. Core Stress Bench (Workers, Computed Fields, 1M Scaling)
run_benchmark "Core Stress & Scaling" "cargo test --test core_stress_bench"

# 2. Reactive Performance (Latency & Throughput)
run_benchmark "Reactive Performance" "cargo test --test reactive_performance_test"

# 3. Reactive Extreme (High-frequency Scaling)
run_benchmark "Reactive Extreme Scaling" "cargo test --test reactive_extreme_test"

# 4. PubSub Fan-out (Delivery Throughput)
run_benchmark "PubSub Fan-out" "cargo test --test pubsub_fanout_test"

# 5. Memory Efficiency (RSS Growth & Ingestion)
run_benchmark "Memory & 1M Ingestion" "cargo test --test memory_1m_test"

# 6. Pagination & Projections (Deep Offset vs Cursor)
run_benchmark "Deep Pagination Stress" "cargo test --test pagination_stress_test"

# 7. Performance Comparison (AQL vs Fluent vs SQLite)
run_benchmark "AQL Overhead Comparison" "cargo test --test performance_comparison"

# 8. Library Unit Performance (Internal subsystems)
run_benchmark "Internal Subsystem Benchmarks" "cargo test --lib"

echo "==========================================================" | tee -a $LOG_FILE
echo "ALL BENCHMARKS COMPLETED" | tee -a $LOG_FILE
echo "Full report saved to: $LOG_FILE" | tee -a $LOG_FILE
echo "==========================================================" | tee -a $LOG_FILE
