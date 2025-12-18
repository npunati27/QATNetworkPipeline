#!/bin/bash

# QAT Compression Benchmark Script
# Usage: ./benchmark.sh [num_runs] [duration_seconds] [warmup_runs]

# Parameters
NUM_RUNS=${1:-30}
DURATION=${2:-60}
WARMUP_RUNS=${3:-3}

SENDER="./qat_sender"
CONSUMER="./qat_consumer_2"
RESULTS_FILE="benchmark_results_$(date +%Y%m%d_%H%M%S).csv"

# Compression fractions to test
FRACTIONS=(0.0 0.05 0.10 0.15 0.20 0.25 0.30)

# Compression levels to test
LEVELS=(1 2 4 9)

echo "QAT Compression Benchmark"

# Check if binaries exist
if [ ! -f "$SENDER" ]; then
    echo "Error: $SENDER not found"
    exit 1
fi

if [ ! -f "$CONSUMER" ]; then
    echo "Error: $CONSUMER not found"
    exit 1
fi

echo "fraction,level,run,total_data_mb,duration_sec,throughput_mbps" > "$RESULTS_FILE"

extract_total_data() {
    local log_file=$1
    # Look for line like: "  Total data received:   95143023 bytes (90.74 MB)"
    # Extract the MB value from parentheses
    local total_mb=$(grep "Total data received:" "$log_file" | grep -oP '\(\K[0-9]+\.[0-9]+(?= MB\))' | head -1)
    echo "$total_mb"
}

extract_duration() {
    local log_file=$1
    # Look for line like: "Test Duration: 10.63 seconds"
    local duration=$(grep "Test Duration:" "$log_file" | grep -oP '[0-9]+\.[0-9]+(?= seconds)' | head -1)
    echo "$duration"
}

run_single_test() {
    local fraction=$1
    local level=$2
    local run_num=$3
    local is_warmup=$4
    
    local sender_log=$(mktemp)
    local consumer_log=$(mktemp)
    
    # Make sure port 9999 is free (kill any lingering processes)
    sudo pkill -9 qat_sender 2>/dev/null
    pkill -9 qat_consumer 2>/dev/null
    sleep 1
    
    # Start sender (server) in background first
    # Note: If QAT doesn't require root, remove sudo to make process control easier
    timeout $((DURATION + 10)) sudo $SENDER -f $fraction -l $level > "$sender_log" 2>&1 &
    local sender_pid=$!
    
    # Save the actual PID for the sudo command
    sleep 0.5
    local actual_sender_pid=$(pgrep -f "qat_sender.*-f $fraction.*-l $level" | tail -1)
    
    # Give sender time to start listening
    sleep 3
    
    # Start consumer (client)
    $CONSUMER -l $level > "$consumer_log" 2>&1 &
    local consumer_pid=$!
    
    # Wait for test duration
    sleep $DURATION
    
    # Stop sender using the actual PID we found
    if [ -n "$actual_sender_pid" ]; then
        sudo kill -SIGTERM $actual_sender_pid 2>/dev/null
        sleep 1
        sudo kill -9 $actual_sender_pid 2>/dev/null
    fi
    
    # Also kill the sudo wrapper process
    kill -9 $sender_pid 2>/dev/null
    
    # Nuclear cleanup
    sudo pkill -9 qat_sender 2>/dev/null
    
    # Give consumer time to finish receiving and print stats
    sleep 3
    
    # Stop consumer
    kill -SIGTERM $consumer_pid 2>/dev/null
    sleep 1
    kill -9 $consumer_pid 2>/dev/null
    pkill -9 qat_consumer 2>/dev/null
    
    # Wait for port to be released
    sleep 2
    
    # Extract metrics
    local total_mb=$(extract_total_data "$consumer_log")
    local duration=$(extract_duration "$consumer_log")
    
    if [ -z "$total_mb" ] || [ -z "$duration" ]; then
        # Debug: show why it failed
        echo "FAILED"
        echo "  Debug - Extracted values:" >&2
        echo "    total_mb='$total_mb'" >&2
        echo "    duration='$duration'" >&2
        echo "" >&2
        echo "  Debug - Looking for these patterns:" >&2
        echo "    'Total data received:'" >&2
        grep "Total data received:" "$consumer_log" >&2 || echo "    NOT FOUND" >&2
        echo "    'Test Duration:'" >&2
        grep "Test Duration:" "$consumer_log" >&2 || echo "    NOT FOUND" >&2
        echo "" >&2
        echo "  Debug - Full consumer output:" >&2
        cat "$consumer_log" >&2
        echo "  ---" >&2
        
        # Cleanup temp files
        rm -f "$sender_log" "$consumer_log"
        return 1
    fi
    
    # Calculate throughput in Mbps
    local throughput=$(echo "scale=2; ($total_mb * 8) / $duration" | bc)
    
    # Sanity check: if throughput > 100 Gbps, something is wrong
    local throughput_int=$(echo "$throughput / 1" | bc)
    if [ $throughput_int -gt 100000 ]; then
        echo "WARNING: Suspiciously high throughput (${throughput} Mbps), check extraction" >&2
    fi
    
    # Only record if not warmup
    if [ "$is_warmup" != "true" ]; then
        echo "$fraction,$level,$run_num,$total_mb,$duration,$throughput" >> "$RESULTS_FILE"
    fi
    
    echo "$total_mb"
    return 0
}

total_configs=$((${#FRACTIONS[@]} * ${#LEVELS[@]}))
current_config=0

for fraction in "${FRACTIONS[@]}"; do
    for level in "${LEVELS[@]}"; do
        current_config=$((current_config + 1))
        
        echo ""
        echo "========================================"
        echo "Config $current_config/$total_configs: fraction=$fraction, level=$level"
        echo "========================================"
        
        # Warmup runs
        if [ $WARMUP_RUNS -gt 0 ]; then
            echo "Warmup runs:"
            for i in $(seq 1 $WARMUP_RUNS); do
                echo -n "  Warmup $i..."
                result=$(run_single_test $fraction $level $i true)
                if [ "$result" != "ERROR" ]; then
                    echo " ${result} MB"
                else
                    echo " FAILED"
                fi
                sleep 1
            done
        fi
        
        # Measurement runs
        echo "Measurement runs:"
        
        for i in $(seq 1 $NUM_RUNS); do
            echo -n "  Run $i/$NUM_RUNS..."
            result=$(run_single_test $fraction $level $i false)
            
            if [ "$result" != "ERROR" ]; then
                echo " ${result} MB"
            else
                echo " FAILED"
            fi
            
            sleep 1
        done
        
        if [ "$fraction" == "0.0" ]; then
            echo ""
            echo "Note: Fraction=0.0 (no compression), skipping other compression levels"
            break
        fi
        sleep 2
    done
done

# Summary file
SUMMARY_FILE="${RESULTS_FILE%.csv}_summary.txt"

{
    echo "QAT Compression Benchmark Summary"
    echo "=================================="
    echo ""
    echo "Configuration:"
    echo "  Measurement runs:  $NUM_RUNS per config"
    echo "  Duration per run:  ${DURATION}s"
    echo "  Warmup runs:       $WARMUP_RUNS"
    echo ""
    echo "Results by Configuration:"
    echo ""
} > "$SUMMARY_FILE"

# Calculate averages for each config
for fraction in "${FRACTIONS[@]}"; do
    for level in "${LEVELS[@]}"; do
        # Extract data for this config
        config_data=$(grep "^$fraction,$level," "$RESULTS_FILE" | cut -d',' -f4)
        
        if [ -n "$config_data" ]; then
            count=$(echo "$config_data" | wc -l)
            sum=$(echo "$config_data" | paste -sd+ | bc)
            avg=$(echo "scale=2; $sum / $count" | bc)
            min=$(echo "$config_data" | sort -n | head -1)
            max=$(echo "$config_data" | sort -n | tail -1)
            
            {
                echo "Fraction=$fraction, Level=$level:"
                echo "  Runs:    $count"
                echo "  Average: $avg MB"
                echo "  Min:     $min MB"
                echo "  Max:     $max MB"
                echo ""
            } >> "$SUMMARY_FILE"
            
            echo "Fraction=$fraction, Level=$level: avg=${avg} MB"
        fi
    done
done

echo ""
echo "Benchmark Complete"
echo ""
echo "Results saved to:"
echo "  Full data:  $RESULTS_FILE"
echo "  Summary:    $SUMMARY_FILE"
