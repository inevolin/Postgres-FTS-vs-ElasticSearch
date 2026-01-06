#!/usr/bin/env python3
"""
Timing Calculation Script
Calculates time differences for benchmarking.
"""

import sys

def calculate_time_difference(end_time: str, start_time: str) -> float:
    """Calculate time difference between end and start times."""
    try:
        return float(end_time) - float(start_time)
    except ValueError as e:
        print(f"Invalid time values: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 timing.py <end_time> <start_time>", file=sys.stderr)
        sys.exit(1)

    end_time = sys.argv[1]
    start_time = sys.argv[2]

    diff = calculate_time_difference(end_time, start_time)
    print(f"{diff:.3f}")

if __name__ == "__main__":
    main()