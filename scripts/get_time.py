#!/usr/bin/env python3
"""
Get current timestamp with high precision
"""

import time
import sys

def main():
    if len(sys.argv) == 2 and sys.argv[1] == "--nanoseconds":
        # Return nanoseconds for compatibility
        print(int(time.time_ns()))
    else:
        # Return seconds with 3 decimal places
        print(f"{time.time():.3f}")

if __name__ == "__main__":
    main()