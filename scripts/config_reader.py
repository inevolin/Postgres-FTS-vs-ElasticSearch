#!/usr/bin/env python3
"""
Config Reader Script
Extracts values from JSON config files using dot notation paths.
"""

import json
import sys
from typing import Any

def get_nested_value(data: dict, path: str) -> Any:
    """Get nested value from dict using dot notation."""
    keys = path.split('.')
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current

def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python3 config_reader.py <config_file> <path> [default_value]", file=sys.stderr)
        sys.exit(1)

    config_file = sys.argv[1]
    path = sys.argv[2]
    default_value = sys.argv[3] if len(sys.argv) == 4 else None

    try:
        with open(config_file, 'r') as f:
            config = json.load(f)

        value = get_nested_value(config, path)
        if value is not None:
            print(value)
        elif default_value is not None:
            print(default_value)
        else:
            print(f"Path '{path}' not found in config", file=sys.stderr)
            sys.exit(1)
    except FileNotFoundError:
        print(f"Config file '{config_file}' not found", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in config file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()