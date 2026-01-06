#!/usr/bin/env python3
import subprocess
import time
import sys
import json
import re
import argparse

def get_pod_name(label_selector):
    try:
        cmd = ["kubectl", "get", "pod", "-l", label_selector, "-o", "jsonpath={.items[0].metadata.name}"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def get_container_id(pod_name):
    try:
        cmd = ["kubectl", "get", "pod", pod_name, "-o", "jsonpath={.status.containerStatuses[0].containerID}"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        container_id = result.stdout.strip()
        # Remove protocol prefix (docker://, containerd://)
        if "://" in container_id:
            return container_id.split("://")[1]
        return container_id
    except subprocess.CalledProcessError:
        return None

def get_kubectl_metrics(pod_name):
    try:
        cmd = ["kubectl", "top", "pod", pod_name, "--no-headers"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        # Output: pod_name   CPU(cores)   MEMORY(bytes)
        parts = result.stdout.split()
        if len(parts) >= 3:
            return parts[1], parts[2]
    except subprocess.CalledProcessError:
        pass
    return None, None

def get_docker_metrics(container_id):
    try:
        # Format: "CPU%,MemUsage" -> "0.00%, 10MiB / 1GiB"
        cmd = ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}},{{.MemUsage}}", container_id]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        if output:
            cpu_perc, mem_usage = output.split(',')
            # Clean CPU: "0.50%" -> "0.50" (We'll treat % as mCores * 10 later or just raw %)
            # Actually kubectl top returns "100m" for 0.1 core (10%).
            # Docker "100%" = 1 core. So "10%" = 0.1 core = 100m.
            # Let's normalize to "m" (millicores) and "Mi" (Mebibytes) for consistency if possible,
            # or just write raw and let plotter handle it.
            # Let's write raw for now and update plotter.
            
            # Clean Memory: "10MiB / 1GiB" -> "10MiB"
            mem_used = mem_usage.split('/')[0].strip()
            
            return cpu_perc, mem_used
    except subprocess.CalledProcessError:
        pass
    return None, None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True, help="Pod label selector (e.g. app=postgres)")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    args = parser.parse_args()

    print(f"Monitoring pod with label {args.label}...", file=sys.stderr)
    
    # Wait for pod to appear
    pod_name = None
    for _ in range(30):
        pod_name = get_pod_name(args.label)
        if pod_name:
            break
        time.sleep(1)
    
    if not pod_name:
        print("Pod not found", file=sys.stderr)
        sys.exit(1)

    print(f"Found pod: {pod_name}", file=sys.stderr)

    # Determine monitoring method
    method = "kubectl"
    # Check if kubectl top works
    cpu, mem = get_kubectl_metrics(pod_name)
    if not cpu:
        print("kubectl top not available, trying docker stats...", file=sys.stderr)
        container_id = get_container_id(pod_name)
        if container_id:
            print(f"Found container ID: {container_id}", file=sys.stderr)
            cpu, mem = get_docker_metrics(container_id)
            if cpu:
                method = "docker"
                print("Using docker stats", file=sys.stderr)
            else:
                print("docker stats failed", file=sys.stderr)
                method = "none"
        else:
            print("Could not find container ID", file=sys.stderr)
            method = "none"
    else:
        print("Using kubectl top", file=sys.stderr)

    if method == "none":
        print("No monitoring method available", file=sys.stderr)
        # Create empty file with header
        with open(args.output, 'w') as f:
            f.write("Timestamp,CPU,Memory\n")
        sys.exit(0)

    # Monitoring loop
    with open(args.output, 'w') as f:
        f.write("Timestamp,CPU,Memory\n")
        
        try:
            while True:
                timestamp = time.time()
                cpu, mem = None, None
                
                if method == "kubectl":
                    cpu, mem = get_kubectl_metrics(pod_name)
                elif method == "docker":
                    # Re-fetch container ID if needed? No, usually stable.
                    # But if pod restarts, we might need to refresh.
                    # For benchmark, assume stable.
                    cpu, mem = get_docker_metrics(container_id)
                
                if cpu and mem:
                    f.write(f"{timestamp},{cpu},{mem}\n")
                    f.flush()
                
                time.sleep(args.interval)
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    main()
