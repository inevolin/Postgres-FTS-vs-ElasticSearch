#!/usr/bin/env python3
"""Postgres vs Elasticsearch Benchmark Plot Generator

Generates performance comparison plots from benchmark results.
"""

import os
import sys
import json
import csv
import argparse
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def parse_startup_file(filepath):
    """Parse startup time file and extract startup time in seconds"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('Startup time'):
                    time_str = line.split(':')[1].strip()
                    return float(time_str.rstrip('s'))
    except (FileNotFoundError, ValueError):
        pass
    return None

def parse_data_loading_file(filepath):
    """Parse data loading time file and extract data loading time in seconds"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            for line in content.split('\n'):
                line = line.strip()
                if 'Data loading' in line and 'time' in line:
                    time_str = line.split(':')[1].strip()
                    return float(time_str.rstrip('s'))
    except (FileNotFoundError, ValueError):
        pass
    return None

def parse_index_creation_file(filepath):
    """Parse index creation time file and extract index creation time in seconds"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            for line in content.split('\n'):
                line = line.strip()
                if 'Index creation' in line and 'time' in line:
                    time_str = line.split(':')[1].strip()
                    return float(time_str.rstrip('s'))
    except (FileNotFoundError, ValueError):
        pass
    return None

def parse_time_file(filepath):
    """Parse query time file and extract average and total times in seconds"""
    try:
        with open(filepath, 'r') as f:
            content = f.read()
            times = {}
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('Average time') or line.startswith('Average Latency'):
                    time_str = line.split(':')[1].strip()
                    times['average'] = float(time_str.rstrip('s'))
                elif line.startswith('Wall time'):
                    time_str = line.split(':')[1].strip()
                    times['total'] = float(time_str.rstrip('s'))
            return times if times else None
    except (FileNotFoundError, ValueError):
        pass
    return None

def generate_plots(databases, results_dir='results', plots_dir='plots', scale='', concurrency='', transactions=''):
    """Generate performance comparison plots"""

    # Ensure plots directory exists
    Path(plots_dir).mkdir(exist_ok=True)

    queries = ['query1', 'query2', 'query3', 'query4', 'query5', 'query6']
    query_labels = ['Simple', 'Phrases', 'Complex', 'Top-N', 'Boolean', 'JOINs']

    # Colors for different databases
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']

    # Collect all times for totals
    total_times = {db: 0.0 for db in databases}
    total_query_times = {db: 0.0 for db in databases}
    startup_times = {db: None for db in databases}
    data_loading_times = {db: None for db in databases}
    index_creation_times = {db: None for db in databases}
    index_sizes = {db: None for db in databases}
    resource_usage = {db: {'cpu': [], 'memory': [], 'timestamps': []} for db in databases}
    query_times = {query: {db: None for db in databases} for query in queries}
    query_tps = {query: {db: None for db in databases} for query in queries}

    # Collect startup times
    for db in databases:
        startup_file = os.path.join(results_dir, f'{scale}_{concurrency}_{transactions}_{db}_startup_time.txt')
        if not os.path.exists(startup_file):
             startup_file = os.path.join(results_dir, f'{scale}_{db}_startup_time.txt')
        startup_times[db] = parse_startup_file(startup_file)

    # Collect metrics from JSON or fallback to text files
    for db in databases:
        json_file = os.path.join(results_dir, f'{scale}_{concurrency}_{transactions}_{db}_results.json')
        if not os.path.exists(json_file):
             json_file = os.path.join(results_dir, f'{scale}_{db}_results.json')
             
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    metrics = data.get('metrics', {})
                    
                    data_loading_times[db] = metrics.get('data_loading_time')
                    index_creation_times[db] = metrics.get('index_creation_time')
                    index_sizes[db] = metrics.get('database_size_bytes')
                    
                    for i, query in enumerate(queries):
                        q_key = f'query_{i+1}'
                        if q_key in metrics:
                            query_times[query][db] = metrics[q_key].get('average_latency')
                            query_tps[query][db] = metrics[q_key].get('tps')
                            if metrics[q_key].get('total_time'):
                                total_times[db] += metrics[q_key].get('total_time')
                                total_query_times[db] += metrics[q_key].get('total_time')
            except Exception as e:
                print(f"Error parsing JSON for {db}: {e}")
        
        # Collect resource usage
        resource_file = os.path.join(results_dir, f'{scale}_{concurrency}_{transactions}_{db}_resources.csv')
        if not os.path.exists(resource_file):
             resource_file = os.path.join(results_dir, f'{scale}_{db}_resources.csv')
             
        if os.path.exists(resource_file):
            try:
                with open(resource_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            ts = float(row['Timestamp'])
                            # Parse CPU (e.g., "100m" -> 0.1, "2" -> 2.0, "0.50%" -> 0.005)
                            cpu_str = row['CPU']
                            if cpu_str.endswith('m'):
                                cpu = float(cpu_str[:-1]) / 1000.0
                            elif cpu_str.endswith('%'):
                                # Docker stats format: 100% = 1 core
                                cpu = float(cpu_str[:-1]) / 100.0
                            else:
                                cpu = float(cpu_str)
                            
                            # Parse Memory (e.g., "500Mi" -> 500, "1Gi" -> 1024, "10MiB" -> 10)
                            mem_str = row['Memory']
                            if mem_str.endswith('Gi'):
                                mem = float(mem_str[:-2]) * 1024
                            elif mem_str.endswith('GiB'):
                                mem = float(mem_str[:-3]) * 1024
                            elif mem_str.endswith('Mi'):
                                mem = float(mem_str[:-2])
                            elif mem_str.endswith('MiB'):
                                mem = float(mem_str[:-3])
                            elif mem_str.endswith('Ki'):
                                mem = float(mem_str[:-2]) / 1024
                            elif mem_str.endswith('KiB'):
                                mem = float(mem_str[:-3]) / 1024
                            else:
                                mem = float(mem_str) # Assume bytes or raw number, treat as MiB? Or just raw.
                                # Actually kubectl top usually returns Mi or Gi.
                                # If it's just a number, it might be bytes, but let's assume MiB if unsure or handle carefully.
                                # For now, let's assume standard kubectl output.
                            
                            resource_usage[db]['timestamps'].append(ts)
                            resource_usage[db]['cpu'].append(cpu)
                            resource_usage[db]['memory'].append(mem)
                        except ValueError:
                            continue
                    
                    # Normalize timestamps to start from 0
                    if resource_usage[db]['timestamps']:
                        start_ts = resource_usage[db]['timestamps'][0]
                        resource_usage[db]['timestamps'] = [t - start_ts for t in resource_usage[db]['timestamps']]
            except Exception as e:
                print(f"Error parsing resources for {db}: {e}")

        # Fallback if data missing
        if data_loading_times[db] is None:
            data_loading_file = os.path.join(results_dir, f'{scale}_{db}_data_loading_time.txt')
            data_loading_times[db] = parse_data_loading_file(data_loading_file)
            
        if index_creation_times[db] is None:
            index_creation_file = os.path.join(results_dir, f'{scale}_{db}_index_creation_time.txt')
            index_creation_times[db] = parse_index_creation_file(index_creation_file)
            
        for i, query in enumerate(queries):
            if query_times[query][db] is None:
                time_file = os.path.join(results_dir, f'{scale}_{db}_{query}_time.txt')
                times = parse_time_file(time_file)
                if times:
                    query_times[query][db] = times['average']
                    if times['total'] is not None:
                        total_query_times[db] += times['total']
                        total_times[db] += times['total']
                    # Estimate TPS if not available
                    if times['average'] > 0:
                        query_tps[query][db] = 1.0 / times['average']

    # Calculate total times (excluding queries as they are already added if available)
    for db in databases:
        if startup_times[db] is not None:
            total_times[db] += startup_times[db]
        if data_loading_times[db] is not None:
            total_times[db] += data_loading_times[db]
        if index_creation_times[db] is not None:
            total_times[db] += index_creation_times[db]

    # Generate aggregated plot (all queries in one chart) - Time
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    fig2.suptitle('Aggregated Performance by Query Type - Time', fontsize=16, fontweight='normal')

    x = np.arange(len(queries))
    width = 0.35

    db_data = []
    for db in databases:
        times = [query_times[q][db] for q in queries]
        db_data.append(times)

    if any(any(t is not None for t in times) for times in db_data):
        for i, db in enumerate(databases):
            times = [query_times[q][db] for q in queries]
            valid_times = [t for t in times if t is not None]
            if valid_times:
                ax2.bar(x + i*width, times, width, label=db.title(), color=colors[i], alpha=0.7)

        ax2.set_xlabel('Query Type')
        ax2.set_ylabel('Time (seconds)')
        ax2.set_title('Query Performance Comparison')
        ax2.set_xticks(x + width/2)
        ax2.set_xticklabels(query_labels)
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        ax2.set_ylim(bottom=0)

        # Add value labels
        for i, db in enumerate(databases):
            for j, time_val in enumerate([query_times[q][db] for q in queries]):
                if time_val is not None:
                    ax2.text(x[j] + i*width, time_val + max([t for sublist in db_data for t in sublist if t is not None])*0.02,
                            f'{time_val:.4f}', ha='center', va='bottom', fontweight='normal')

    else:
        ax2.text(0.5, 0.5, 'No data available',
                transform=ax2.transAxes, ha='center', va='center',
                fontsize=12, color='gray')

    plt.tight_layout()
    # agg_plot_file = os.path.join(plots_dir, f'{scale}_{concurrency}_{transactions}_aggregated_performance_time.png')
    # plt.savefig(agg_plot_file, dpi=300, bbox_inches='tight')
    plt.close()

    # print(f"Aggregated time performance plot saved to: {agg_plot_file}")

    # Generate aggregated plot (all queries in one chart) - TPS
    fig3, ax3 = plt.subplots(figsize=(10, 6))
    fig3.suptitle('Aggregated Performance by Query Type - TPS', fontsize=16, fontweight='normal')

    db_tps_data = []
    for db in databases:
        tps_values = [query_tps[q][db] for q in queries]
        db_tps_data.append(tps_values)

    if any(any(t is not None for t in tps_values) for tps_values in db_tps_data):
        for i, db in enumerate(databases):
            tps_values = [query_tps[q][db] for q in queries]
            valid_tps = [t for t in tps_values if t is not None]
            if valid_tps:
                ax3.bar(x + i*width, tps_values, width, label=db.title(), color=colors[i], alpha=0.7)

        ax3.set_xlabel('Query Type')
        ax3.set_ylabel('Transactions Per Second')
        ax3.set_title('Query TPS Comparison')
        ax3.set_xticks(x + width/2)
        ax3.set_xticklabels(query_labels)
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')
        ax3.set_ylim(bottom=0)

        # Add value labels
        for i, db in enumerate(databases):
            for j, tps_val in enumerate([query_tps[q][db] for q in queries]):
                if tps_val is not None:
                    ax3.text(x[j] + i*width, tps_val + max([t for sublist in db_tps_data for t in sublist if t is not None])*0.02,
                            f'{tps_val:.2f}', ha='center', va='bottom', fontweight='normal')

    else:
        ax3.text(0.5, 0.5, 'No data available',
                transform=ax3.transAxes, ha='center', va='center',
                fontsize=12, color='gray')

    plt.tight_layout()
    # agg_tps_plot_file = os.path.join(plots_dir, f'{scale}_{concurrency}_{transactions}_aggregated_performance_tps.png')
    # plt.savefig(agg_tps_plot_file, dpi=300, bbox_inches='tight')
    plt.close()

    # print(f"Aggregated TPS performance plot saved to: {agg_tps_plot_file}")

    # Generate Database Size Plot
    # fig4, ax4 = plt.subplots(figsize=(10, 6))
    # fig4.suptitle('Database Size Comparison', fontsize=16, fontweight='normal')
    
    # db_names = []
    # sizes_mb = []
    # for db in databases:
    #     if index_sizes[db] is not None:
    #         db_names.append(db.title())
    #         sizes_mb.append(index_sizes[db] / (1024 * 1024)) # Convert to MB

    # if sizes_mb:
    #     bars = ax4.bar(range(len(db_names)), sizes_mb, color=colors[:len(db_names)], alpha=0.7)
    #     ax4.set_ylabel('Size (MB)')
    #     ax4.set_xticks(range(len(db_names)))
    #     ax4.set_xticklabels(db_names)
    #     for bar, size in zip(bars, sizes_mb):
    #         ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(sizes_mb)*0.02, 
    #                f'{size:.2f} MB', ha='center', va='bottom', fontsize=10)
    # else:
    #     ax4.text(0.5, 0.5, 'No database size data available',
    #             transform=ax4.transAxes, ha='center', va='center',
    #             fontsize=12, color='gray')
    
    # plt.tight_layout()
    # size_plot_file = os.path.join(plots_dir, f'{scale}_{concurrency}_{transactions}_database_size_comparison.png')
    # plt.savefig(size_plot_file, dpi=300, bbox_inches='tight')
    # plt.close()
    # print(f"Database size plot saved to: {size_plot_file}")

    # Generate Resource Usage Plots (CPU & Memory)
    if any(len(resource_usage[db]['timestamps']) > 0 for db in databases):
        fig5, (ax5_cpu, ax5_mem) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
        fig5.suptitle('Resource Usage Over Time', fontsize=16, fontweight='normal')
        
        for i, db in enumerate(databases):
            if resource_usage[db]['timestamps']:
                ax5_cpu.plot(resource_usage[db]['timestamps'], resource_usage[db]['cpu'], 
                           label=db.title(), color=colors[i], linewidth=2)
                ax5_mem.plot(resource_usage[db]['timestamps'], resource_usage[db]['memory'], 
                           label=db.title(), color=colors[i], linewidth=2)
        
        ax5_cpu.set_ylabel('CPU Usage (Cores)')
        ax5_cpu.set_title('CPU Usage')
        ax5_cpu.legend()
        ax5_cpu.grid(True, alpha=0.3)
        
        ax5_mem.set_ylabel('Memory Usage (MiB)')
        ax5_mem.set_title('Memory Usage')
        ax5_mem.set_xlabel('Time (seconds)')
        ax5_mem.legend()
        ax5_mem.grid(True, alpha=0.3)
        
        plt.tight_layout()
        # resource_plot_file = os.path.join(plots_dir, f'{scale}_{concurrency}_{transactions}_resource_usage.png')
        # plt.savefig(resource_plot_file, dpi=300, bbox_inches='tight')
        plt.close()
        # print(f"Resource usage plot saved to: {resource_plot_file}")

    # Generate Combined Summary Plot (3x4)
    fig_combined = plt.figure(figsize=(24, 24))
    fig_combined.suptitle(f'Postgres vs Elasticsearch Benchmark Summary ({scale})', fontsize=20, fontweight='normal')
    
    # Row 1: Startup, Data Loading, Total Query Duration, Database Size
    
    # 1. Startup Time (Top-Left)
    ax_startup = plt.subplot2grid((3, 4), (0, 0), fig=fig_combined)
    db_names = []
    startup_values = []
    for db in databases:
        if startup_times[db] is not None:
            db_names.append(db.title())
            startup_values.append(startup_times[db])

    if startup_values:
        bars = ax_startup.bar(range(len(db_names)), startup_values, color=colors[:len(db_names)], alpha=0.7)
        ax_startup.set_title('Startup Time (seconds)')
        ax_startup.set_ylabel('Time (s)')
        ax_startup.set_xticks(range(len(db_names)))
        ax_startup.set_xticklabels(db_names, rotation=0)
        for bar, time in zip(bars, startup_values):
            ax_startup.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01, f'{time:.2f}s', 
                   ha='center', va='bottom', fontsize=10)
    else:
        ax_startup.text(0.5, 0.5, 'No data available', transform=ax_startup.transAxes, ha='center', va='center')

    # 2. Data Loading & Indexing Time (Top-Center-Left)
    ax_loading = plt.subplot2grid((3, 4), (0, 1), fig=fig_combined)
    db_names = []
    data_loading_values = []
    for db in databases:
        value = data_loading_times[db]
        if index_creation_times[db] is not None:
            value = (value or 0) + index_creation_times[db]
        if value is not None:
            db_names.append(db.title())
            data_loading_values.append(value)

    if data_loading_values:
        bars = ax_loading.bar(range(len(db_names)), data_loading_values, color=colors[:len(db_names)], alpha=0.7)
        ax_loading.set_title('Data Loading & Indexing Time (seconds)')
        ax_loading.set_ylabel('Time (s)')
        ax_loading.set_xticks(range(len(db_names)))
        ax_loading.set_xticklabels(db_names, rotation=0)
        for bar, time in zip(bars, data_loading_values):
            ax_loading.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(data_loading_values)*0.02, 
                   f'{time:.2f}s', ha='center', va='bottom', fontsize=10)
    else:
        ax_loading.text(0.5, 0.5, 'No data available', transform=ax_loading.transAxes, ha='center', va='center')

    # 3. Total Query Duration (Top-Center-Right)
    ax_total = plt.subplot2grid((3, 4), (0, 2), fig=fig_combined)
    db_names = []
    totals = []
    for db in databases:
        if total_query_times[db] > 0:
            db_names.append(db.title())
            totals.append(total_query_times[db])

    if totals:
        bars = ax_total.bar(range(len(db_names)), totals, color=colors[:len(db_names)], alpha=0.7)
        for bar, total in zip(bars, totals):
            height = bar.get_height()
            ax_total.text(bar.get_x() + bar.get_width()/2., height + max(totals)*0.02,
                   f'{total:.4f}', ha='center', va='bottom', fontweight='normal')
        ax_total.set_title('Total Query Duration', fontweight='normal')
        ax_total.set_ylabel('Time (seconds)')
        ax_total.set_xticks(range(len(db_names)))
        ax_total.set_xticklabels(db_names, rotation=0, ha='center')
        ax_total.grid(True, alpha=0.3, axis='y')
        ax_total.set_ylim(bottom=0)
    else:
        ax_total.text(0.5, 0.5, 'No data available', transform=ax_total.transAxes, ha='center', va='center')

    # 4. Database Size (Top-Right)
    ax_c3 = plt.subplot2grid((3, 4), (0, 3), fig=fig_combined)
    db_names_size = []
    sizes_mb_combined = []
    for db in databases:
        if index_sizes[db] is not None:
            db_names_size.append(db.title())
            sizes_mb_combined.append(index_sizes[db] / (1024 * 1024))

    if sizes_mb_combined:
        bars = ax_c3.bar(range(len(db_names_size)), sizes_mb_combined, color=colors[:len(db_names_size)], alpha=0.7)
        ax_c3.set_ylabel('Size (MB)')
        ax_c3.set_title('Database Size Comparison', fontweight='normal')
        ax_c3.set_xticks(range(len(db_names_size)))
        ax_c3.set_xticklabels(db_names_size, rotation=0)
        for bar, size in zip(bars, sizes_mb_combined):
            ax_c3.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f'{size:.2f} MB', 
                      ha='center', va='bottom', fontsize=10)
    else:
        ax_c3.text(0.5, 0.5, 'No data available', transform=ax_c3.transAxes, ha='center', va='center')

    # Row 2: Aggregated Time, Aggregated TPS
    
    # 5. Aggregated Time (Middle-Left)
    ax_c1 = plt.subplot2grid((3, 4), (1, 0), colspan=2, fig=fig_combined)
    x = np.arange(len(queries))
    width = 0.35
    
    db_time_data = []
    for db in databases:
        times = [query_times[q][db] for q in queries]
        db_time_data.append(times)

    if any(any(t is not None for t in times) for times in db_time_data):
        for i, db in enumerate(databases):
            times = [query_times[q][db] for q in queries]
            valid_times = [t for t in times if t is not None]
            if valid_times:
                ax_c1.bar(x + i*width, times, width, label=db.title(), color=colors[i], alpha=0.7)
        
        ax_c1.set_xlabel('Query Type')
        ax_c1.set_ylabel('Time (seconds)')
        ax_c1.set_title('Query Performance (Time)', fontweight='normal')
        ax_c1.set_xticks(x + width/2)
        ax_c1.set_xticklabels(query_labels, rotation=0)
        ax_c1.legend()
        ax_c1.grid(True, alpha=0.3, axis='y')
        ax_c1.set_ylim(bottom=0)
    else:
        ax_c1.text(0.5, 0.5, 'No data available', transform=ax_c1.transAxes, ha='center', va='center')

    # 6. Aggregated TPS (Middle-Right)
    ax_c2 = plt.subplot2grid((3, 4), (1, 2), colspan=2, fig=fig_combined)
    db_tps_data_combined = []
    for db in databases:
        tps_values = [query_tps[q][db] for q in queries]
        db_tps_data_combined.append(tps_values)

    if any(any(t is not None for t in tps_values) for tps_values in db_tps_data_combined):
        for i, db in enumerate(databases):
            tps_values = [query_tps[q][db] for q in queries]
            valid_tps = [t for t in tps_values if t is not None]
            if valid_tps:
                ax_c2.bar(x + i*width, tps_values, width, label=db.title(), color=colors[i], alpha=0.7)
        
        ax_c2.set_xlabel('Query Type')
        ax_c2.set_ylabel('TPS')
        ax_c2.set_title('Query Performance (TPS)', fontweight='normal')
        ax_c2.set_xticks(x + width/2)
        ax_c2.set_xticklabels(query_labels, rotation=0)
        ax_c2.legend()
        ax_c2.grid(True, alpha=0.3, axis='y')
        ax_c2.set_ylim(bottom=0)
    else:
        ax_c2.text(0.5, 0.5, 'No data available', transform=ax_c2.transAxes, ha='center', va='center')

    # Calculate query start times for each database
    query_start_times = {}
    for db in databases:
        # Note: Resource monitoring starts after startup, so we don't include startup time.
        # There is a small delay (sleep 5) in run_tests.sh before data loading starts
        start_time = (data_loading_times[db] or 0) + (index_creation_times[db] or 0)
        query_start_times[db] = start_time - 5 # Adjust for sleep delay

    # 7. Resource Usage - CPU (Bottom-Left)
    ax_cpu = plt.subplot2grid((3, 4), (2, 0), colspan=2, fig=fig_combined)
    
    # 8. Resource Usage - Memory (Bottom-Right)
    ax_mem = plt.subplot2grid((3, 4), (2, 2), colspan=2, fig=fig_combined)
    
    has_resource_data = False
    for i, db in enumerate(databases):
        if resource_usage[db]['timestamps']:
            has_resource_data = True
            # CPU plot
            ax_cpu.plot(resource_usage[db]['timestamps'], resource_usage[db]['cpu'], 
                       label=f"{db.title()}", color=colors[i], linewidth=2)
            # Memory plot
            ax_mem.plot(resource_usage[db]['timestamps'], resource_usage[db]['memory'], 
                       label=f"{db.title()}", color=colors[i], linewidth=2)
            # Add vertical line for query start on both plots
            if query_start_times[db] > 0:
                ax_cpu.axvline(x=query_start_times[db], color=colors[i], linestyle=':', linewidth=2, label=f'{db.title()} Query Start')
                ax_mem.axvline(x=query_start_times[db], color=colors[i], linestyle=':', linewidth=2, label=f'{db.title()} Query Start')
    
    if has_resource_data:
        ax_cpu.set_xlabel('Time (s)')
        ax_cpu.set_ylabel('CPU (Cores)')
        ax_cpu.set_title('CPU Usage', fontweight='normal')
        ax_cpu.legend(loc='upper left')
        ax_cpu.grid(True, alpha=0.3)
        
        ax_mem.set_xlabel('Time (s)')
        ax_mem.set_ylabel('Memory (MiB)')
        ax_mem.set_title('Memory Usage', fontweight='normal')
        ax_mem.legend(loc='upper left')
        ax_mem.grid(True, alpha=0.3)
    else:
        ax_cpu.text(0.5, 0.5, 'No data available', transform=ax_cpu.transAxes, ha='center', va='center')
        ax_mem.text(0.5, 0.5, 'No data available', transform=ax_mem.transAxes, ha='center', va='center')

    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    combined_plot_file = os.path.join(plots_dir, f'{scale}_{concurrency}_{transactions}_combined_summary.png')
    plt.savefig(combined_plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Combined summary plot saved to: {combined_plot_file}")

    # Generate summary text file
    summary_file = os.path.join(plots_dir, f'{scale}_{concurrency}_{transactions}_performance_summary.txt')
    with open(summary_file, 'w') as f:
        f.write("# Performance Comparison Summary\n\n")

        # Add startup times
        f.write("Startup Times:\n")
        for db in databases:
            if startup_times[db] is not None:
                f.write(f"  {db.title()}: {startup_times[db]:.2f}s\n")
            else:
                f.write(f"  {db.title()}: N/A\n")

        f.write("\n")

        # Add data loading & indexing times
        f.write("Data Loading & Indexing Times:\n")
        for db in databases:
            value = data_loading_times[db]
            if index_creation_times[db] is not None:
                value = (value or 0) + index_creation_times[db]
            if value is not None:
                f.write(f"  {db.title()}: {value:.2f}s\n")
            else:
                f.write(f"  {db.title()}: N/A\n")

        f.write("\n")
        
        # Add Database Sizes
        f.write("Database Sizes:\n")
        for db in databases:
            if index_sizes[db] is not None:
                f.write(f"  {db.title()}: {index_sizes[db] / (1024*1024):.2f} MB\n")
            else:
                f.write(f"  {db.title()}: N/A\n")
        f.write("\n")
        
        # Add Resource Usage Summary (Peak)
        f.write("Peak Resource Usage:\n")
        for db in databases:
            if resource_usage[db]['cpu']:
                peak_cpu = max(resource_usage[db]['cpu'])
                peak_mem = max(resource_usage[db]['memory'])
                f.write(f"  {db.title()}: {peak_cpu:.2f} Cores, {peak_mem:.2f} MiB\n")
            else:
                f.write(f"  {db.title()}: N/A\n")
        f.write("\n")

        for i, (query, label) in enumerate(zip(queries, query_labels)):
            f.write(f"Query {i+1}: {label}\n")

            for db in databases:
                time_seconds = query_times[query][db]
                tps = query_tps[query][db]
                if time_seconds is not None:
                    f.write(f"  {db.title()}: {time_seconds:.4f}s")
                    if tps is not None:
                        f.write(f" ({tps:.2f} TPS)")
                    f.write("\n")
                else:
                    f.write(f"  {db.title()}: N/A\n")

            f.write("\n")

        # Add total query durations
        f.write("Total Query Duration:\n")
        for db in databases:
            if total_query_times[db] > 0:
                f.write(f"  {db.title()}: {total_query_times[db]:.4f}s\n")
            else:
                f.write(f"  {db.title()}: N/A\n")

        f.write("\n")

        # Add total durations
        f.write("Total Workflow Duration (Setup + Ingest + Query):\n")
        for db in databases:
            if total_times[db] > 0:
                f.write(f"  {db.title()}: {total_times[db]:.4f}s\n")
            else:
                f.write(f"  {db.title()}: N/A\n")

        f.write("\n")

        # Add TPS summary
        f.write("TPS Summary (Average across queries):\n")
        for db in databases:
            total_tps = 0
            count = 0
            for query in queries:
                if query_tps[query][db] is not None:
                    total_tps += query_tps[query][db]
                    count += 1
            if count > 0:
                avg_tps = total_tps / count
                f.write(f"  {db.title()}: {avg_tps:.2f} TPS\n")
            else:
                f.write(f"  {db.title()}: N/A\n")

        f.write("\n")

    print(f"Summary text saved to: {summary_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate performance comparison plots')
    parser.add_argument('--databases', nargs='+', required=True, help='List of databases to compare')
    parser.add_argument('--scale', required=True, help='Data scale (small, medium, large)')
    parser.add_argument('--concurrency', required=True, help='Concurrency level')
    parser.add_argument('--transactions', required=True, help='Number of transactions')
    parser.add_argument('--results-dir', required=True, help='Directory containing results')
    parser.add_argument('--plots-dir', required=True, help='Directory to save plots')

    args = parser.parse_args()

    print(f"Generating plots for databases: {args.databases}, scale: {args.scale}, concurrency: {args.concurrency}, transactions: {args.transactions}")

    generate_plots(args.databases, args.results_dir, args.plots_dir, args.scale, args.concurrency, args.transactions)

if __name__ == '__main__':
    main()