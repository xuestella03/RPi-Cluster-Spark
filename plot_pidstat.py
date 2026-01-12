#!/usr/bin/env python3
"""
Plot pidstat logs to visualize Spark executor performance
"""

import re
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys

def parse_pidstat_log(filepath):
    """Parse pidstat log file and extract metrics"""
    data = []
    
    with open(filepath, 'r') as f:
        for line in f:
            # Skip headers and empty lines
            if line.startswith('#') or not line.strip():
                continue
            
            # Parse data lines
            parts = line.split()
            if len(parts) >= 17:
                try:
                    record = {
                        'time': parts[0],
                        'uid': int(parts[1]),
                        'pid': int(parts[2]),
                        'usr': float(parts[3]),
                        'system': float(parts[4]),
                        'cpu_pct': float(parts[7]),
                        'minflt': float(parts[9]),
                        'majflt': float(parts[10]),
                        'vsz': int(parts[11]),
                        'rss': int(parts[12]),
                        'mem_pct': float(parts[13]),
                        'kb_rd': float(parts[14]),
                        'kb_wr': float(parts[15]),
                        'command': parts[17] if len(parts) > 17 else parts[16]
                    }
                    data.append(record)
                except (ValueError, IndexError):
                    continue
    
    return pd.DataFrame(data)

def plot_metrics(df, output_prefix='pidstat'):
    """Create plots for key metrics"""
    
    # Filter for executor process (high CPU usage indicates executor)
    # Usually the process with varying high CPU
    df['timestamp'] = pd.to_datetime(df['time'], format='%H:%M:%S')
    
    # Find the executor (process with highest average CPU)
    cpu_by_pid = df.groupby('pid')['cpu_pct'].mean()
    executor_pid = cpu_by_pid.idxmax()
    worker_pids = cpu_by_pid[cpu_by_pid < 50].index.tolist()
    
    executor_df = df[df['pid'] == executor_pid].copy()
    executor_df['seconds'] = (executor_df['timestamp'] - executor_df['timestamp'].min()).dt.total_seconds()
    
    print(f"Executor PID: {executor_pid}")
    print(f"Worker PID(s): {worker_pids}")
    print(f"Data points: {len(executor_df)}")
    
    # Create figure with subplots
    fig, axes = plt.subplots(4, 1, figsize=(14, 12))
    fig.suptitle(f'Spark Executor Performance (PID {executor_pid})', fontsize=16, fontweight='bold')
    
    # 1. CPU Usage
    ax1 = axes[0]
    ax1.plot(executor_df['seconds'], executor_df['cpu_pct'], 'b-', linewidth=1.5, label='Total CPU %')
    ax1.plot(executor_df['seconds'], executor_df['usr'], 'g--', alpha=0.7, label='User CPU %')
    ax1.plot(executor_df['seconds'], executor_df['system'], 'r--', alpha=0.7, label='System CPU %')
    ax1.axhline(y=400, color='gray', linestyle=':', alpha=0.5, label='4-core max (400%)')
    ax1.set_ylabel('CPU Usage (%)', fontsize=11, fontweight='bold')
    ax1.set_xlabel('Time (seconds)', fontsize=10)
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    ax1.set_title('CPU Utilization Over Time')
    
    # 2. Memory Usage
    ax2 = axes[1]
    ax2_twin = ax2.twinx()
    
    line1 = ax2.plot(executor_df['seconds'], executor_df['rss'] / 1024, 'b-', linewidth=1.5, label='RSS (MB)')
    line2 = ax2_twin.plot(executor_df['seconds'], executor_df['mem_pct'], 'r-', linewidth=1.5, label='Memory %')
    
    ax2.set_ylabel('RSS Memory (MB)', fontsize=11, fontweight='bold', color='b')
    ax2_twin.set_ylabel('Memory %', fontsize=11, fontweight='bold', color='r')
    ax2.set_xlabel('Time (seconds)', fontsize=10)
    ax2.tick_params(axis='y', labelcolor='b')
    ax2_twin.tick_params(axis='y', labelcolor='r')
    
    # Combine legends
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax2.legend(lines, labels, loc='upper left')
    ax2.grid(True, alpha=0.3)
    ax2.set_title('Memory Usage Over Time')
    
    # 3. Disk I/O
    ax3 = axes[2]
    ax3.plot(executor_df['seconds'], executor_df['kb_rd'] / 1024, 'b-', linewidth=1.5, label='Read (MB/s)')
    ax3.plot(executor_df['seconds'], executor_df['kb_wr'] / 1024, 'r-', linewidth=1.5, label='Write (MB/s)')
    ax3.set_ylabel('Disk I/O (MB/s)', fontsize=11, fontweight='bold')
    ax3.set_xlabel('Time (seconds)', fontsize=10)
    ax3.legend(loc='upper right')
    ax3.grid(True, alpha=0.3)
    ax3.set_title('Disk I/O Over Time')
    
    # 4. Page Faults
    ax4 = axes[3]
    ax4.plot(executor_df['seconds'], executor_df['minflt'], 'g-', linewidth=1.5, label='Minor Faults/s')
    ax4.plot(executor_df['seconds'], executor_df['majflt'], 'r-', linewidth=1.5, label='Major Faults/s')
    ax4.set_ylabel('Page Faults/s', fontsize=11, fontweight='bold')
    ax4.set_xlabel('Time (seconds)', fontsize=10)
    ax4.legend(loc='upper right')
    ax4.grid(True, alpha=0.3)
    ax4.set_title('Page Faults (Memory Pressure Indicator)')
    ax4.set_yscale('log')  # Log scale since major faults are usually much lower
    
    plt.tight_layout()
    
    # Save figure
    output_file = f'{output_prefix}_analysis.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_file}")
    
    # Print summary statistics
    print("\n" + "="*60)
    print("PERFORMANCE SUMMARY")
    print("="*60)
    print(f"Duration: {executor_df['seconds'].max():.1f} seconds")
    print(f"\nCPU Usage:")
    print(f"  Average: {executor_df['cpu_pct'].mean():.1f}%")
    print(f"  Peak: {executor_df['cpu_pct'].max():.1f}%")
    print(f"  Cores utilized (avg): {executor_df['cpu_pct'].mean()/100:.2f} cores")
    print(f"\nMemory:")
    print(f"  Peak RSS: {executor_df['rss'].max()/1024:.1f} MB")
    print(f"  Peak Memory %: {executor_df['mem_pct'].max():.1f}%")
    print(f"\nDisk I/O:")
    print(f"  Peak Read: {executor_df['kb_rd'].max()/1024:.1f} MB/s")
    print(f"  Peak Write: {executor_df['kb_wr'].max()/1024:.1f} MB/s")
    print(f"  Total Read: {executor_df['kb_rd'].sum()/1024/1024:.1f} GB")
    print(f"\nPage Faults:")
    print(f"  Total Minor: {executor_df['minflt'].sum():.0f}")
    print(f"  Total Major: {executor_df['majflt'].sum():.0f}")
    print("="*60)
    
    return fig

def main():
    if len(sys.argv) < 2:
        print("Usage: python plot_pidstat.py <pidstat_log_file> [output_prefix]")
        print("\nExample:")
        print("  python plot_pidstat.py pidstat_raspberry-pi_20250111.log rpi")
        sys.exit(1)
    
    log_file = sys.argv[1]
    output_prefix = sys.argv[2] if len(sys.argv) > 2 else 'pidstat'
    
    print(f"Parsing {log_file}...")
    df = parse_pidstat_log(log_file)
    
    if df.empty:
        print("Error: No data found in log file")
        sys.exit(1)
    
    print(f"Found {len(df)} data points")
    
    plot_metrics(df, output_prefix)
    
    plt.show()

if __name__ == '__main__':
    main()