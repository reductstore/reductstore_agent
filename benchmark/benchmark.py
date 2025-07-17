# %% Imports
import numpy as np
import os
import psutil
import time
import subprocess
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import threading
from pathlib import Path

# %% Paths
BASE_DIR = Path(__file__).parent.resolve()
bag_file = BASE_DIR / "test_data.mcap"
output_dir = BASE_DIR / "output_rosbag"
log_dir = BASE_DIR / "logs_rosbag"
ros2_path = "/opt/ros/jazzy/bin/ros2"

# %% Plots CPU, memory, and disk write over time
def plot_metrics(df):

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    plt.figure(figsize=(8, 10))
    
    plt.subplot(3, 1, 1)
    plt.plot(df["timestamp"], df["cpu_usage_percent"], color='orange')
    plt.title("CPU Usage")
    plt.ylabel("CPU (%)")
    plt.grid(False)
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    #plt.xticks(rotation=45, ha='right')
    
    plt.subplot(3, 1, 2)
    plt.plot(df["timestamp"], df["mem_usage_mb"], color='blue')
    plt.title("Memory Usage")
    plt.ylabel("Memory (MB)")
    plt.grid(False)
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    #plt.xticks(rotation=45, ha='right')
    
    plt.subplot(3, 1, 3)
    plt.plot(df["timestamp"], df["disk_write_kb_per_s"], color='green')
    plt.title("Disk Write Throughput")
    plt.xlabel("Time")
    plt.ylabel("kB/s")
    plt.grid(False)
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    #plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.show()

# %% Monitors CPU, memory, and disk write of a process by PID
def monitor(stop_event, monitor_data, interval, pid):
    try:
        proc = psutil.Process(pid) # Get process by PID
    except psutil.NoSuchProcess:
        return # Exit if process not found
    
    prev_write_bytes = None # Previous disk write bytes
    
    while not stop_event.is_set():
        try:
            cpu = proc.cpu_percent(interval=interval) # CPU usage
            mem = proc.memory_info().rss / (1024 * 1024) # Memory usage
            
            try:
                write_bytes = proc.io_counters().write_bytes # Disk writes
                prev_write_bytes = write_bytes 
            except psutil.AccessDenied:
                if prev_write_bytes is None:
                    write_bytes = 0
                    prev_write_bytes = 0
                else:
                    write_bytes = prev_write_bytes
            except psutil.NoSuchProcess:
                break

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # Timestamp

            monitor_data.append((timestamp, cpu, mem, write_bytes))
            
        except psutil.NoSuchProcess:
            break # Stop if process ended

# %% Runs ros2 bag play and ros2 bag record while monitoring resource usage
def run_ros2_recorder():
    interval = 0.1 # Monitoring interval (s)

    os.makedirs(log_dir, exist_ok=True) # Create log dir if needed
    
    # Clear old log files
    for file in ["monitor_log.csv", "bag_play.log", "rosbag_record.log"]:
        open(os.path.join(log_dir, file), "w").close()

    shutil.rmtree(output_dir, ignore_errors=True) # Remove old recordings

    monitor_data = [] # Resource data
    stop_event = threading.Event() # Stop flag for monitoring
    
    # Open log files for subprocess output
    with open(os.path.join(log_dir, "bag_play.log"), "w") as bag_play_log, \
         open(os.path.join(log_dir, "rosbag_record.log"), "w") as rosbag_record_log:

        # Start ros2 bag playback
        bag_play = subprocess.Popen(
            [ros2_path, "bag", "play", bag_file],
            stdout=bag_play_log,
            stderr=subprocess.STDOUT
        )

        # Start recording selected topics using ROS 2 bag with Zstd compression
        rosbag_record = subprocess.Popen(
            [
                ros2_path, "bag", "record", "-o", output_dir,
                "--compression-format", "zstd",
                "--compression-mode", "message",
                "--storage", "mcap",
                "/nissan/vehicle_speed", "/nissan/vehicle_steering"
            ],
            stdout=rosbag_record_log,
            stderr=subprocess.STDOUT
        )

        # Start monitoring threads
        monitor_thread = threading.Thread(target=monitor, args=(stop_event, monitor_data, interval, rosbag_record.pid))
        monitor_thread.start()

        bag_play.wait() # Wait for playback to finish

        rosbag_record.terminate() # Terminate recording

        stop_event.set() # # Stop monitoring
        monitor_thread.join() # Wait for monitoring to finish

    # Save metrics
    df = pd.DataFrame(monitor_data, columns=["timestamp", "cpu_usage_percent", "mem_usage_mb", "disk_write_kb_per_s"])
    df["disk_write_kb_per_s"] = df["disk_write_kb_per_s"].diff().fillna(0) / 1024 / interval
    df.to_csv(os.path.join(log_dir, "monitor_log.csv"), index=False)
    
    return df

# %% Calculates average CPU, peak RAM, disk throughput, and recorded data size
def report_metrics(df):

    cpu_avg = df["cpu_usage_percent"].mean()                # Avg CPU usage (%)
    ram_peak = df["mem_usage_mb"].max()                     # Peak RAM usage (MB)
    write_throughput_avg = df["disk_write_kb_per_s"].mean() # Avg disk write (KB/s)
    
    total_record_size_bytes = 0
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            total_record_size_bytes += os.path.getsize(os.path.join(root, f))
    total_record_size_kb = total_record_size_bytes / 1024   # Recorded data size
    
    return cpu_avg, ram_peak, write_throughput_avg, total_record_size_kb

# %% Print system resource usage statistics and total size of the recorded data
def print_metrics(df):
    cpu_avg, ram_peak, write_throughput_avg, total_record_size_mb = report_metrics(df)
    print(f"Average CPU usage         : {cpu_avg:.2f}%")
    print(f"Peak RAM usage            : {ram_peak:.2f} MB")
    print(f"Average write throughput  : {write_throughput_avg:.2f} KB/s")
    print(f"Total record size         : {total_record_size_mb:.2f} KB")

# %% Run recording and collect performance metrics
metrics = run_ros2_recorder()

# %% Print and plot metrics
print_metrics(metrics)
plot_metrics(metrics)

# %% Run ros2 recorder 50 times and collect performance metrics
cpu, ram, throughput, size = [], [], [], []
for _ in range(50):
    metrics = run_ros2_recorder()
    c, r, t, s = report_metrics(metrics)
    cpu.append(c)
    ram.append(r)
    throughput.append(t)
    size.append(s)
    time.sleep(10)

# %% Print median values for metrics
print(f"Average CPU usage         : {np.median(cpu):.2f}%")
print(f"Peak RAM usage            : {np.median(ram):.2f} MB")
print(f"Average write throughput  : {np.median(throughput):.2f} KB/s")
print(f"Total record size         : {np.median(size):.2f} KB")

# %% Plot boxplots for CPU, RAM, and write throughput
fig, axs = plt.subplots(1, 3, figsize=(12, 4))

axs[0].boxplot(cpu)
axs[0].set_title('Average CPU usage')

axs[1].boxplot(ram)
axs[1].set_title('Peak RAM usage')

axs[2].boxplot(throughput)
axs[2].set_title('Average write throughput')

plt.tight_layout()
plt.show()

# %%
# Before running:
# 1. Start the ReductStore
# 2. Activate the environment
# 3. Build the agent

# %% Imports
import numpy as np
import os
import psutil
import time
import subprocess
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import threading
from pathlib import Path
from reduct import Client

# %% Paths
BASE_DIR = Path(__file__).parent.resolve()
bag_file = BASE_DIR / "test_data.mcap"
config_path = BASE_DIR / "config.yaml"
log_dir = BASE_DIR / "logs_agent"
ros2_path = "/opt/ros/jazzy/bin/ros2"

# %% Runs script to get current ReductStore size
def get_reductstore_size():
    result = subprocess.run(
        ["python3", "get_reductstore_size.py"],
        capture_output=True,
        text=True
    )
    return float(result.stdout.strip()) # Return size as float

start_size = get_reductstore_size() # Initial size before recording

# %% Plots CPU, memory, and disk write over time
def plot_metrics(df):

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    plt.figure(figsize=(8, 10))
    
    plt.subplot(3, 1, 1)
    plt.plot(df["timestamp"], df["cpu_usage_percent"], color='orange')
    plt.title("CPU Usage")
    plt.ylabel("CPU (%)")
    plt.grid(False)
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    #plt.xticks(rotation=45, ha='right')
    
    plt.subplot(3, 1, 2)
    plt.plot(df["timestamp"], df["mem_usage_mb"], color='blue')
    plt.title("Memory Usage")
    plt.ylabel("Memory (MB)")
    plt.grid(False)
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    #plt.xticks(rotation=45, ha='right')
    
    plt.subplot(3, 1, 3)
    plt.plot(df["timestamp"], df["disk_write_kb_per_s"], color='green')
    plt.title("Disk Write Throughput")
    plt.xlabel("Time")
    plt.ylabel("kB/s")
    plt.grid(False)
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    #plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.show()

# %% Monitors CPU, memory, and disk write of a process by PID
def monitor(stop_event, monitor_data, interval, pid):
    try:
        proc = psutil.Process(pid) # Get process by PID
    except psutil.NoSuchProcess:
        return # Exit if process not found

    prev_write_bytes = 0 # Previous disk write bytes

    while not stop_event.is_set():
        try:
            processes = [proc] + proc.children(recursive=True) # Main + child processes
            cpu_total = 0
            mem_total = 0
            write_bytes_total = 0

            for p in processes:
                try:
                    cpu_total += p.cpu_percent(interval=None if p != proc else interval) # CPU usage
                    mem_total += p.memory_info().rss # Memory usage
                    write_bytes_total += p.io_counters().write_bytes # Disk writes
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            mem_total /= 1024 * 1024 # Convert to MB

            write_bytes = write_bytes_total if write_bytes_total else prev_write_bytes
            prev_write_bytes = write_bytes_total
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] # Timestamp
            
            monitor_data.append((timestamp, cpu_total, mem_total, write_bytes))

        except psutil.NoSuchProcess:
            break # Stop if process ended

# %% Terminates a process and its child processes
def kill_process_tree(pid, include_parent=True):
    try:
        parent = psutil.Process(pid) # Get parent process
        children = parent.children(recursive=True) # Get all child processes
        for child in children:
            child.terminate() # Try graceful termination
        gone, alive = psutil.wait_procs(children, timeout=5) # Wait for them to exit
        for p in alive:
            print(f"Force killing {p.pid}")
            p.kill() # Force kill if still alive
        if include_parent:
            parent.terminate() # Terminate parent if needed
            parent.wait(5) # Wait for parent to exit
    except psutil.NoSuchProcess:
        pass # Process already exited

# %% Runs ros2 bag play and agent while monitoring resource usage
def run_agent_recorder():
    interval = 0.1 # Monitoring interval (s)

    os.makedirs(log_dir, exist_ok=True) # Create log dir if missing
    
    # Clear old log files
    for file in ["monitor_log.csv", "bag_play.log", "agent_record.log", "reductstore_monitor_log.csv"]:
        open(os.path.join(log_dir, file), "w").close()

    agent_data = [] # Agent resource data
    reductstore_data = [] # ReductStore resource data
    stop_event = threading.Event() # Stop flag for monitoring
    
    # Get ReductStore process PID
    reductstore_pid = None
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = " ".join(proc.info["cmdline"])
            if proc.info["cmdline"] and "reductstore" in proc.info["cmdline"][0] and "reductstore_agent" not in " ".join(proc.info["cmdline"]):
                reductstore_pid = proc.info["pid"]
                break
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if not reductstore_pid:
        raise RuntimeError("ReductStore process not found.")

    # Open log files for subprocess output
    with open(os.path.join(log_dir, "bag_play.log"), "w") as bag_play_log, \
         open(os.path.join(log_dir, "agent_record.log"), "w") as agent_record_log:

        # Start ros2 bag playback
        bag_play = subprocess.Popen(
            [ros2_path, "bag", "play", bag_file],
            stdout=bag_play_log,
            stderr=subprocess.STDOUT
        )

        # Start agent recording
        agent = subprocess.Popen(
            [
                "ros2", "run", "reductstore_agent", "recorder",
                "--ros-args", "--params-file", config_path
            ],
            stdout=agent_record_log,
            stderr=subprocess.STDOUT
        )
             
        # Start monitoring threads
        monitor_agent_thread = threading.Thread(target=monitor, args=(stop_event, agent_data, interval, agent.pid))
        monitor_rs_thread = threading.Thread(target=monitor, args=(stop_event, reductstore_data, interval, reductstore_pid))

        monitor_agent_thread.start()
        monitor_rs_thread.start()

        bag_play.wait() # Wait for playback to finish
        
        try:
            agent.wait(timeout=1) # Wait briefly for agent
        except subprocess.TimeoutExpired:
            kill_process_tree(agent.pid) # Force kill if needed

        stop_event.set() # Stop monitoring
        monitor_agent_thread.join() # Wait for monitor threads
        monitor_rs_thread.join()

    # Save agent metrics
    df_agent = pd.DataFrame(agent_data, columns=["timestamp", "cpu_usage_percent", "mem_usage_mb", "disk_write_kb_per_s"])
    df_agent["disk_write_kb_per_s"] = df_agent["disk_write_kb_per_s"].diff().fillna(0) / 1024 / interval
    df_agent.to_csv(os.path.join(log_dir, "monitor_log.csv"), index=False)

    # Save ReductStore metrics
    df_rs = pd.DataFrame(reductstore_data, columns=["timestamp", "cpu_usage_percent", "mem_usage_mb", "disk_write_kb_per_s"])
    df_rs["disk_write_kb_per_s"] = df_rs["disk_write_kb_per_s"].diff().fillna(0) / 1024 / interval
    df_rs.to_csv(os.path.join(log_dir, "reductstore_monitor_log.csv"), index=False)
    
    return df_agent, df_rs

# %% Calculates average CPU, peak RAM, disk throughput, and recorded data size
def report_metrics(df):
    cpu_avg = df["cpu_usage_percent"].mean()                # Avg CPU usage (%)
    ram_peak = df["mem_usage_mb"].max()                     # Peak RAM usage (MB)
    write_throughput_avg = df["disk_write_kb_per_s"].mean() # Avg disk write (KB/s)
    result_size = get_reductstore_size()                    # ReductStore size after recording
    record_size = result_size - start_size                  # Recorded data size
    return cpu_avg, ram_peak, write_throughput_avg, record_size

# %% Print system resource usage statistics and total size of the recorded data
def print_metrics(df):
    cpu_avg, ram_peak, write_throughput_avg, record_size = report_metrics(df)
    print(f"Average CPU usage         : {cpu_avg:.2f}%")
    print(f"Peak RAM usage            : {ram_peak:.2f} MB")
    print(f"Average write throughput  : {write_throughput_avg:.2f} KB/s")
    print(f"Record size               : {record_size:.2f} Kb")

# %% Run recording and collect performance metrics for agent and ReductStore
metrics_agent, metrics_rs = run_agent_recorder()

# %% Print and plot metrics for both agent and ReductStore processes
print_metrics(metrics_agent)
plot_metrics(metrics_agent)
print_metrics(metrics_rs)
plot_metrics(metrics_rs)
