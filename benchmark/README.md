# Benchmark: rosbag2 vs ros2_reduct_agent

---

## Goal

---

Compare the performance of **rosbag2** and **ros2_reduct_agent** when recording data into MCAP files with Zstd compression.

## **Metrics to compare**

---

- Average CPU usage
- Peak RAM usage
- Disk write throughput
- Final record size

## Dataset

---

`test_data.mcap`

## Tools

---

- **rosbag2**
- **ros2_reduct_agent**

## Installation

---

Install **ReductStore:** https://www.reduct.store/docs/getting-started

Install **ros2_reduct_agent**: https://github.com/reductstore/reductstore_agent

## Usage Instructions

---

**1. Start ReductStore in a terminal:**

```bash
cd ~/Downloads/reductstore.aarch64-unknown-linux-gnu
RS_DATA_PATH=~/red_data ./reductstore
```

**2. In a new terminal build the agent and source the workspace:**

```bash
source ~/mcap_venv/bin/activate
cd ~/ros2_ws
colcon build --packages-select reductstore_agent
source install/local_setup.bash
```

**3. Run the benchmark:**

```bash
code ~/benchmark/benchmark.py
```