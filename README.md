# Electric Vehicle Population Analytics — Big Data Pipeline

A distributed **Big Data** pipeline that analyses the *Electric Vehicle Population*
dataset using **Apache Hadoop (HDFS + YARN)** for storage/resource management and
**Apache Spark** for distributed processing — the whole cluster is defined with
**Docker Compose** and spins up with a single command.

![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C?logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-66CCFF?logo=apachehadoop&logoColor=black)
![Python](https://img.shields.io/badge/python-3.x-3776AB?logo=python&logoColor=white)

---

## Table of contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Analyses performed](#analyses-performed)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Dataset](#dataset)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Web UIs](#web-uis)
- [Design notes](#design-notes)

---

## Overview

The pipeline demonstrates a classic distributed data-processing stack end to end:

1. A raw CSV is loaded into **HDFS**, replicated across multiple DataNodes.
2. **PySpark** jobs read the data out of HDFS and run distributed aggregations.
3. **YARN** provides the resource-management layer for the Hadoop cluster; the Spark
   cluster runs in **standalone mode** with one master and three workers.

It is intentionally dependency-free of managed cloud services so it can run locally
on any machine with Docker.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                     Docker network (bridge)                    │
│                                                                │
│   ┌───────────┐     ┌─────────────┐      ┌──────────────────┐  │
│   │ NameNode  │────▶│ DataNode 1  │      │ ResourceManager  │  │
│   │  (HDFS)   │     ├─────────────┤      │     (YARN)       │  │
│   │ :9870/:9000│    │ DataNode 2  │      └────────┬─────────┘  │
│   └───────────┘     ├─────────────┤               │            │
│                     │ DataNode 3  │      ┌────────▼─────────┐  │
│                     └─────────────┘      │   NodeManager 1  │  │
│                                          └──────────────────┘  │
│   ┌───────────────────────────────────────────────────────┐   │
│   │                     Spark cluster                      │   │
│   │   ┌───────────────┐    ┌────────────┐ ┌────────────┐   │   │
│   │   │ Spark Master  │───▶│ Worker 1   │ │ Worker 2   │…  │   │
│   │   │  :7077/:8080  │    └────────────┘ └────────────┘   │   │
│   │   └───────────────┘                                    │   │
│   └───────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

**Data flow**

1. The raw CSV (mounted from `./datasets`) is put into **HDFS** under `/datasets`.
2. A **PySpark** job reads the CSV from `hdfs://namenode:9000/...`, normalizes the
   column names, and computes distributed aggregations.
3. Results are printed on the Spark driver console (and can be extended to write back
   to HDFS, Parquet, or a dashboard).

### Cluster sizing

| Service | Count | Image |
|---|---|---|
| NameNode (HDFS) | 1 | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` |
| DataNode (HDFS) | 3 | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` |
| ResourceManager (YARN) | 1 | `bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8` |
| NodeManager (YARN) | 1 | `bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8` |
| Spark Master | 1 | `apache/spark:3.5.1` |
| Spark Worker | 3 | `apache/spark:3.5.1` |

---

## Analyses performed

Both `scripts/analyse.py` (cluster mode) and `scripts/graph.py` (local/cluster mode)
compute the same aggregations over the EV dataset:

- **Top states** by number of registered electric vehicles
- **Top cities** by vehicle count
- **Top makes** (manufacturers) by volume
- **Vehicle distribution by model year**
- **EV type breakdown** (BEV vs PHEV)
- **CAFV eligibility distribution**
- **Average electric range by manufacturer**

Column names are normalized before analysis (`analyse.py::clean_column_name`): lowercased,
non-alphanumerics collapsed to `_`, duplicate/leading/trailing underscores removed.

---

## Tech stack

| Layer | Technology | Version |
|---|---|---|
| Distributed storage | Apache Hadoop HDFS | 3.2.1 |
| Resource management | Apache YARN | 3.2.1 |
| Distributed processing | Apache Spark | 3.5.1 |
| Language | Python / PySpark | 3.x |
| Containerization | Docker & Docker Compose | — |

---

## Project structure

```
ev-analytics-spark-hadoop/
├── docker-compose.yml        # full cluster definition (Hadoop + Spark)
├── Dockerfile.spark          # custom Spark image with Python/pip deps
├── datasets/
│   └── Electric_Vehicle_Population_Data.csv   # source dataset (~65 MB)
└── scripts/
    ├── analyse.py            # PySpark job — reads from HDFS, runs analytics
    └── graph.py              # flexible local/cluster-mode analytics
```

---

## Dataset

**Electric Vehicle Population Data** — publicly available from the
[Washington State Department of Licensing](https://catalog.data.gov/dataset/electric-vehicle-population-data).
It contains registration records for Battery Electric Vehicles (BEV) and Plug-in
Hybrid Electric Vehicles (PHEV) in Washington State: make, model, model year, electric
range, city, county, state, and CAFV eligibility.

| Property | Value |
|---|---|
| Filename | `datasets/Electric_Vehicle_Population_Data.csv` |
| Size | ~64.6 MB |
| Rows | 279,781 (incl. header) |

> The dataset is **included in this repository** so the pipeline runs out of the box.
> To use a different dataset, drop a CSV in `datasets/` and update the path in the
> scripts (or the `DATA_PATH` env var for `graph.py`).

---

## Getting started

### Prerequisites

- [Docker](https://www.docker.com/) ≥ 20.x
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.x
- ~8 GB RAM available for the cluster

### 1. Clone the repository

```bash
git clone https://github.com/AzizMadhbouh/ev-analytics-spark-hadoop.git
cd ev-analytics-spark-hadoop
```

### 2. Start the cluster

```bash
docker compose up -d
```

This spins up 1 NameNode + 3 DataNodes, a YARN ResourceManager + NodeManager, and a
Spark master + 3 workers.

### 3. Load the dataset into HDFS

```bash
# Wait for the NameNode to be ready, then:
docker exec namenode hdfs dfs -mkdir -p /datasets
docker exec namenode hdfs dfs -put /datasets/Electric_Vehicle_Population_Data.csv /datasets/
```

### 4. Run the analytics job

Cluster mode (reads from HDFS, submitted to the Spark master):

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/analyse.py
```

Local mode (reads a local CSV, no cluster required):

```bash
docker exec spark-master python3 /opt/scripts/graph.py
```

### 5. Tear down

```bash
docker compose down          # stops the cluster
docker compose down -v       # also removes volumes
```

---

## Configuration

Key environment variables in `docker-compose.yml`:

| Variable | Description |
|---|---|
| `CLUSTER_NAME` | Hadoop cluster name |
| `CORE_CONF_fs_defaultFS` | Default HDFS URI (`hdfs://namenode:9000`) |
| `YARN_CONF_yarn_resourcemanager_hostname` | ResourceManager hostname |
| `SPARK_MASTER` | Spark master URL used by `graph.py` (default `local[*]`) |
| `DATA_PATH` | CSV path used by `graph.py` in local mode |

`graph.py` also ignores a stale `SPARK_HOME` that points to a non-existent path,
which avoids a common PySpark startup failure.

---

## Web UIs

| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Spark Master | http://localhost:8080 |

---

## Design notes

- **`analyse.py`** hard-codes the cluster master and the HDFS path — ideal for the
  submitted-job flow.
- **`graph.py`** is environment-driven (`SPARK_MASTER`, `DATA_PATH`) so the same code
  runs either against the cluster or purely locally — useful for quick iteration
  without the full Docker cluster.

---

## License

Released under the [MIT License](LICENSE).

Copyright (c) 2026 Mohamed Aziz Madhbouh.
