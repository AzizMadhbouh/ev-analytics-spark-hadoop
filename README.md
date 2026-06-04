# Electric Vehicle Population Analytics — Big Data Pipeline

A distributed big data pipeline for analysing the **Electric Vehicle Population Dataset** using **Apache Hadoop (HDFS + YARN)** and **Apache Spark**, all orchestrated with **Docker Compose**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Docker Network (bridge)              │
│                                                             │
│   ┌──────────┐     ┌────────────┐   ┌────────────────────┐ │
│   │NameNode  │────▶│ DataNode 1 │   │  ResourceManager   │ │
│   │(HDFS)    │     │            │   │  (YARN)            │ │
│   │:9870/:9000│    ├────────────┤   └────────┬───────────┘ │
│   └──────────┘     │ DataNode 2 │            │             │
│                    ├────────────┤   ┌─────────▼──────────┐ │
│                    │ DataNode 3 │   │   NodeManager 1    │ │
│                    └────────────┘   └────────────────────┘ │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐  │
│   │              Spark Cluster                          │  │
│   │  ┌──────────────┐   ┌──────────┐  ┌──────────┐     │  │
│   │  │ Spark Master │──▶│ Worker 1 │  │ Worker 2 │ ... │  │
│   │  │  :7077/:8080 │   └──────────┘  └──────────┘     │  │
│   │  └──────────────┘                                   │  │
│   └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

**Data flow:**

1. The raw CSV dataset is mounted into the NameNode container and loaded into **HDFS**.
2. **PySpark** jobs read the data from HDFS, clean column names, and run distributed aggregations.
3. Results are printed to the Spark driver console (and can be extended to write back to HDFS or a dashboard).

---

## Technologies

| Layer | Technology | Version |
|---|---|---|
| Distributed Storage | Apache Hadoop HDFS | 3.2.1 |
| Resource Management | Apache YARN | 3.2.1 |
| Distributed Processing | Apache Spark | 3.5.1 |
| Language | Python / PySpark | 3.x |
| Containerisation | Docker & Docker Compose | — |
| Base Images | `bde2020/hadoop-*`, `apache/spark` | — |

---

## Project Structure

```
bigdata(part2)/
├── docker-compose.yml        # Full cluster definition (Hadoop + Spark)
├── Dockerfile.spark          # Custom Spark image with Python & pip deps
├── datasets/
│   └── Electric_Vehicle_Population_Data.csv   # Source dataset (~67 MB)
└── scripts/
    ├── analyse.py            # PySpark job — reads from HDFS, runs analytics
    └── graph.py              # PySpark job — flexible local/cluster mode analytics
```

---

## Getting Started

### Prerequisites

- [Docker](https://www.docker.com/) ≥ 20.x
- [Docker Compose](https://docs.docker.com/compose/) ≥ 2.x
- ~8 GB RAM available for the cluster

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

### 2. Start the cluster

```bash
docker compose up -d
```

This spins up:
- 1 Hadoop NameNode + 3 DataNodes
- 1 YARN ResourceManager + 1 NodeManager
- 1 Spark Master + 3 Spark Workers

### 3. Load the dataset into HDFS

```bash
# Wait for the NameNode to be ready, then:
docker exec namenode hdfs dfs -mkdir -p /datasets
docker exec namenode hdfs dfs -put /datasets/Electric_Vehicle_Population_Data.csv /datasets/
```

### 4. Run the analytics job

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/scripts/analyse.py
```

Or run in local mode directly:

```bash
docker exec spark-master python3 /opt/scripts/graph.py
```

---

## Analyses Performed

The PySpark scripts compute the following aggregations over the EV dataset:

- **Top states** by number of registered electric vehicles
- **Top cities** by vehicle count
- **Top makes** (manufacturers) by volume
- **Vehicle distribution by model year**
- **EV type breakdown** (BEV vs PHEV)
- **CAFV eligibility distribution**
- **Average electric range by manufacturer**

---

## Web UIs

| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8088 |
| Spark Master | http://localhost:8080 |

---

## Configuration

Environment variables in `docker-compose.yml` control cluster settings. Key ones:

| Variable | Description |
|---|---|
| `CLUSTER_NAME` | Hadoop cluster name |
| `CORE_CONF_fs_defaultFS` | Default HDFS URI |
| `YARN_CONF_yarn_resourcemanager_hostname` | ResourceManager hostname |
| `SPARK_MASTER` | Spark master URL (overridable in `graph.py`) |
| `DATA_PATH` | CSV path used by `graph.py` in local mode |

---

## Dataset

**Electric Vehicle Population Data** — publicly available from the [Washington State Department of Licensing](https://catalog.data.gov/dataset/electric-vehicle-population-data). It contains registration records for Battery Electric Vehicles (BEV) and Plug-in Hybrid Electric Vehicles (PHEV) in Washington State, with fields including make, model, model year, electric range, city, county, state, and CAFV eligibility.

> The dataset file (~67 MB) is **not tracked by Git**. Download it separately and place it in the `datasets/` folder before running the pipeline.

Add this to your `.gitignore`:

