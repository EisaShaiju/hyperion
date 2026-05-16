# HYPERION — AI COPILOT CONTEXT

> Paste this file at the start of every coding session.
> It tells the copilot everything it needs to generate consistent, correctly-wired code for this project.

---

## 1. WHAT THIS PROJECT IS

**Hyperion** is a production-grade AIOps platform for intelligent virtual machine scheduling on cloud infrastructure. It uses Monte Carlo Tree Search (MCTS) + AC-3 Constraint Satisfaction as its scheduling core, a real-time Apache Kafka streaming pipeline for VM telemetry, deep learning models for anomaly detection and demand forecasting, a Graph Neural Network rollout policy, and a full MLOps lifecycle (MLflow, Airflow, Evidently). It deploys on AWS (EC2, MSK, RDS, ElastiCache, S3) and is observable via Prometheus + Grafana.

**The scheduling problem in one sentence:** Given N physical servers with P cores each, and K virtual machines with stochastic CPU/memory demand, find the optimal assignment of vCPUs to physical cores — respecting hard constraints (memory caps, affinity rules, SLA deadlines) — that maximises SLA compliance and throughput while minimising migration cost.

**Phase status at context generation:**
- [x] Phase 0 — Environment setup (docker-compose, repo scaffold, pyproject.toml)
- [ ] Phase 1 — Physical model + simulation engine
- [ ] Phase 2 — Kafka streaming layer
- [ ] Phase 3 — ML models (anomaly, forecasting, SLA predictor)
- [ ] Phase 4 — MCTS + CSP scheduler
- [ ] Phase 5 — GNN rollout policy
- [ ] Phase 6 — AIOps feedback loop
- [ ] Phase 7 — Observability + FastAPI control plane
- [ ] Phase 8 — AWS deployment + experiments

**Update this status block as phases complete.**

---

## 2. REPOSITORY LAYOUT

```
hyperion/                          ← project root
├── docker/
│   └── docker-compose.yml         ← ALL local services (Kafka, PG, Redis, MLflow, Prometheus, Grafana)
├── infra/                         ← Terraform (Phase 8)
│   ├── main.tf
│   ├── ec2.tf
│   ├── msk.tf
│   ├── rds.tf
│   ├── s3.tf
│   └── elasticache.tf
├── hyperion/                      ← main Python package
│   ├── __init__.py
│   ├── config.py                  ← Settings (Pydantic BaseSettings, reads .env)
│   ├── simulation/                ← Phase 1
│   │   ├── __init__.py
│   │   ├── models.py              ← Server, NUMANode, Core, VM, SLAProfile dataclasses
│   │   ├── hypervisor.py          ← HypervisorSimulator (pin / migrate / reslice)
│   │   └── workload.py            ← WorkloadGenerator (Gaussian mixture + Poisson bursts)
│   ├── streaming/                 ← Phase 2
│   │   ├── __init__.py
│   │   ├── topics.py              ← Kafka topic name constants + Pydantic message schemas
│   │   ├── producers/
│   │   │   ├── __init__.py
│   │   │   ├── vm_metrics.py      ← produces to vm.metrics
│   │   │   └── cloudwatch.py      ← CloudWatch → Kafka bridge
│   │   ├── consumers/
│   │   │   ├── __init__.py
│   │   │   ├── metrics_consumer.py
│   │   │   └── decisions_consumer.py
│   │   └── processors/
│   │       ├── __init__.py
│   │       ├── windowing.py       ← Faust stream processor (sliding windows)
│   │       └── feature_writer.py  ← writes features → Redis + S3
│   ├── ml/                        ← Phase 3 + 5
│   │   ├── __init__.py
│   │   ├── anomaly/
│   │   │   ├── __init__.py
│   │   │   ├── lstm_autoencoder.py
│   │   │   └── isolation_forest.py
│   │   ├── forecasting/
│   │   │   ├── __init__.py
│   │   │   ├── prophet_model.py
│   │   │   └── lstm_forecaster.py
│   │   ├── sla_predictor/
│   │   │   ├── __init__.py
│   │   │   └── xgboost_model.py
│   │   └── policy/                ← Phase 5
│   │       ├── __init__.py
│   │       ├── gnn.py             ← HyperionGNN (PyTorch Geometric)
│   │       └── trainer.py         ← self-play training loop
│   ├── scheduler/                 ← Phase 4
│   │   ├── __init__.py
│   │   ├── state.py               ← SchedulerState, SchedulingAction
│   │   ├── csp.py                 ← SchedulingCSP, AC-3, Constraint protocol
│   │   ├── mcts.py                ← MCTSNode, MCTSScheduler, UCT
│   │   └── baselines.py           ← RandomScheduler, FFDScheduler, RoundRobinScheduler
│   ├── aiops/                     ← Phase 6
│   │   ├── __init__.py
│   │   ├── rca.py                 ← RootCauseAnalyzer
│   │   ├── autoscaler.py          ← PredictiveAutoscaler
│   │   └── drift.py               ← DriftDetector (Evidently)
│   ├── api/                       ← Phase 7
│   │   ├── __init__.py
│   │   ├── main.py                ← FastAPI app factory
│   │   ├── dependencies.py        ← get_db, get_redis, get_kafka
│   │   ├── routes/
│   │   │   ├── cluster.py
│   │   │   ├── scheduler.py
│   │   │   └── models.py
│   │   └── ws.py                  ← WebSocket event broadcaster
│   └── observability/             ← Phase 7
│       ├── __init__.py
│       ├── metrics.py             ← all Prometheus Gauge/Counter/Histogram definitions
│       └── tracing.py             ← OpenTelemetry setup
├── dags/                          ← Airflow DAGs (Phase 6)
│   ├── retrain_anomaly.py
│   ├── retrain_forecaster.py
│   └── data_pipeline.py
├── experiments/                   ← Phase 8
│   ├── benchmark.py
│   └── configs/
├── tests/
│   ├── unit/
│   └── integration/
├── prometheus/
│   └── prometheus.yml
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       └── dashboards/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
├── .env                           ← never committed (in .gitignore)
├── .env.example                   ← committed, all keys with placeholder values
├── .pre-commit-config.yaml
├── pyproject.toml
└── docker/
    └── docker-compose.yml
```

---

## 3. EXACT TECH STACK AND VERSIONS

```
# Python
python = "3.11"

# Core
pydantic = "2.x"           # BaseModel + BaseSettings throughout
structlog = "24.x"         # ALL logging — never use stdlib logging directly

# Streaming
confluent-kafka = "2.3.x"  # Kafka producer + consumer (NOT kafka-python)
faust-streaming = "0.10.x" # Faust fork (Python 3.11 compatible)

# Data
sqlalchemy = "2.x"         # async engine, NOT 1.x patterns
alembic = "1.13.x"         # database migrations
asyncpg = "0.29.x"         # async PostgreSQL driver
redis = "5.x"              # async Redis client (redis.asyncio)
pyarrow = "14.x"           # Parquet read/write
boto3 = "1.34.x"           # AWS SDK (S3 operations)

# ML / DL
torch = "2.2.x"            # PyTorch
torch-geometric = "2.5.x"  # PyG (Graph Neural Networks)
scikit-learn = "1.4.x"
xgboost = "2.0.x"
prophet = "1.1.x"
numpy = "1.26.x"
pandas = "2.x"

# MLOps
mlflow = "2.10.x"          # experiment tracking + model registry
optuna = "3.5.x"           # hyperparameter search
dvc = "3.x"                # data version control
evidently = "0.4.x"        # drift detection

# API
fastapi = "0.110.x"
uvicorn = "0.27.x"         # with [standard] extras
websockets = "12.x"

# Observability
prometheus-client = "0.19.x"
opentelemetry-sdk = "1.22.x"

# Workflow
apache-airflow = "2.8.x"

# Dev
pytest = "8.x"
pytest-asyncio = "0.23.x"
ruff = "0.3.x"             # linter + formatter
mypy = "1.8.x"
pre-commit = "3.6.x"
```

---

## 4. CORE DATA MODELS

These are the canonical schemas. Every file that touches infrastructure state uses these exact types.

```python
# hyperion/simulation/models.py

from __future__ import annotations
from enum import Enum
from pydantic import BaseModel, Field
import uuid


class WorkloadType(str, Enum):
    CPU_INTENSIVE      = "cpu_intensive"
    MEMORY_INTENSIVE   = "memory_intensive"
    LATENCY_SENSITIVE  = "latency_sensitive"
    BATCH              = "batch"


class SLAProfile(BaseModel):
    max_p99_latency_ms:   float  # hard ceiling on tail latency
    min_throughput_rps:   float  # minimum requests per second
    max_memory_gb:        float  # hard memory cap
    priority:             int = Field(ge=1, le=5)  # 5 = highest


class Core(BaseModel):
    id:           str = Field(default_factory=lambda: str(uuid.uuid4()))
    physical_id:  int         # OS-visible core number
    numa_node_id: int
    is_pinned:    bool = False
    pinned_vm_id: str | None = None   # set when a vCPU is pinned here


class NUMANode(BaseModel):
    id:               int
    cores:            list[Core]
    local_memory_gb:  float
    # cross-NUMA memory access incurs ~40% latency overhead (modelled as multiplier)
    cross_numa_penalty: float = 1.4


class Server(BaseModel):
    id:                  str = Field(default_factory=lambda: str(uuid.uuid4()))
    aws_instance_type:   str            # e.g. "c5.xlarge"
    availability_zone:   str            # e.g. "ap-south-1a"
    numa_nodes:          list[NUMANode]
    total_memory_gb:     float
    allocated_memory_gb: float = 0.0
    hosted_vm_ids:       list[str] = Field(default_factory=list)

    @property
    def total_cores(self) -> int:
        return sum(len(n.cores) for n in self.numa_nodes)

    @property
    def available_memory_gb(self) -> float:
        return self.total_memory_gb - self.allocated_memory_gb


class VM(BaseModel):
    id:               str = Field(default_factory=lambda: str(uuid.uuid4()))
    name:             str
    vcpu_count:       int
    memory_req_gb:    float
    workload_type:    WorkloadType
    sla:              SLAProfile
    pinning_required: bool = False        # if True, each vCPU must have a dedicated core
    current_server_id: str | None = None  # None = unscheduled


class VMMetrics(BaseModel):
    """Real-time metrics — message schema for the vm.metrics Kafka topic."""
    vm_id:           str
    timestamp:       float              # unix epoch seconds
    cpu_pct:         float = Field(ge=0.0, le=100.0)
    memory_pct:      float = Field(ge=0.0, le=100.0)
    iops:            float = Field(ge=0.0)
    network_mbps:    float = Field(ge=0.0)
    source:          str = "simulation" # "simulation" | "cloudwatch"


class ClusterState(BaseModel):
    """Full snapshot of the data center — used as MCTS root state."""
    servers:     dict[str, Server]  # server_id → Server
    vms:         dict[str, VM]      # vm_id → VM
    metrics:     dict[str, VMMetrics]  # vm_id → latest metrics
    timestamp:   float
```

---

## 5. KAFKA TOPICS — NAMES, KEYS, AND SCHEMAS

```python
# hyperion/streaming/topics.py

# ─── Topic name constants ───────────────────────────────────────────────────
TOPIC_VM_METRICS         = "vm.metrics"          # 1s cadence per VM
TOPIC_VM_EVENTS          = "vm.events"           # on-event
TOPIC_SCHEDULER_DECISIONS = "scheduler.decisions" # on each MCTS decision
TOPIC_SLA_VIOLATIONS     = "sla.violations"      # on SLA breach
TOPIC_ANOMALY_SCORES     = "anomaly.scores"      # 10s cadence per VM

# ─── Message schemas (Pydantic, serialised as JSON) ─────────────────────────
from enum import Enum
from pydantic import BaseModel

class EventType(str, Enum):
    VM_STARTED      = "vm_started"
    VM_STOPPED      = "vm_stopped"
    SLA_RISK        = "sla_risk"          # XGBoost score > 0.7
    SPIKE_DETECTED  = "spike_detected"   # anomaly model fired
    SCALE_OUT_REQ   = "scale_out_request"

class VMEvent(BaseModel):
    vm_id:      str
    event_type: EventType
    severity:   float = 0.0   # 0–1
    metadata:   dict  = {}
    timestamp:  float

class ActionType(str, Enum):
    PIN_VCPU       = "pin_vcpu"
    MIGRATE_VM     = "migrate_vm"
    RESLICE        = "reslice"           # adjust time-slice quantum
    NO_OP          = "no_op"

class SchedulerDecision(BaseModel):
    decision_id:     str
    action_type:     ActionType
    vm_id:           str
    source_server_id: str | None
    target_server_id: str | None
    target_core_id:   str | None         # for PIN_VCPU
    new_quantum_ms:   int | None         # for RESLICE
    mcts_iterations:  int
    reward:          float               # realised reward after execution
    state_hash:      str                 # hash of ClusterState before action
    timestamp:       float

class SLAViolation(BaseModel):
    vm_id:          str
    violation_type: str     # "latency" | "throughput" | "memory"
    duration_ms:    float
    severity:       float   # 0–1
    measured_value: float
    sla_threshold:  float
    timestamp:      float

class AnomalyScore(BaseModel):
    vm_id:          str
    score:          float   # 0–1, higher = more anomalous
    model:          str     # "lstm_autoencoder" | "isolation_forest"
    model_version:  str
    features_hash:  str
    timestamp:      float
```

---

## 6. KAFKA PRODUCER / CONSUMER PATTERNS

```python
# ─── Producer (always use this pattern) ────────────────────────────────────
from confluent_kafka import Producer
from hyperion.config import settings

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id":         "hyperion-producer",
        "acks":              "all",          # durability
        "compression.type":  "lz4",
        "linger.ms":         5,              # micro-batching
    })

def produce(producer: Producer, topic: str, key: str, value: BaseModel) -> None:
    producer.produce(
        topic=topic,
        key=key.encode(),
        value=value.model_dump_json().encode(),
        on_delivery=_delivery_callback,
    )
    producer.poll(0)   # non-blocking, triggers callbacks

def _delivery_callback(err, msg):
    if err:
        logger.error("kafka_delivery_failed", error=str(err), topic=msg.topic())

# ─── Consumer (always use this pattern) ────────────────────────────────────
from confluent_kafka import Consumer, KafkaException

def make_consumer(group_id: str, topics: list[str]) -> Consumer:
    c = Consumer({
        "bootstrap.servers":  settings.kafka_bootstrap_servers,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,         # manual commit after processing
    })
    c.subscribe(topics)
    return c
```

---

## 7. DATABASE SCHEMAS

```sql
-- Run via Alembic migrations, NOT raw SQL in application code

-- VM registry
CREATE TABLE vms (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    vcpu_count      INTEGER NOT NULL,
    memory_req_gb   FLOAT NOT NULL,
    workload_type   TEXT NOT NULL,
    pinning_required BOOLEAN DEFAULT FALSE,
    current_server_id UUID,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Scheduling decisions log (append-only)
CREATE TABLE scheduling_decisions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    decision_id     TEXT UNIQUE NOT NULL,
    action_type     TEXT NOT NULL,
    vm_id           UUID REFERENCES vms(id),
    source_server   TEXT,
    target_server   TEXT,
    mcts_iterations INTEGER,
    reward          FLOAT,
    state_hash      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- SLA violation log (append-only, used by RCA)
CREATE TABLE sla_violations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vm_id           UUID REFERENCES vms(id),
    violation_type  TEXT NOT NULL,
    duration_ms     FLOAT,
    severity        FLOAT,
    measured_value  FLOAT,
    sla_threshold   FLOAT,
    preceding_decision_id TEXT,   -- FK to scheduling_decisions.decision_id
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- MLflow models cache (denormalised for fast API queries)
CREATE TABLE model_registry_cache (
    model_name      TEXT NOT NULL,
    version         TEXT NOT NULL,
    stage           TEXT NOT NULL,  -- "staging" | "production"
    metrics         JSONB,
    last_trained_at TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (model_name, version)
);
```

---

## 8. ENVIRONMENT VARIABLES — COMPLETE .env SCHEMA

The settings class reads ALL config from environment. Never hardcode connection strings.

```python
# hyperion/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Kafka ────────────────────────────────────────────────────────
    kafka_bootstrap_servers: str = "localhost:9092"   # comma-separated for multi-broker
    kafka_schema_registry_url: str = "http://localhost:8081"  # if using Schema Registry

    # ── PostgreSQL ───────────────────────────────────────────────────
    postgres_host:     str = "localhost"
    postgres_port:     int = 5432
    postgres_db:       str = "hyperion"
    postgres_user:     str = "hyperion"
    postgres_password: str = "hyperion"

    @property
    def postgres_dsn(self) -> str:
        return (f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
                f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}")

    # ── Redis ────────────────────────────────────────────────────────
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db:   int = 0

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # ── MLflow ───────────────────────────────────────────────────────
    mlflow_tracking_uri:    str = "http://localhost:5000"
    mlflow_experiment_name: str = "hyperion"
    mlflow_s3_bucket:       str = "hyperion-mlflow-artifacts"   # used in AWS

    # ── AWS (used in Phase 8; ignored locally) ───────────────────────
    aws_region:         str = "ap-south-1"
    aws_access_key_id:  str = ""
    aws_secret_access_key: str = ""
    s3_data_lake_bucket: str = "hyperion-data-lake"
    s3_prefix_raw:       str = "raw/vm_metrics"
    s3_prefix_features:  str = "features/window_5m"

    # ── Simulation ───────────────────────────────────────────────────
    num_servers:    int = 4
    num_vms:        int = 20
    cores_per_numa: int = 4     # per NUMA node
    numa_per_server: int = 2    # NUMA nodes per server → 8 cores/server

    # ── Scheduler ───────────────────────────────────────────────────
    mcts_iterations:      int   = 500
    mcts_exploration_c:   float = 1.41   # UCT constant
    mcts_rollout_depth:   int   = 50     # K forward steps in MC rollout
    scheduling_interval_s: int  = 30     # how often MCTS runs in steady state
    sla_risk_threshold:   float = 0.7    # XGBoost score above which to trigger MCTS

    # ── Observability ────────────────────────────────────────────────
    prometheus_port: int = 8001   # separate from FastAPI (port 8000)
    log_level:       str = "INFO"

settings = Settings()
```

```bash
# .env.example — copy to .env and fill in secrets

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=hyperion
POSTGRES_USER=hyperion
POSTGRES_PASSWORD=hyperion
REDIS_HOST=localhost
REDIS_PORT=6379
MLFLOW_TRACKING_URI=http://localhost:5000
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_DATA_LAKE_BUCKET=hyperion-data-lake
NUM_SERVERS=4
NUM_VMS=20
MCTS_ITERATIONS=500
MCTS_EXPLORATION_C=1.41
```

---

## 9. REDIS KEY CONVENTIONS

```
features:{vm_id}                    → Hash, latest computed features (TTL: 120s)
metrics:{vm_id}                     → Hash, latest VMMetrics (TTL: 10s)
cluster:state                       → JSON string, latest ClusterState (TTL: 60s)
anomaly:{vm_id}                     → Hash, latest anomaly score (TTL: 60s)
forecast:{vm_id}                    → JSON, 15-min demand forecast (TTL: 300s)
scheduler:lock                      → String, MCTS mutex (TTL: 30s, SET NX)
model:versions                      → Hash, model_name → current_version
```

---

## 10. S3 DATA LAKE PATH CONVENTIONS

```
s3://hyperion-data-lake/
├── raw/vm_metrics/year=YYYY/month=MM/day=DD/hour=HH/part-NNNN.parquet
├── features/window_5m/year=YYYY/month=MM/day=DD/hour=HH/part-NNNN.parquet
├── decisions/year=YYYY/month=MM/day=DD/part-NNNN.parquet
└── reference/                           ← Evidently reference dataset (static)
    ├── vm_metrics_reference.parquet
    └── features_reference.parquet

mlflow-artifacts/                        ← separate bucket for MLflow
├── {experiment_id}/{run_id}/artifacts/
```

**Parquet feature schema** (written by `streaming/processors/feature_writer.py`):
```python
# columns in features/window_5m/
{
    "vm_id":              str,
    "window_end_ts":      float,    # unix epoch
    "cpu_mean_5m":        float,
    "cpu_std_5m":         float,
    "cpu_max_5m":         float,
    "memory_mean_5m":     float,
    "memory_std_5m":      float,
    "memory_max_5m":      float,
    "iops_mean_5m":       float,
    "network_mean_5m":    float,
    "cpu_rate_of_change": float,    # (current - 5min_ago) / 5min_ago
    "burst_count_5m":     int,      # number of >80% CPU spikes in window
}
```

---

## 11. ML MODEL INTERFACES

```python
# ── Anomaly Detection ─────────────────────────────────────────────────────
# Input:  torch.Tensor, shape (batch, seq_len=60, features=4)
# features order: [cpu_pct, memory_pct, iops, network_mbps]
# Output: torch.Tensor, shape (batch, seq_len, features)  ← reconstruction
# Anomaly score = mean reconstruction MSE over the sequence
# Threshold: 99th percentile of reconstruction error on training data (stored in MLflow)

class LSTMAutoencoder(nn.Module):
    # hidden_dim: tuned by Optuna, logged to MLflow
    # seq_len: 60 (60 one-second samples)
    # inference: score = mse_loss(model(x), x).mean(dim=[1,2])

# ── Demand Forecaster ─────────────────────────────────────────────────────
# Input:  last 60 minutes of VMMetrics for a single VM
# Output: dict with keys:
#   "forecast_horizon_min": 15
#   "cpu_mean":   list[float]  (15 values, one per minute)
#   "cpu_lower":  list[float]  (80% CI lower)
#   "cpu_upper":  list[float]  (80% CI upper)
#   "memory_mean": list[float]
# Used by: MCTS rollout (samples from [lower, upper] range), predictive autoscaler

# ── SLA Violation Predictor ───────────────────────────────────────────────
# Input:  numpy array, shape (n_samples, 12)
# Feature order (MUST match this exactly, always):
#   0: cpu_mean_5m          1: cpu_std_5m
#   2: memory_mean_5m       3: memory_std_5m
#   4: cpu_max_5m           5: memory_max_5m
#   6: burst_count_5m       7: cpu_rate_of_change
#   8: time_of_day_sin      9: time_of_day_cos   (cyclic encoding)
#  10: numa_locality_score  11: is_pinned (0/1)
# Output: float in [0, 1] — probability of SLA violation in next 5 min
# Threshold: settings.sla_risk_threshold (default 0.7)

# ── GNN Policy Network ────────────────────────────────────────────────────
# Input:  torch_geometric.data.Data graph
#   Node features (servers): [cpu_util, memory_util, core_count, numa_count, az_onehot×3]
#   Node features (VMs):     [vcpu_count/8, memory_req/64, sla_urgency, workload_onehot×4, is_pinned]
#   Edge types: "hosted_on" (VM→Server), "co_located" (VM↔VM), "same_az" (Server↔Server)
# Output: (value: Tensor[batch, 1], policy_logits: Tensor[batch, num_actions])
# num_actions = num_vms × num_servers + 1 (no-op)
```

---

## 12. MCTS + CSP ALGORITHM INTERFACES

```python
# hyperion/scheduler/state.py

from dataclasses import dataclass

@dataclass(frozen=True)
class SchedulerState:
    """Hashable state for MCTS nodes."""
    assignment: frozenset[tuple[str, str]]  # frozenset of (vm_id, server_id)
    utilization: frozenset[tuple[str, float, float]]  # (server_id, cpu_pct, mem_pct)
    timestamp: float

    def __hash__(self):
        return hash((self.assignment, self.utilization))

@dataclass
class SchedulingAction:
    action_type:      ActionType
    vm_id:            str
    target_server_id: str | None = None
    target_core_id:   str | None = None
    new_quantum_ms:   int | None = None

    def __hash__(self):
        return hash((self.action_type, self.vm_id, self.target_server_id))
```

```python
# hyperion/scheduler/csp.py  — key interfaces

from typing import Protocol

class Constraint(Protocol):
    def is_satisfied(self, assignment: dict[str, str]) -> bool: ...
    def prune(self, vm_id: str, domain: list[str], assignment: dict[str, str]) -> list[str]: ...

class SchedulingCSP:
    def __init__(self, vms: list[VM], servers: list[Server], constraints: list[Constraint]):
        ...
    def get_valid_actions(self, state: SchedulerState) -> list[SchedulingAction]:
        """AC-3 propagation → returns only feasible actions. Called by MCTS expansion."""
        ...
    def is_consistent(self, state: SchedulerState) -> bool:
        """Returns True iff the full assignment satisfies all constraints."""
        ...

# Built-in constraints:
# AntiAffinityConstraint(vm_a_id, vm_b_id)   — must not share server
# MemoryCapConstraint(server_id, max_gb)       — sum of VM memory ≤ cap
# PinExclusivityConstraint()                   — each pinned vCPU owns one core
# NUMALocalityConstraint(vm_id, preferred_node_id)
```

```python
# hyperion/scheduler/mcts.py  — key interfaces

class MCTSNode:
    state:       SchedulerState
    action:      SchedulingAction | None   # action that produced this node
    parent:      MCTSNode | None
    children:    list[MCTSNode]
    visit_count: int       # N(s)
    total_value: float     # W(s)

    @property
    def q_value(self) -> float:
        return self.total_value / self.visit_count if self.visit_count > 0 else 0.0

    def is_fully_expanded(self, valid_actions: list[SchedulingAction]) -> bool:
        return len(self.children) == len(valid_actions)

class MCTSScheduler:
    def __init__(self, csp: SchedulingCSP, hypervisor: HypervisorSimulator,
                 forecaster, policy_network=None):
        # policy_network: None = random rollout; set to GNN in Phase 5
        ...

    def search(self, root_state: SchedulerState,
               iterations: int = None) -> SchedulingAction:
        """Run MCTS and return best action. iterations defaults to settings.mcts_iterations."""
        ...

    # UCT formula:
    # score(node) = Q(node) + C * sqrt(ln(N(parent)) / N(node))
    # C = settings.mcts_exploration_c (default 1.41)

    # Reward function:
    # reward = (sla_compliance_rate * 0.6) + (throughput_normalised * 0.3) - (migration_cost * 0.1)
    # sla_compliance_rate: fraction of VMs meeting their SLA at rollout end
    # throughput_normalised: mean(vm_rps / vm_sla.min_throughput_rps) clamped to [0, 1]
    # migration_cost: number_of_migrations * 0.05 (configurable penalty)
```

---

## 13. FASTAPI ROUTES — COMPLETE CONTRACT

```
Base URL (local): http://localhost:8000
Base URL (AWS):   http://{ec2-public-ip}:8000

GET  /healthz                              → {"status": "ok", "version": str}
GET  /api/v1/cluster/state                 → ClusterState (JSON)
GET  /api/v1/cluster/servers               → list[Server]
GET  /api/v1/cluster/servers/{server_id}   → Server
GET  /api/v1/vms                           → list[VM]
GET  /api/v1/vms/{vm_id}                   → VM
GET  /api/v1/vms/{vm_id}/metrics           → VMMetrics (from Redis)
GET  /api/v1/vms/{vm_id}/forecast          → DemandForecast
GET  /api/v1/vms/{vm_id}/anomaly           → AnomalyScore
POST /api/v1/scheduler/trigger             → {"decision_id": str, "action": SchedulerDecision}
GET  /api/v1/scheduler/decisions?limit=50  → list[SchedulerDecision]
GET  /api/v1/models/health                 → list[ModelHealth]
GET  /metrics                              → Prometheus text (port 8001, not 8000)
WS   /ws/events                            → streams SchedulerDecision JSON on each decision
```

---

## 14. PROMETHEUS METRIC NAMES

```python
# hyperion/observability/metrics.py
# Always import from here — never define metrics in other modules

METRICS = {
    # Gauges
    "hyperion_sla_compliance_rate":   Gauge(..., labelnames=["vm_type"]),
    "hyperion_anomaly_score":         Gauge(..., labelnames=["vm_id"]),
    "hyperion_kafka_consumer_lag":    Gauge(..., labelnames=["topic", "group_id"]),
    "hyperion_model_drift_score":     Gauge(..., labelnames=["model_name"]),
    "hyperion_cluster_cpu_pct":       Gauge(..., labelnames=["server_id"]),
    "hyperion_cluster_memory_pct":    Gauge(..., labelnames=["server_id"]),

    # Counters
    "hyperion_decisions_total":       Counter(..., labelnames=["action_type"]),
    "hyperion_migrations_total":      Counter(...),
    "hyperion_sla_violations_total":  Counter(..., labelnames=["vm_type", "violation_type"]),
    "hyperion_retraining_runs_total": Counter(..., labelnames=["model_name"]),

    # Histograms
    "hyperion_mcts_latency_seconds":  Histogram(..., buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]),
    "hyperion_api_latency_seconds":   Histogram(..., labelnames=["endpoint"]),
    "hyperion_kafka_produce_latency": Histogram(..., labelnames=["topic"]),
}
```

---

## 15. LOGGING PATTERN (STRUCTLOG — ALWAYS USE THIS)

```python
import structlog

logger = structlog.get_logger(__name__)

# Correct usage throughout the project:
logger.info("mcts_search_started",
            vm_count=len(state.vms),
            iterations=iterations,
            state_hash=str(hash(state)))

logger.error("kafka_produce_failed",
             topic=topic,
             error=str(e),
             vm_id=vm_id)

logger.warning("sla_risk_detected",
               vm_id=vm_id,
               score=score,
               threshold=settings.sla_risk_threshold)

# NEVER use:
# print(...)
# import logging; logging.info(...)
# f-string messages in log calls
```

---

## 16. ASYNC PATTERNS

```python
# All DB access is async via SQLAlchemy 2.x
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

engine = create_async_engine(settings.postgres_dsn, echo=False, pool_size=10)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def get_db() -> AsyncSession:  # FastAPI dependency
    async with AsyncSessionLocal() as session:
        yield session

# All Redis access is async
import redis.asyncio as aioredis
redis_client = aioredis.from_url(settings.redis_url, decode_responses=True)

# Kafka consumers run in asyncio tasks, NOT threads
# Faust app handles its own event loop — run as separate process

# ML inference (PyTorch) runs in a ThreadPoolExecutor to avoid blocking the event loop:
from concurrent.futures import ThreadPoolExecutor
_thread_pool = ThreadPoolExecutor(max_workers=4)
result = await asyncio.get_event_loop().run_in_executor(_thread_pool, model_infer, x)
```

---

## 17. MLFLOW CONVENTIONS

```python
# Experiment naming: always "hyperion/{model_type}"
# e.g. "hyperion/lstm_autoencoder", "hyperion/forecaster", "hyperion/gnn_policy"

# Run naming: "{model_type}_v{date}_{optuna_trial_number}"
# e.g. "lstm_autoencoder_v20250515_t042"

# Always log these at minimum:
mlflow.log_param("model_type", ...)
mlflow.log_param("phase", 3)           # which project phase produced this
mlflow.log_param("optuna_trial", ...)
mlflow.log_metric("val_loss", ...)
mlflow.log_metric("train_loss", ...)
mlflow.log_metric("threshold", ...)    # anomaly threshold if applicable
mlflow.set_tag("status", "production") # after promotion

# Model registration names (canonical, never change these):
MLFLOW_MODEL_ANOMALY     = "hyperion-anomaly-detector"
MLFLOW_MODEL_FORECASTER  = "hyperion-forecaster"
MLFLOW_MODEL_SLA_PRED    = "hyperion-sla-predictor"
MLFLOW_MODEL_GNN_POLICY  = "hyperion-gnn-policy"
```

---

## 18. AIRFLOW DAG CONVENTIONS

```python
# File naming: {purpose}_dag.py in /dags/
# DAG ID naming: "hyperion_{purpose}" e.g. "hyperion_retrain_forecaster"
# All DAGs use: schedule_interval for cron, catchup=False, tags=["hyperion"]

# Standard DAG template:
from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(
    dag_id="hyperion_retrain_forecaster",
    schedule_interval="0 3 * * *",    # 3am daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hyperion", "ml", "retraining"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
)
def retrain_forecaster_dag():
    @task
    def load_data(): ...
    @task
    def train(data): ...
    @task
    def evaluate(model, data): ...
    @task
    def promote(model, metrics): ...
    promote(evaluate(train(load_data())))
```

---

## 19. ERROR HANDLING PATTERN

```python
# Use custom exception hierarchy — never raise bare Exception
# hyperion/exceptions.py

class HyperionError(Exception): pass
class SchedulerError(HyperionError): pass
class CSPInfeasibleError(SchedulerError): pass      # no valid action exists
class MCTSTimeoutError(SchedulerError): pass         # exceeded time budget
class KafkaProduceError(HyperionError): pass
class ModelNotLoadedError(HyperionError): pass
class SLAViolationError(HyperionError): pass

# In API routes: let FastAPI exception handlers convert to HTTP responses
# In Kafka consumers: log + continue (never crash the consumer loop)
# In MCTS: if CSPInfeasibleError → fall back to NO_OP action + alert
```

---

## 20. TESTING CONVENTIONS

```python
# tests/unit/          — pure unit tests, no I/O, fast
# tests/integration/   — requires docker-compose services running

# Fixtures:
# conftest.py provides: mock_cluster_state, mock_vm, mock_server, mock_metrics
# integration/conftest.py: async_db_session, kafka_producer, redis_client

# Test file naming: test_{module_name}.py
# Test function naming: test_{function_name}_{scenario}
# e.g. test_csp_prune_removes_memory_overloaded_servers

# Always use pytest-asyncio for async tests:
@pytest.mark.asyncio
async def test_mcts_returns_valid_action(mock_cluster_state):
    ...

# Minimum coverage targets:
# scheduler/csp.py:  90%
# scheduler/mcts.py: 85%
# simulation/:       80%
# api/routes/:       75%
```

---

## 21. LOCAL SERVICE ENDPOINTS (docker-compose)

```
Kafka broker:          localhost:9092
Kafka (internal):      kafka:29092              ← use inside docker network
Zookeeper:             localhost:2181
Schema Registry:       localhost:8081
Kafka UI:              localhost:8080            ← web UI for topic inspection
PostgreSQL:            localhost:5432  db=hyperion  user=hyperion  pass=hyperion
Redis:                 localhost:6379
MLflow tracking:       localhost:5000
Prometheus:            localhost:9090
Grafana:               localhost:3000  admin/hyperion
Airflow webserver:     localhost:8082
Flower (Kafka UI):     localhost:5555
```

---

## 22. AWS ↔ DOCKER LOCAL MAPPING

| AWS Service | docker-compose equivalent | Notes |
|---|---|---|
| MSK (Kafka) | `confluentinc/cp-kafka:7.5.0` | Same client config, different bootstrap URL |
| RDS PostgreSQL | `postgres:16-alpine` | Same schema, same SQLAlchemy DSN format |
| ElastiCache Redis | `redis:7-alpine` | Same client, no cluster mode locally |
| S3 | MinIO (`minio/minio`) OR direct AWS | Use `boto3` with endpoint_url for MinIO |
| CloudWatch | Simulated producer (`streaming/producers/vm_metrics.py`) | In Phase 8, replaced by Kafka Connect |
| ECS containers | `docker-compose up` | Same Docker images |

---

## 23. SIMULATION PARAMETERS (defaults, all configurable via .env)

```python
# Workload generator defaults
BASE_DEMAND_DISTRIBUTION = {
    WorkloadType.CPU_INTENSIVE:    {"means": [0.3, 0.6, 0.85], "stds": [0.05, 0.08, 0.1], "weights": [0.5, 0.35, 0.15]},
    WorkloadType.MEMORY_INTENSIVE: {"means": [0.2, 0.5, 0.8],  "stds": [0.05, 0.1, 0.15],  "weights": [0.6, 0.3, 0.1]},
    WorkloadType.LATENCY_SENSITIVE:{"means": [0.4, 0.7, 0.9],  "stds": [0.03, 0.05, 0.08], "weights": [0.7, 0.2, 0.1]},
    WorkloadType.BATCH:            {"means": [0.1, 0.4, 0.9],  "stds": [0.05, 0.15, 0.1],  "weights": [0.4, 0.4, 0.2]},
}
# Burst events: Poisson(λ=0.1 per minute), magnitude 2–4× base demand, duration 30–300s

# Hypervisor cost model
TIME_SLICE_OVERHEAD_PCT = 0.02    # 2% CPU overhead per vCPU sharing a core
CROSS_NUMA_PENALTY      = 1.4     # 40% latency increase for cross-NUMA memory
MIGRATION_THROUGHPUT_DIP = 0.3    # 30% throughput loss during migration
MIGRATION_DURATION_S     = {"formula": "vm_memory_gb * 2.5", "min": 5, "max": 60}
# e.g. a 4GB VM takes ~10s to migrate
```

---

## 24. PHASE COMPLETION CHECKLIST

Update the checkboxes as each phase is finished.

### Phase 0 — Environment Setup
- [ ] `docker/docker-compose.yml` with all 8 services
- [ ] `pyproject.toml` with all dependencies + tool configs
- [ ] `hyperion/config.py` (Settings)
- [ ] `.env.example` committed
- [ ] `.pre-commit-config.yaml` (ruff, mypy, trailing-whitespace)
- [ ] `.github/workflows/ci.yml` (lint + test on PR)
- [ ] `prometheus/prometheus.yml` scaffold
- [ ] Repository directory skeleton created with `__init__.py` files
- [ ] `docker-compose up` brings up all services cleanly

### Phase 1 — Physical Model
- [ ] `hyperion/simulation/models.py` all dataclasses
- [ ] `hyperion/simulation/hypervisor.py` pin / migrate / reslice
- [ ] `hyperion/simulation/workload.py` Gaussian mixture generator
- [ ] `hyperion/scheduler/baselines.py` random + FFD + round-robin
- [ ] Unit tests ≥ 80% coverage on simulation/
- [ ] `make simulate` CLI command runs a 100-VM simulation for 60s

### Phase 2 — Kafka Streaming
- [ ] `hyperion/streaming/topics.py` all schemas
- [ ] `hyperion/streaming/producers/vm_metrics.py`
- [ ] `hyperion/streaming/processors/windowing.py` (Faust)
- [ ] `hyperion/streaming/processors/feature_writer.py` (Redis + S3/MinIO)
- [ ] Kafka UI shows messages arriving in vm.metrics topic
- [ ] Redis HGETALL features:{vm_id} returns computed window features

### Phase 3 — ML Models
- [ ] LSTM Autoencoder trains and reaches val_loss < 0.01
- [ ] Isolation Forest fits and produces per-VM scores
- [ ] Prophet + LSTM forecaster produces 15-min confidence intervals
- [ ] XGBoost SLA predictor AUC-ROC > 0.85 on synthetic data
- [ ] All 4 models registered in MLflow model registry
- [ ] Optuna study for each model (≥ 20 trials)

### Phase 4 — MCTS + CSP
- [ ] AC-3 prunes invalid actions; CSP tests pass
- [ ] MCTS produces valid SchedulingAction in < 2s for 20-VM cluster
- [ ] Kafka consumer triggers MCTS on SLA risk event
- [ ] End-to-end: metrics → Kafka → MCTS → decision → hypervisor → state update

### Phase 5 — GNN Policy
- [ ] Graph construction from ClusterState works
- [ ] GAT network forward pass produces (value, policy_logits)
- [ ] Self-play training loop runs 100 episodes without error
- [ ] GNN rollout outperforms random rollout by ≥ 5% SLA compliance

### Phase 6 — AIOps Loop
- [ ] RCA links SLA violation to preceding scheduling decision
- [ ] Predictive autoscaling proactively triggers MCTS on forecast breach
- [ ] Evidently drift detection fires when feature distribution shifts
- [ ] Airflow DAG retrain_forecaster_dag runs end-to-end

### Phase 7 — Observability + API
- [ ] All Prometheus metrics visible at localhost:9090
- [ ] Grafana dashboard shows 4 panels with live data
- [ ] FastAPI /api/v1/cluster/state returns real cluster state
- [ ] WebSocket /ws/events streams SchedulerDecision on each decision
- [ ] API latency < 50ms P99 under load

### Phase 8 — AWS + Experiments
- [ ] Terraform applies cleanly (`terraform apply`)
- [ ] CloudWatch → Kafka Connect → vm.metrics pipeline works on AWS
- [ ] Benchmark suite runs 5 algorithms across 3 burstiness levels
- [ ] Results CSV exported to S3
- [ ] README and demo screencast complete

---

*This document is the single source of truth for copilot context. When you complete a phase, tick the checkboxes and update the phase status block at the top of Section 1.*
