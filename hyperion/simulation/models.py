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
    vcpu_count:       int = Field(gt=0)
    memory_req_gb:    float = Field(gt=0.0)
    workload_type:    WorkloadType
    sla:              SLAProfile
    pinning_required: bool = False        # if True, each vCPU must have a dedicated core
    current_server_id: str | None = None  # None = unscheduled

    cpu_quantum_ms: int = Field(default=10, ge=1, le=100)


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