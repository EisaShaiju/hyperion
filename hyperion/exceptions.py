# hyperion/exceptions.py

from __future__ import annotations

# ── Base ─────────────────────────────────────────────────────────────────────

class HyperionError(Exception):
    """Root exception for all Hyperion errors."""

# ── Hypervisor ───────────────────────────────────────────────────────────────

class HypervisorError(HyperionError):
    """Raised by HypervisorSimulator for invalid operations."""

class InsufficientResourcesError(HypervisorError):
    """Target server lacks memory or cores to satisfy the request."""

class CoreAlreadyPinnedError(HypervisorError):
    """Requested physical core is already exclusively assigned to another VM."""

class VMNotFoundError(HypervisorError):
    """VM ID does not exist in the cluster state."""

class ServerNotFoundError(HypervisorError):
    """Server ID does not exist in the cluster state."""

class VMAlreadyMigratingError(HypervisorError):
    """VM is currently undergoing a live migration; concurrent migration is not allowed."""

class InvalidActionError(HypervisorError):
    """The requested scheduling action is semantically invalid (e.g. pin an already-pinned VM)."""

# ── Scheduler ────────────────────────────────────────────────────────────────

class SchedulerError(HyperionError):
    """Raised by MCTS / CSP scheduling components."""

class CSPInfeasibleError(SchedulerError):
    """No valid scheduling action exists that satisfies all CSP constraints."""

class MCTSTimeoutError(SchedulerError):
    """MCTS search exceeded its time or iteration budget without converging."""

# ── Streaming / Kafka ────────────────────────────────────────────────────────

class StreamingError(HyperionError):
    """Raised by Kafka producers or consumers."""

class KafkaProduceError(StreamingError):
    """Failed to deliver a message to a Kafka topic."""

class KafkaConsumeError(StreamingError):
    """Failed to read or deserialise a Kafka message."""

# ── ML / Models ──────────────────────────────────────────────────────────────

class MLError(HyperionError):
    """Raised by ML model components."""

class ModelNotLoadedError(MLError):
    """Attempted inference on a model that has not been loaded or registered."""

class ModelStalenessError(MLError):
    """Model is too old relative to the current data distribution (drift detected)."""

# ── AIOps ────────────────────────────────────────────────────────────────────

class AIOpsError(HyperionError):
    """Raised by AIOps feedback loop components."""

class RCAError(AIOpsError):
    """Root cause analysis could not be performed."""