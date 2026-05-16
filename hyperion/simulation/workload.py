# hyperion/simulation/workload.py

import numpy as np
import structlog
from hyperion.simulation.models import VM, WorkloadType

logger = structlog.get_logger(__name__)

class WorkloadGenerator:
    """
    Generates realistic, chaotic CPU and memory demand for VMs using 
    Gaussian mixtures and Poisson-distributed burst events.
    """
    def __init__(self):
        # Base demand distribution from Phase 1 specifications
        self.BASE_DEMAND = {
            WorkloadType.CPU_INTENSIVE:    {"means": [0.3, 0.6, 0.85], "stds": [0.05, 0.08, 0.1], "weights": [0.5, 0.35, 0.15]},
            WorkloadType.MEMORY_INTENSIVE: {"means": [0.2, 0.5, 0.8],  "stds": [0.05, 0.1, 0.15],  "weights": [0.6, 0.3, 0.1]},
            WorkloadType.LATENCY_SENSITIVE:{"means": [0.4, 0.7, 0.9],  "stds": [0.03, 0.05, 0.08], "weights": [0.7, 0.2, 0.1]},
            WorkloadType.BATCH:            {"means": [0.1, 0.4, 0.9],  "stds": [0.05, 0.15, 0.1],  "weights": [0.4, 0.4, 0.2]},
        }
        
        # Tracks currently active spikes: {vm_id: {"end_time": float, "multiplier": float}}
        self.active_bursts = {}

    def _sample_gaussian_mixture(self, workload_type: WorkloadType) -> float:
        """Samples a base CPU utilization (0.0 to 1.0) from the VM's specific mixture model."""
        params = self.BASE_DEMAND[workload_type]
        
        # 1. Choose which "mode" the VM is currently in based on the weights
        component_idx = np.random.choice(len(params["weights"]), p=params["weights"])
        mean = params["means"][component_idx]
        std = params["stds"][component_idx]

        # 2. Sample the actual CPU usage from that specific bell curve
        sample = np.random.normal(mean, std)
        
        # 3. Clamp to valid percentages [0.0, 1.0]
        return max(0.0, min(1.0, sample))

    def generate_cpu_demand(self, vm: VM, current_time_s: float) -> float:
        """
        Calculates the current CPU demand percentage (0 to 100) for a VM, 
        factoring in base demand and random Poisson bursts.
        """
        base_cpu = self._sample_gaussian_mixture(vm.workload_type)
        burst_multiplier = 1.0

        # --- Poisson Burst Logic ---
        # If the VM is already experiencing a traffic spike, keep applying it
        if vm.id in self.active_bursts:
            if current_time_s < self.active_bursts[vm.id]["end_time"]:
                burst_multiplier = self.active_bursts[vm.id]["multiplier"]
            else:
                # The spike has finished, remove it
                del self.active_bursts[vm.id] 
        else:
            # If not in a spike, roll the dice to see if one starts right now.
            # Lambda = 0.1 per minute -> roughly 0.0016 probability per second
            if np.random.random() < (0.1 / 60):
                # Calculate spike severity (2x to 4x) and duration (30s to 300s)
                duration = np.random.uniform(30.0, 300.0)
                burst_multiplier = np.random.uniform(2.0, 4.0)
                
                self.active_bursts[vm.id] = {
                    "end_time": current_time_s + duration,
                    "multiplier": burst_multiplier
                }
                logger.info("burst_started", 
                            vm_id=vm.id, 
                            duration_seconds=round(duration, 1), 
                            multiplier=round(burst_multiplier, 2))

        # Apply the spike multiplier and return as a percentage (e.g., 85.5%)
        final_cpu = base_cpu * burst_multiplier
        return max(0.0, min(100.0, final_cpu * 100.0))