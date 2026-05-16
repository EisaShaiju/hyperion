import structlog
from hyperion.simulation.models import ClusterState, Server, VM
from hyperion.exceptions import (
    VMNotFoundError, ServerNotFoundError, InsufficientResourcesError,
    CoreAlreadyPinnedError, InvalidActionError
)

logger = structlog.get_logger(__name__)

class HypervisorSimulator:
    """
    The physics engine of the data center. Executes scheduling actions 
    while enforcing hardware constraints and cost models.
    """
    def __init__(self, state: ClusterState):
        self.state = state
        
        # Hypervisor Cost Models (from Phase 1 specs)
        self.TIME_SLICE_OVERHEAD_PCT = 0.02
        self.CROSS_NUMA_PENALTY = 1.4
        self.MIGRATION_THROUGHPUT_DIP = 0.3

    def migrate_vm(self, vm_id: str, target_server_id: str) -> float:
        """
        Moves a VM to a new server. 
        Returns the duration of the migration in seconds.
        """
        vm = self.state.vms.get(vm_id)
        if not vm:
            raise VMNotFoundError(f"VM {vm_id} not found.")

        target_server = self.state.servers.get(target_server_id)
        if not target_server:
            raise ServerNotFoundError(f"Server {target_server_id} not found.")

        # 1. Physics Check: Does it fit?
        if target_server.available_memory_gb < vm.memory_req_gb:
            raise InsufficientResourcesError(
                f"Server {target_server_id} lacks memory. "
                f"Has {target_server.available_memory_gb}GB, needs {vm.memory_req_gb}GB."
            )

        source_server_id = vm.current_server_id

        # 2. Execute Migration
        if source_server_id:
            source_server = self.state.servers[source_server_id]
            source_server.allocated_memory_gb -= vm.memory_req_gb
            if vm_id in source_server.hosted_vm_ids:
                source_server.hosted_vm_ids.remove(vm_id)

        target_server.allocated_memory_gb += vm.memory_req_gb
        target_server.hosted_vm_ids.append(vm_id)
        vm.current_server_id = target_server_id

        # 3. Calculate Cost (Migration Formula: memory * 2.5, clamped 5s to 60s)
        duration_s = max(5.0, min(60.0, vm.memory_req_gb * 2.5))

        logger.info("vm_migrated", 
                    vm_id=vm_id, 
                    source=source_server_id, 
                    target=target_server_id, 
                    cost_seconds=duration_s)
        
        return duration_s

    def pin_vcpu(self, vm_id: str, server_id: str, core_id: str) -> None:
        """Locks a VM to a specific physical core for maximum performance."""
        vm = self.state.vms.get(vm_id)
        server = self.state.servers.get(server_id)

        if not vm or not server:
            raise InvalidActionError("VM or Server not found for pinning.")

        if vm.current_server_id != server_id:
            raise InvalidActionError("Cannot pin VM to a core on a server it doesn't live on.")

        # Find the specific core across all NUMA nodes
        target_core = None
        for node in server.numa_nodes:
            for core in node.cores:
                if core.id == core_id:
                    target_core = core
                    break

        if not target_core:
            raise InvalidActionError(f"Core {core_id} not found.")

        # Physics Check: Is the core already taken?
        if target_core.is_pinned and target_core.pinned_vm_id != vm_id:
            raise CoreAlreadyPinnedError(f"Core {core_id} already in use by another VM.")

        target_core.is_pinned = True
        target_core.pinned_vm_id = vm_id
        
        logger.info("vcpu_pinned", vm_id=vm_id, core_id=core_id)

    def reslice(self, vm_id: str, new_quantum_ms: int) -> None:
            """Adjusts the CPU time-slice quantum for a VM."""
            vm = self.state.vms.get(vm_id)
            if not vm:
                raise VMNotFoundError(f"VM {vm_id} not found.")

            # 1. Physics Check: Ensure the quantum is within physical bounds
            if new_quantum_ms < 1 or new_quantum_ms > 100:
                raise InvalidActionError("Quantum must be between 1ms and 100ms.")

            # 2. Mutate the State
            old_quantum = vm.cpu_quantum_ms
            vm.cpu_quantum_ms = new_quantum_ms

            logger.info("quantum_resliced", 
                        vm_id=vm_id, 
                        old_quantum=old_quantum, 
                        new_quantum=new_quantum_ms)