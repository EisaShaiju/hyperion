# run_sim.py

import time
import structlog
from hyperion.simulation.models import ClusterState, Server, NUMANode, Core, VM, WorkloadType, SLAProfile
from hyperion.simulation.hypervisor import HypervisorSimulator
from hyperion.simulation.workload import WorkloadGenerator
from hyperion.scheduler.baselines import FFDScheduler

logger = structlog.get_logger()

def setup_mock_datacenter() -> ClusterState:
    """Builds a tiny fake data center with 2 Servers and 3 VMs."""
    # Build 2 Servers (32GB RAM each)
    servers = {}
    for i in range(2):
        server_id = f"server-{i}"
        node = NUMANode(id=0, cores=[Core(id=f"core-{c}", physical_id=c, numa_node_id=0) for c in range(4)], local_memory_gb=32.0)
        servers[server_id] = Server(id=server_id, aws_instance_type="c5.xlarge", availability_zone="us-east-1a", numa_nodes=[node], total_memory_gb=32.0)

    # Build 3 VMs
    vms = {}
    for i in range(3):
        vm_id = f"vm-{i}"
        sla = SLAProfile(max_p99_latency_ms=100.0, min_throughput_rps=500.0, max_memory_gb=8.0, priority=3)
        vms[vm_id] = VM(id=vm_id, name=f"web-server-{i}", vcpu_count=2, memory_req_gb=4.0, workload_type=WorkloadType.CPU_INTENSIVE, sla=sla)

    return ClusterState(servers=servers, vms=vms, metrics={}, timestamp=time.time())


def main():
    logger.info("booting_simulation_engine")
    
    # 1. Initialize our components
    state = setup_mock_datacenter()
    hypervisor = HypervisorSimulator(state)
    workload_gen = WorkloadGenerator()
    scheduler = FFDScheduler()

    # 2. Initial Placement: Use the FFD Scheduler to place the 3 VMs
    for vm in state.vms.values():
        target_server = scheduler.schedule(state, vm)
        if target_server:
            hypervisor.migrate_vm(vm.id, target_server)
        else:
            logger.error("scheduling_failed", vm_id=vm.id)

    logger.info("initial_placement_complete", state=state.model_dump_json(indent=2))

    # 3. Run the "Time Loop" for 5 simulated seconds
    logger.info("starting_traffic_simulation")
    for tick in range(1, 6):
        current_time = time.time() + tick
        
        for vm in state.vms.values():
            # Generate chaotic CPU spikes for each VM
            cpu_usage = workload_gen.generate_cpu_demand(vm, current_time)
            logger.info("vm_telemetry", tick=tick, vm_id=vm.id, cpu_pct=round(cpu_usage, 2))
            
        time.sleep(1) # Wait 1 real second so you can watch it run

if __name__ == "__main__":
    main()