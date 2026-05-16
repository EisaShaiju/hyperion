import pytest
from pydantic import ValidationError
from hyperion.simulation.models import Core, NUMANode, Server, VM, WorkloadType, SLAProfile, ClusterState
from hyperion.simulation.hypervisor import HypervisorSimulator
from hyperion.exceptions import InsufficientResourcesError, CoreAlreadyPinnedError
import time

def test_pydantic_validation_blocks_bad_data():
    """Proves that our models reject physically impossible hardware states."""
    # Memory cannot be negative
    with pytest.raises(ValidationError):
        VM(name="bad-vm", vcpu_count=2, memory_req_gb=-5.0, 
           workload_type=WorkloadType.BATCH, 
           sla=SLAProfile(max_p99_latency_ms=100, min_throughput_rps=10, max_memory_gb=8, priority=1))

def test_hypervisor_migration_cost_and_logic():
    """Proves the hypervisor correctly moves VMs and calculates time penalties."""
    # 1. Setup a tiny cluster
    sla = SLAProfile(max_p99_latency_ms=100, min_throughput_rps=10, max_memory_gb=8, priority=1)
    vm = VM(id="vm-test", name="test", vcpu_count=2, memory_req_gb=4.0, workload_type=WorkloadType.BATCH, sla=sla)
    
    node1 = NUMANode(id=0, cores=[], local_memory_gb=10.0)
    server1 = Server(id="server-1", aws_instance_type="t3.micro", availability_zone="us-east-1a", numa_nodes=[node1], total_memory_gb=10.0)
    
    state = ClusterState(servers={server1.id: server1}, vms={vm.id: vm}, metrics={}, timestamp=time.time())
    hypervisor = HypervisorSimulator(state)

    # 2. Execute Migration
    cost = hypervisor.migrate_vm(vm.id, server1.id)

    # 3. Verify Physics
    assert vm.current_server_id == "server-1"
    assert server1.allocated_memory_gb == 4.0
    assert vm.id in server1.hosted_vm_ids
    # Cost should be memory * 2.5 (clamped to min 5.0) => 4.0 * 2.5 = 10.0 seconds
    assert cost == 10.0

def test_hypervisor_blocks_overloaded_server():
    """Proves the hypervisor throws an error if a server is out of RAM."""
    sla = SLAProfile(max_p99_latency_ms=100, min_throughput_rps=10, max_memory_gb=8, priority=1)
    
    # VM needs 16GB, Server only has 10GB
    vm = VM(id="huge-vm", name="test", vcpu_count=8, memory_req_gb=16.0, workload_type=WorkloadType.BATCH, sla=sla)
    node1 = NUMANode(id=0, cores=[], local_memory_gb=10.0)
    server1 = Server(id="tiny-server", aws_instance_type="t3.micro", availability_zone="us-east-1a", numa_nodes=[node1], total_memory_gb=10.0)
    
    state = ClusterState(servers={server1.id: server1}, vms={vm.id: vm}, metrics={}, timestamp=time.time())
    hypervisor = HypervisorSimulator(state)

    with pytest.raises(InsufficientResourcesError):
        hypervisor.migrate_vm(vm.id, server1.id)