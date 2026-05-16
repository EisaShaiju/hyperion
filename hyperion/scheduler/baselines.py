import random
from typing import Optional
from hyperion.simulation.models import ClusterState, VM

class RandomScheduler:
    """A dumb scheduler that picks a random server that has enough memory."""
    def schedule(self, state: ClusterState, vm: VM) -> Optional[str]:
        valid_servers = [
            s for s in state.servers.values() 
            if s.available_memory_gb >= vm.memory_req_gb
        ]
        
        if not valid_servers:
            return None # No server has enough space!
            
        return random.choice(valid_servers).id

class RoundRobinScheduler:
    """
    Distributes VMs evenly across all servers in a rotating order, 
    regardless of how full they are (as long as they physically fit).
    """
    def __init__(self):
        self._last_index = -1

    def schedule(self, state: ClusterState, vm: VM) -> Optional[str]:
        servers = list(state.servers.values())
        if not servers:
            return None

        # Try every server once, starting from the next one in the rotation
        for _ in range(len(servers)):
            self._last_index = (self._last_index + 1) % len(servers)
            target = servers[self._last_index]
            
            if target.available_memory_gb >= vm.memory_req_gb:
                return target.id
                
        return None

        
class FFDScheduler:
    """
    First-Fit Decreasing: Sorts servers by available memory (descending) 
    and picks the first one that fits. This packs servers tightly.
    """
    def schedule(self, state: ClusterState, vm: VM) -> Optional[str]:
        # Sort servers from most empty to most full
        sorted_servers = sorted(
            state.servers.values(), 
            key=lambda s: s.available_memory_gb, 
            reverse=True
        )
        
        for server in sorted_servers:
            if server.available_memory_gb >= vm.memory_req_gb:
                return server.id
                
        return None