"""
Planner module - Computes upgrade plans from specs
"""
import logging
from kubernetes import client

logger = logging.getLogger(__name__)


def make_plan(spec):
    """
    Create an upgrade plan from the spec
    Returns dict with:
      - control_plane_nodes: list of control plane node names
      - worker_nodes: list of worker node names
      - total: total number of nodes
    """
    v1 = client.CoreV1Api()
    
    try:
        nodes = v1.list_node().items
    except Exception as e:
        logger.error(f"Failed to list nodes: {e}")
        raise
    
    control_plane = []
    workers = []
    
    for node in nodes:
        labels = node.metadata.labels or {}
        node_name = node.metadata.name
        
        # Check if control-plane node
        is_control_plane = (
            labels.get("node-role.kubernetes.io/control-plane") is not None or
            labels.get("node-role.kubernetes.io/master") is not None
        )
        
        # Apply node selector filter if specified
        node_selector = spec.get('nodeSelector', {})
        if node_selector:
            matches = all(labels.get(k) == v for k, v in node_selector.items())
            if not matches:
                logger.debug(f"Node {node_name} excluded by nodeSelector")
                continue
        
        if is_control_plane:
            control_plane.append(node_name)
        else:
            workers.append(node_name)
    
    # Handle canary nodes if specified
    canary_config = spec.get('canary', {})
    if not canary_config.get('enabled', False):
        plan = {
            'control_plane_nodes': control_plane,
            'worker_nodes': workers,
            'total': len(control_plane) + len(workers)
        }
        logger.info(f"Plan: {len(control_plane)} control-plane, {len(workers)} workers")
        return plan
    
    canary_nodes = canary_config.get('nodes', [])
    if not canary_nodes:
        plan = {
            'control_plane_nodes': control_plane,
            'worker_nodes': workers,
            'total': len(control_plane) + len(workers)
        }
        logger.info(f"Plan: {len(control_plane)} control-plane, {len(workers)} workers")
        return plan
    
    # Move canary nodes to front of worker list
    canary_in_workers = [n for n in canary_nodes if n in workers]
    non_canary_workers = [n for n in workers if n not in canary_nodes]
    workers = canary_in_workers + non_canary_workers
    logger.info(f"Canary enabled with {len(canary_in_workers)} canary nodes")
    
    plan = {
        'control_plane_nodes': control_plane,
        'worker_nodes': workers,
        'total': len(control_plane) + len(workers)
    }
    
    logger.info(f"Plan: {len(control_plane)} control-plane, {len(workers)} workers")
    return plan


def get_node_info(node_name):
    """Get detailed information about a specific node"""
    v1 = client.CoreV1Api()
    
    try:
        node = v1.read_node(node_name)
        return {
            'name': node.metadata.name,
            'labels': node.metadata.labels,
            'annotations': node.metadata.annotations,
            'status': node.status.conditions,
            'kubelet_version': node.status.node_info.kubelet_version,
            'os_image': node.status.node_info.os_image,
            'container_runtime': node.status.node_info.container_runtime_version
        }
    except Exception as e:
        logger.error(f"Failed to get node info for {node_name}: {e}")
        return None
