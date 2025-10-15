"""
Preflight checks module - Validates cluster state before upgrade
"""
import logging
from kubernetes import client

logger = logging.getLogger(__name__)


def run_preflight_checks(spec, plan):
    """
    Run preflight checks before starting upgrade
    Returns dict with:
      - passed: boolean indicating if all checks passed
      - checks: list of check results
    """
    checks = []
    all_passed = True
    
    # Check 1: API server reachability
    check_result = check_api_server()
    checks.append(check_result)
    if not check_result['passed']:
        all_passed = False
    
    # Check 2: Node readiness
    check_result = check_nodes_ready(plan)
    checks.append(check_result)
    if not check_result['passed']:
        all_passed = False
    
    # Check 3: Disk space on nodes
    check_result = check_disk_space(plan)
    checks.append(check_result)
    if not check_result['passed']:
        all_passed = False
    
    # Check 4: PodDisruptionBudgets
    check_result = check_pdbs()
    checks.append(check_result)
    if not check_result['passed']:
        all_passed = False
    
    # Check 5: Air-gapped bundle (if enabled)
    if spec.get('airGapped', {}).get('enabled', False):
        check_result = check_airgap_bundle(spec)
        checks.append(check_result)
        if not check_result['passed']:
            all_passed = False
    
    return {
        'passed': all_passed,
        'checks': checks
    }


def check_api_server():
    """Check if API server is reachable"""
    try:
        v1 = client.CoreV1Api()
        # Simple API call to verify connectivity
        v1.get_api_resources()
        
        return {
            'name': 'API Server Connectivity',
            'passed': True,
            'message': 'API server is reachable'
        }
    except Exception as e:
        logger.error(f"API server check failed: {e}")
        return {
            'name': 'API Server Connectivity',
            'passed': False,
            'message': f'API server unreachable: {str(e)}'
        }


def check_nodes_ready(plan):
    """Check if all nodes are in Ready state"""
    v1 = client.CoreV1Api()
    all_nodes = plan['control_plane_nodes'] + plan['worker_nodes']
    not_ready = []
    
    try:
        for node_name in all_nodes:
            node = v1.read_node(node_name)
            
            # Check node conditions
            is_ready = False
            for condition in node.status.conditions:
                if condition.type == 'Ready' and condition.status == 'True':
                    is_ready = True
                    break
            
            if not is_ready:
                not_ready.append(node_name)
        
        if not_ready:
            return {
                'name': 'Node Readiness',
                'passed': False,
                'message': f'Nodes not ready: {", ".join(not_ready)}'
            }
        
        return {
            'name': 'Node Readiness',
            'passed': True,
            'message': f'All {len(all_nodes)} nodes are ready'
        }
    
    except Exception as e:
        logger.error(f"Node readiness check failed: {e}")
        return {
            'name': 'Node Readiness',
            'passed': False,
            'message': f'Failed to check node readiness: {str(e)}'
        }


def check_disk_space(plan):
    """Check disk space on nodes (basic check via node status)"""
    v1 = client.CoreV1Api()
    all_nodes = plan['control_plane_nodes'] + plan['worker_nodes']
    low_disk = []
    
    try:
        for node_name in all_nodes:
            node = v1.read_node(node_name)
            
            # Check for DiskPressure condition
            for condition in node.status.conditions:
                if condition.type == 'DiskPressure' and condition.status == 'True':
                    low_disk.append(node_name)
                    break
        
        if low_disk:
            return {
                'name': 'Disk Space',
                'passed': False,
                'message': f'Nodes with disk pressure: {", ".join(low_disk)}'
            }
        
        return {
            'name': 'Disk Space',
            'passed': True,
            'message': f'All nodes have sufficient disk space'
        }
    
    except Exception as e:
        logger.error(f"Disk space check failed: {e}")
        return {
            'name': 'Disk Space',
            'passed': False,
            'message': f'Failed to check disk space: {str(e)}'
        }


def check_pdbs():
    """Check PodDisruptionBudgets to identify potential drain issues"""
    policy_v1 = client.PolicyV1Api()
    
    try:
        pdbs = policy_v1.list_pod_disruption_budget_for_all_namespaces()
        
        restrictive_pdbs = []
        for pdb in pdbs.items:
            # Check if PDB is very restrictive
            if pdb.status.disruptions_allowed == 0:
                restrictive_pdbs.append(f"{pdb.metadata.namespace}/{pdb.metadata.name}")
        
        if not restrictive_pdbs:
            return {
                'name': 'PodDisruptionBudgets',
                'passed': True,
                'message': 'PDBs are not overly restrictive'
            }
        
        return {
            'name': 'PodDisruptionBudgets',
            'passed': True,  # Warning, not a failure
            'message': f'Warning: {len(restrictive_pdbs)} PDBs with 0 disruptions allowed: {", ".join(restrictive_pdbs[:3])}'
        }
    
    except Exception as e:
        logger.warning(f"PDB check failed (non-critical): {e}")
        return {
            'name': 'PodDisruptionBudgets',
            'passed': True,
            'message': 'Could not check PDBs (non-critical)'
        }


def check_airgap_bundle(spec):
    """Check if air-gapped bundle ConfigMap exists"""
    v1 = client.CoreV1Api()
    bundle_cm = spec.get('airGapped', {}).get('bundleConfigMap')
    
    if not bundle_cm:
        return {
            'name': 'Air-Gap Bundle',
            'passed': False,
            'message': 'Air-gap enabled but no bundleConfigMap specified'
        }
    
    try:
        # Check if ConfigMap exists
        namespace = 'kube-system'
        v1.read_namespaced_config_map(bundle_cm, namespace)
        
        return {
            'name': 'Air-Gap Bundle',
            'passed': True,
            'message': f'Air-gap bundle ConfigMap {bundle_cm} exists'
        }
    
    except client.exceptions.ApiException as e:
        if e.status == 404:
            return {
                'name': 'Air-Gap Bundle',
                'passed': False,
                'message': f'Air-gap bundle ConfigMap {bundle_cm} not found'
            }
        
        logger.error(f"Air-gap bundle check failed: {e}")
        return {
            'name': 'Air-Gap Bundle',
            'passed': False,
            'message': f'Failed to check air-gap bundle: {str(e)}'
        }
