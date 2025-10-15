"""
State management module - Handles CR status updates
"""
import logging
from kubernetes import client
import copy

logger = logging.getLogger(__name__)


def patch_status(group, version, plural, name, status_patch):
    """
    Patch the status of a custom resource
    Uses strategic merge patch to avoid overwriting other fields
    """
    api = client.CustomObjectsApi()
    
    try:
        # Get current resource to merge status
        current = api.get_cluster_custom_object_status(group, version, plural, name)
        current_status = current.get('status', {})
        
        # Deep merge status_patch into current_status
        merged_status = deep_merge(current_status, status_patch)
        
        # Patch with merged status
        body = {'status': merged_status}
        api.patch_cluster_custom_object_status(
            group, version, plural, name, body
        )
        
        logger.debug(f"Patched status for {plural}/{name}")
        return True
        
    except client.exceptions.ApiException as e:
        if e.status == 404:
            logger.error(f"Resource {plural}/{name} not found")
            return False
        
        logger.error(f"Failed to patch status for {plural}/{name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error patching status: {e}")
        return False


def deep_merge(base, updates):
    """
    Deep merge two dictionaries
    Updates values in base with values from updates, recursively
    """
    result = copy.deepcopy(base)
    
    for key, value in updates.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def get_status(group, version, plural, name):
    """Get the current status of a custom resource"""
    api = client.CustomObjectsApi()
    
    try:
        resource = api.get_cluster_custom_object_status(group, version, plural, name)
        return resource.get('status', {})
    except Exception as e:
        logger.error(f"Failed to get status for {plural}/{name}: {e}")
        return {}


def update_node_phase(group, version, plural, cr_name, node_name, phase, message=''):
    """Helper to update a specific node's phase in the CR status"""
    from datetime import datetime, timezone
    
    status_patch = {
        'nodes': {
            node_name: {
                'phase': phase,
                'message': message,
                'lastUpdated': datetime.now(timezone.utc).isoformat()
            }
        },
        'lastUpdated': datetime.now(timezone.utc).isoformat()
    }
    
    return patch_status(group, version, plural, cr_name, status_patch)


def compute_summary(nodes_status):
    """Compute summary statistics from node statuses"""
    if not nodes_status:
        return {
            'total': 0,
            'completed': 0,
            'upgrading': 0,
            'pending': 0,
            'failed': 0
        }
    
    return {
        'total': len(nodes_status),
        'completed': sum(1 for n in nodes_status.values() if n.get('phase') == 'Completed'),
        'upgrading': sum(1 for n in nodes_status.values() 
                        if n.get('phase') in ['Draining', 'Upgrading', 'Verifying', 'Uncordoning']),
        'pending': sum(1 for n in nodes_status.values() if n.get('phase') == 'Pending'),
        'failed': sum(1 for n in nodes_status.values() if n.get('phase') == 'Failed')
    }
