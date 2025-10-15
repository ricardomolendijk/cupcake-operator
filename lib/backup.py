"""
Backup module - Handles etcd backup operations
"""
import logging
import os
from datetime import datetime, timezone
from kubernetes import client

logger = logging.getLogger(__name__)


def is_backup_enabled():
    """Check if external backup store is enabled"""
    enabled = os.getenv('BACKUP_STORE_ENABLED', 'false').lower() == 'true'
    logger.debug(f"Backup enabled: {enabled}")
    return enabled


def get_backup_config():
    """Get backup store configuration from environment"""
    return {
        'enabled': is_backup_enabled(),
        'type': os.getenv('BACKUP_STORE_TYPE', 's3'),
        'bucket': os.getenv('BACKUP_STORE_BUCKET', ''),
        'endpoint': os.getenv('BACKUP_STORE_ENDPOINT', ''),
        'region': os.getenv('BACKUP_STORE_REGION', 'us-east-1')
    }


def trigger_etcd_backup(node_name, operation_id):
    """
    Trigger etcd backup on the specified control-plane node
    This creates a ConfigMap that the agent will pick up and execute
    Returns backup info dict
    """
    v1 = client.CoreV1Api()
    namespace = os.getenv('NAMESPACE', 'kube-system')
    
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    snapshot_name = f"etcd-snapshot-{operation_id}-{timestamp}"
    
    # Create ConfigMap with backup instructions
    cm_name = f"backup-{operation_id}-{node_name}".replace('.', '-')
    
    config_map = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=cm_name,
            namespace=namespace,
            labels={
                'app.kubernetes.io/managed-by': 'cupcake',
                'cupcake.ricardomolendijk.com/operation-id': operation_id,
                'cupcake.ricardomolendijk.com/backup': 'true'
            }
        ),
        data={
            'operation_id': operation_id,
            'node_name': node_name,
            'snapshot_name': snapshot_name,
            'backup_type': 'etcd',
            'timestamp': timestamp
        }
    )
    
    try:
        v1.create_namespaced_config_map(namespace, config_map)
        logger.info(f"Created backup ConfigMap {cm_name} for node {node_name}")
        
        backup_info = {
            'etcdSnapshot': snapshot_name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'node': node_name,
            'status': 'initiated'
        }
        
        return backup_info
        
    except Exception as e:
        logger.error(f"Failed to create backup ConfigMap: {e}")
        raise


def check_backup_status(operation_id, node_name):
    """
    Check the status of a backup operation
    Looks for a status ConfigMap created by the agent
    """
    v1 = client.CoreV1Api()
    namespace = os.getenv('NAMESPACE', 'kube-system')
    
    status_cm_name = f"backup-status-{operation_id}-{node_name}".replace('.', '-')
    
    try:
        cm = v1.read_namespaced_config_map(status_cm_name, namespace)
        
        return {
            'completed': cm.data.get('completed', 'false') == 'true',
            'success': cm.data.get('success', 'false') == 'true',
            'message': cm.data.get('message', ''),
            'snapshot_path': cm.data.get('snapshot_path', ''),
            'upload_path': cm.data.get('upload_path', '')
        }
        
    except client.exceptions.ApiException as e:
        if e.status == 404:
            # Status not yet available
            return {
                'completed': False,
                'success': False,
                'message': 'Backup in progress'
            }
        else:
            logger.error(f"Failed to check backup status: {e}")
            return {
                'completed': False,
                'success': False,
                'message': f'Error checking status: {str(e)}'
            }


def cleanup_backup_configmaps(operation_id):
    """Clean up backup-related ConfigMaps for completed operation"""
    v1 = client.CoreV1Api()
    namespace = os.getenv('NAMESPACE', 'kube-system')
    
    try:
        # List ConfigMaps with operation ID label
        cms = v1.list_namespaced_config_map(
            namespace,
            label_selector=f'cupcake.ricardomolendijk.com/operation-id={operation_id},cupcake.ricardomolendijk.com/backup=true'
        )
        
        for cm in cms.items:
            try:
                v1.delete_namespaced_config_map(cm.metadata.name, namespace)
                logger.info(f"Deleted backup ConfigMap {cm.metadata.name}")
            except Exception as e:
                logger.warning(f"Failed to delete ConfigMap {cm.metadata.name}: {e}")
                
    except Exception as e:
        logger.error(f"Failed to cleanup backup ConfigMaps: {e}")
