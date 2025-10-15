"""
DirectUpdate CR Handler
Handles the creation, update, and reconciliation of DirectUpdate resources
"""
import kopf
import logging
import uuid
from datetime import datetime, timezone
from kubernetes import client

from lib import planner, preflight, state, backup, version

logger = logging.getLogger(__name__)

GROUP = "cupcake.ricardomolendijk.com"
VERSION = "v1"
PLURAL = "directupdates"


@kopf.on.create(GROUP, VERSION, PLURAL)
def create_direct_update(spec, name, namespace, status, **kwargs):
    """Handle DirectUpdate creation"""
    logger.info(f"DirectUpdate {name} created")
    
    # Generate operation ID
    operation_id = str(uuid.uuid4())
    logger.info(f"Generated operation ID: {operation_id}")
    
    # Validate spec
    target_version_str = spec.get('targetVersion')
    if not target_version_str:
        raise kopf.PermanentError("targetVersion is required")
    
    # Validate version format
    is_valid, message = version.validate_version_string(target_version_str)
    if not is_valid:
        raise kopf.PermanentError(f"Invalid target version: {message}")
    
    logger.info(f"Target version validation: {message}")
    
    # Get current cluster version
    current_version = version.get_current_cluster_version()
    if not current_version:
        logger.warning("Could not determine current cluster version, proceeding with caution")
        initial_message = 'Operation initialized'
        upgrade_path_info = None
    else:
        logger.info(f"Current cluster version: {current_version}")
        
        target_version = version.Version(target_version_str)
        
        # Check if upgrade path is needed
        upgrade_path = version.calculate_upgrade_path(current_version, target_version)
        
        if not upgrade_path:
            raise kopf.PermanentError(
                f"Target version {target_version} is not newer than current {current_version}"
            )
        
        # Log upgrade path
        path_message = version.format_upgrade_path_message(upgrade_path)
        logger.info(f"Upgrade path: {path_message}")
        
        # Get and log warnings
        warnings = version.get_upgrade_warnings(current_version, target_version)
        for warning in warnings:
            logger.warning(f"Upgrade warning: {warning}")
        
        # Prepare upgrade path info
        if len(upgrade_path) > 1:
            logger.warning(
                f"Multi-step upgrade required! Will upgrade through {len(upgrade_path)} versions: "
                f"{' → '.join(str(v) for v in upgrade_path)}"
            )
            upgrade_path_info = {
                'currentVersion': str(current_version),
                'targetVersion': str(target_version),
                'steps': [str(v) for v in upgrade_path],
                'currentStep': 0,
                'totalSteps': len(upgrade_path)
            }
            initial_message = (
                f'Multi-step upgrade required: {len(upgrade_path)} versions. '
                f'Path: {" → ".join(str(v) for v in upgrade_path)}'
            )
        elif version.is_patch_upgrade(current_version, target_version):
            upgrade_path_info = None
            initial_message = f'Patch upgrade: {current_version} → {target_version}'
        else:
            upgrade_path_info = None
            initial_message = f'Minor version upgrade: {current_version} → {target_version}'
    
    # Initialize status
    initial_status = {
        'phase': 'Pending',
        'operationID': operation_id,
        'startedAt': datetime.now(timezone.utc).isoformat(),
        'lastUpdated': datetime.now(timezone.utc).isoformat(),
        'message': initial_message,
        'nodes': {},
        'summary': {
            'total': 0,
            'completed': 0,
            'upgrading': 0,
            'pending': 0,
            'failed': 0
        }
    }
    
    # Add upgrade path if multi-step
    if upgrade_path_info:
        initial_status['upgradePath'] = upgrade_path_info
    
    # Patch status
    state.patch_status(GROUP, VERSION, PLURAL, name, initial_status)
    
    # Compute plan
    try:
        plan = planner.make_plan(spec)
        logger.info(f"Plan computed: {len(plan['control_plane_nodes'])} control-plane, "
                   f"{len(plan['worker_nodes'])} worker nodes")
        
        # Initialize node status entries
        nodes_status = {}
        all_nodes = plan['control_plane_nodes'] + plan['worker_nodes']
        
        for node_name in all_nodes:
            nodes_status[node_name] = {
                'phase': 'Pending',
                'lastStep': 'initialized',
                'startedAt': datetime.now(timezone.utc).isoformat(),
                'lastUpdated': datetime.now(timezone.utc).isoformat(),
                'message': 'Waiting to start'
            }
        
        # Update status with plan
        status_update = {
            'nodes': nodes_status,
            'summary': {
                'total': len(all_nodes),
                'completed': 0,
                'upgrading': 0,
                'pending': len(all_nodes),
                'failed': 0
            },
            'message': 'Plan computed, ready to begin'
        }
        
        state.patch_status(GROUP, VERSION, PLURAL, name, status_update)
        
    except Exception as e:
        logger.error(f"Failed to compute plan: {e}")
        state.patch_status(GROUP, VERSION, PLURAL, name, {
            'phase': 'Failed',
            'message': f'Planning failed: {str(e)}',
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        })
        raise kopf.PermanentError(f"Planning failed: {e}")
    
    return {'message': f'DirectUpdate {name} initialized with operation ID {operation_id}'}


@kopf.timer(GROUP, VERSION, PLURAL, interval=30.0, sharp=True)
def reconcile_direct_update(spec, name, status, **kwargs):
    """
    Reconcile DirectUpdate resources periodically
    This is the main control loop that drives the upgrade process
    """
    if not status:
        logger.debug(f"DirectUpdate {name} has no status yet, skipping")
        return
    
    phase = status.get('phase')
    operation_id = status.get('operationID')
    
    # Skip if not in a phase that requires action
    if phase in ['Succeeded', 'Failed', 'Cancelled']:
        logger.debug(f"DirectUpdate {name} is in terminal phase {phase}, skipping")
        return
    
    logger.debug(f"Reconciling DirectUpdate {name} (phase: {phase}, operation: {operation_id})")
    
    # If Pending, run preflight checks
    if phase == 'Pending':
        handle_pending_phase(spec, name, status, operation_id)
    
    # If InProgress, orchestrate upgrades
    elif phase == 'InProgress':
        handle_in_progress_phase(spec, name, status, operation_id)
    
    # If RequiresAttention, log and wait for manual intervention
    elif phase == 'RequiresAttention':
        logger.warning(f"DirectUpdate {name} requires attention")


def handle_pending_phase(spec, name, status, operation_id):
    """Handle operations in Pending phase"""
    logger.info(f"Running preflight checks for {name}")
    
    # Run preflight checks if enabled
    if not spec.get('preflightChecks', True):
        # Skip preflight checks
        state.patch_status(GROUP, VERSION, PLURAL, name, {
            'phase': 'InProgress',
            'message': 'Preflight checks skipped, starting upgrade',
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        })
        return
    
    try:
        plan = planner.make_plan(spec)
        checks = preflight.run_preflight_checks(spec, plan)
        
        # Update status with preflight results
        status_update = {
            'preflightResults': {
                'passed': checks['passed'],
                'checks': checks['checks']
            },
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        }
        
        if not checks['passed']:
            status_update['phase'] = 'RequiresAttention'
            status_update['message'] = 'Preflight checks failed'
            logger.error(f"Preflight checks failed for {name}")
        else:
            status_update['phase'] = 'InProgress'
            status_update['message'] = 'Preflight checks passed, starting upgrade'
            logger.info(f"Preflight checks passed for {name}")
        
        state.patch_status(GROUP, VERSION, PLURAL, name, status_update)
        
    except Exception as e:
        logger.error(f"Preflight checks error: {e}")
        state.patch_status(GROUP, VERSION, PLURAL, name, {
            'phase': 'RequiresAttention',
            'message': f'Preflight checks error: {str(e)}',
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        })


def handle_in_progress_phase(spec, name, status, operation_id):
    """Handle operations in InProgress phase"""
    logger.debug(f"Processing in-progress operation {name}")
    
    # Get current plan
    plan = planner.make_plan(spec)
    nodes_status = status.get('nodes', {})
    
    # Check control-plane nodes first
    control_plane_complete = all(
        nodes_status.get(node, {}).get('phase') in ['Completed', 'Failed']
        for node in plan['control_plane_nodes']
    )
    
    if not control_plane_complete:
        # Process control-plane nodes (one at a time)
        process_control_plane_nodes(spec, name, plan, nodes_status, operation_id)
    else:
        # Process worker nodes (with concurrency)
        process_worker_nodes(spec, name, plan, nodes_status, operation_id)
    
    # Update summary
    update_summary(name, nodes_status)
    
    # Check if all nodes are complete
    all_nodes = plan['control_plane_nodes'] + plan['worker_nodes']
    all_complete = all(
        nodes_status.get(node, {}).get('phase') == 'Completed'
        for node in all_nodes
    )
    
    if all_complete:
        state.patch_status(GROUP, VERSION, PLURAL, name, {
            'phase': 'Succeeded',
            'message': 'All nodes upgraded successfully',
            'completedAt': datetime.now(timezone.utc).isoformat(),
            'lastUpdated': datetime.now(timezone.utc).isoformat()
        })
        logger.info(f"DirectUpdate {name} completed successfully")


def process_control_plane_nodes(spec, name, plan, nodes_status, operation_id):
    """Process control-plane nodes sequentially"""
    for node_name in plan['control_plane_nodes']:
        node_phase = nodes_status.get(node_name, {}).get('phase', 'Pending')
        
        if node_phase == 'Pending':
            # Start upgrade for this control-plane node
            logger.info(f"Starting control-plane upgrade for node {node_name}")
            
            # Trigger backup if enabled
            if backup.is_backup_enabled():
                try:
                    logger.info(f"Taking etcd backup before upgrading {node_name}")
                    backup_result = backup.trigger_etcd_backup(node_name, operation_id)
                    
                    state.patch_status(GROUP, VERSION, PLURAL, name, {
                        'backupInfo': backup_result,
                        'lastUpdated': datetime.now(timezone.utc).isoformat()
                    })
                except Exception as e:
                    logger.error(f"Backup failed for {node_name}: {e}")
                    state.patch_status(GROUP, VERSION, PLURAL, name, {
                        'phase': 'RequiresAttention',
                        'message': f'Backup failed for {node_name}: {str(e)}',
                        'lastUpdated': datetime.now(timezone.utc).isoformat()
                    })
                    return
            
            # Annotate node for agent pickup
            annotate_node_for_upgrade(node_name, operation_id, spec)
            
            # Update node status
            update_node_status(name, node_name, 'Upgrading', 'Control-plane upgrade initiated')
            
            # Only process one control-plane node at a time
            break
        
        elif node_phase in ['Draining', 'Upgrading', 'Verifying', 'Uncordoning']:
            # Still in progress, wait
            logger.debug(f"Node {node_name} is in phase {node_phase}, waiting")
            break


def process_worker_nodes(spec, name, plan, nodes_status, operation_id):
    """Process worker nodes with configured concurrency"""
    concurrency = spec.get('concurrency', 1)
    
    # Count currently upgrading workers
    upgrading_count = sum(
        1 for node in plan['worker_nodes']
        if nodes_status.get(node, {}).get('phase') in ['Draining', 'Upgrading', 'Verifying', 'Uncordoning']
    )
    
    # Start new upgrades up to concurrency limit
    for node_name in plan['worker_nodes']:
        if upgrading_count >= concurrency:
            break
        
        node_phase = nodes_status.get(node_name, {}).get('phase', 'Pending')
        
        if node_phase == 'Pending':
            logger.info(f"Starting worker upgrade for node {node_name}")
            
            # Annotate node for agent pickup
            annotate_node_for_upgrade(node_name, operation_id, spec)
            
            # Update node status
            update_node_status(name, node_name, 'Upgrading', 'Worker upgrade initiated')
            
            upgrading_count += 1


def annotate_node_for_upgrade(node_name, operation_id, spec):
    """Annotate node with upgrade instructions for agent"""
    try:
        v1 = client.CoreV1Api()
        
        # Create annotation with operation details
        annotation_value = operation_id
        
        body = {
            "metadata": {
                "annotations": {
                    "cupcake.ricardomolendijk.com/operation-id": operation_id,
                    "cupcake.ricardomolendijk.com/target-version": spec.get('targetVersion'),
                    "cupcake.ricardomolendijk.com/components": ",".join(spec.get('components', ['kubeadm', 'kubelet'])),
                    "cupcake.ricardomolendijk.com/status": "pending"
                }
            }
        }
        
        v1.patch_node(node_name, body)
        logger.info(f"Annotated node {node_name} with operation {operation_id}")
        
    except Exception as e:
        logger.error(f"Failed to annotate node {node_name}: {e}")
        raise


def update_node_status(cr_name, node_name, phase, message):
    """Update status for a specific node"""
    status_update = {
        'nodes': {
            node_name: {
                'phase': phase,
                'message': message,
                'lastUpdated': datetime.now(timezone.utc).isoformat()
            }
        },
        'lastUpdated': datetime.now(timezone.utc).isoformat()
    }
    
    state.patch_status(GROUP, VERSION, PLURAL, cr_name, status_update)


def update_summary(cr_name, nodes_status):
    """Update summary counts in CR status"""
    summary = {
        'total': len(nodes_status),
        'completed': sum(1 for n in nodes_status.values() if n.get('phase') == 'Completed'),
        'upgrading': sum(1 for n in nodes_status.values() 
                        if n.get('phase') in ['Draining', 'Upgrading', 'Verifying', 'Uncordoning']),
        'pending': sum(1 for n in nodes_status.values() if n.get('phase') == 'Pending'),
        'failed': sum(1 for n in nodes_status.values() if n.get('phase') == 'Failed')
    }
    
    state.patch_status(GROUP, VERSION, PLURAL, cr_name, {
        'summary': summary,
        'lastUpdated': datetime.now(timezone.utc).isoformat()
    })


@kopf.on.delete(GROUP, VERSION, PLURAL)
def delete_direct_update(name, **kwargs):
    """Handle DirectUpdate deletion"""
    logger.info(f"DirectUpdate {name} deleted")
    return {'message': f'DirectUpdate {name} cleanup complete'}
