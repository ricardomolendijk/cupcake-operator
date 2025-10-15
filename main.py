#!/usr/bin/env python3
"""
CUPCAKE - Control-plane Upgrade Platform for Continuous Kubernetes Automation and Evolution
Main Entry Point for the Operator
"""
import os
import sys
import logging
import kopf
from kubernetes import client, config
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
upgrade_operations_total = Counter(
    'upgrade_operations_total',
    'Total number of upgrade operations',
    ['phase', 'operation_id']
)
upgrade_operation_nodes_total = Gauge(
    'upgrade_operation_nodes_total',
    'Number of nodes in various states',
    ['operation_id', 'status']
)
upgrade_node_step_duration = Histogram(
    'upgrade_node_step_duration_seconds',
    'Duration of node upgrade steps',
    ['operation_id', 'node', 'step']
)
upgrade_in_progress = Gauge(
    'upgrade_in_progress',
    'Number of upgrades currently in progress',
    ['operation_id']
)


def load_kubernetes_config():
    """Load Kubernetes configuration (in-cluster or kubeconfig)"""
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes configuration")
        return
    except config.ConfigException:
        pass
    
    try:
        config.load_kube_config()
        logger.info("Loaded kubeconfig configuration")
        return
    except config.ConfigException:
        pass
    
    logger.error("Could not load Kubernetes configuration")
    sys.exit(1)


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    """Configure operator settings on startup"""
    # Load Kubernetes config
    load_kubernetes_config()
    
    # Leader election settings
    leader_enabled = os.getenv('LEADER_ELECTION_ENABLED', 'true').lower() == 'true'
    
    if not leader_enabled:
        settings.peering.standalone = True
    else:
        settings.peering.priority = 0
        settings.peering.name = os.getenv('OPERATOR_NAME', 'cupcake')
        settings.peering.lifetime = 60
    
    # Configure watching
    settings.watching.server_timeout = 600
    settings.watching.client_timeout = 660
    
    # Start metrics server
    metrics_enabled = os.getenv('METRICS_ENABLED', 'true').lower() == 'true'
    if not metrics_enabled:
        logger.info("Operator configuration complete")
        return
    
    metrics_port = int(os.getenv('METRICS_PORT', '8080'))
    start_http_server(metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")
    logger.info("Operator configuration complete")


@kopf.on.probe(id='now')
def get_current_timestamp(**kwargs):
    """Health probe that returns current timestamp"""
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# Import handlers after configuration
from handlers import direct_update, scheduled_update, update_schedule

# Register handlers
logger.info("CUPCAKE operator starting...")


if __name__ == '__main__':
    kopf.run()
