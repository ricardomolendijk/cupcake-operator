"""
ScheduledUpdate CR Handler
Handles scheduled upgrades that trigger DirectUpdate at a specific time
"""
import kopf
import logging
from datetime import datetime, timezone
from dateutil import parser as date_parser

from lib import state

logger = logging.getLogger(__name__)

GROUP = "cupcake.ricardomolendijk.com"
VERSION = "v1"
PLURAL = "scheduledupdates"


@kopf.on.create(GROUP, VERSION, PLURAL)
def create_scheduled_update(spec, name, **kwargs):
    """Handle ScheduledUpdate creation"""
    logger.info(f"ScheduledUpdate {name} created")
    
    schedule_at = spec.get('scheduleAt')
    if not schedule_at:
        raise kopf.PermanentError("scheduleAt is required")
    
    # Parse and validate schedule time
    try:
        scheduled_time = date_parser.isoparse(schedule_at)
    except Exception as e:
        raise kopf.PermanentError(f"Invalid scheduleAt format: {e}")
    
    initial_status = {
        'phase': 'Scheduled',
        'scheduledFor': schedule_at,
        'message': f'Scheduled for {schedule_at}'
    }
    
    state.patch_status(GROUP, VERSION, PLURAL, name, initial_status)
    
    return {'message': f'ScheduledUpdate {name} scheduled for {schedule_at}'}


@kopf.timer(GROUP, VERSION, PLURAL, interval=60.0)
def check_scheduled_update(spec, name, status, **kwargs):
    """Check if it's time to execute the scheduled update"""
    if not status or status.get('phase') != 'Scheduled':
        return
    
    schedule_at = spec.get('scheduleAt')
    scheduled_time = date_parser.isoparse(schedule_at)
    now = datetime.now(timezone.utc)
    
    if now >= scheduled_time:
        logger.info(f"Executing scheduled update {name}")
        # Create DirectUpdate CR
        # Implementation would create the DirectUpdate resource here
        
        state.patch_status(GROUP, VERSION, PLURAL, name, {
            'phase': 'Executing',
            'executedAt': now.isoformat(),
            'message': 'DirectUpdate created'
        })
