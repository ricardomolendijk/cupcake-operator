"""
UpdateSchedule CR Handler
Handles recurring update schedules with cron-like syntax
"""
import kopf
import logging

logger = logging.getLogger(__name__)

GROUP = "cupcake.ricardomolendijk.com"
VERSION = "v1"
PLURAL = "updateschedules"


@kopf.on.create(GROUP, VERSION, PLURAL)
def create_update_schedule(spec, name, **kwargs):
    """Handle UpdateSchedule creation"""
    logger.info(f"UpdateSchedule {name} created")
    
    schedule = spec.get('schedule')
    if not schedule:
        raise kopf.PermanentError("schedule is required")
    
    return {'message': f'UpdateSchedule {name} created with schedule {schedule}'}


@kopf.timer(GROUP, VERSION, PLURAL, interval=300.0)
def check_update_schedule(spec, name, status, **kwargs):
    """Check and execute scheduled updates based on cron schedule"""
    if spec.get('suspended', False):
        logger.debug(f"UpdateSchedule {name} is suspended")
        return
    
    # Implementation would check cron schedule and create ScheduledUpdate CRs
    logger.debug(f"Checking schedule for {name}")
