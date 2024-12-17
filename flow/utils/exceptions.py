import traceback
from flow.config.system.log import logger
from flow.config.system import enmus
def print_excp_traceback(*args,**kwargs):
    """
    exceptions record in logs and screen

    """
    traceback.print_exc()
    logger.error('-'.join([str(args),str(kwargs),traceback.format_exc()]))
