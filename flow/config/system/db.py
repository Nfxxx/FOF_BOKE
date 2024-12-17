from urllib.parse import quote_plus
from sqlalchemy import engine_from_config
#from flow.utils.parmext import is_any_enpty
from flow.config.system.log import logger
from .config import get_setting

def init_db_engines():
    """
    get db  engine from url or setting
    :return:
    """
    db_setting = get_setting(partition='db')
    engines = {}
    for name , setting in db_setting.items():
        if setting.get('enabled',True) is False:
            continue
        company_no = get_setting('deployed_company_no',None)
        dburl = None
        if company_no is not None :
            pass