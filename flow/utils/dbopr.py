from  functools import lru_cache
from typing import List
import pandas as pd
from flow.config.system.log import logger


@lru_cache(maxsize=2 ** 10, typed=True)
def get_engine(name :str):
    """
    get db engine
    :param name: db name
    :return: db_engine
    """
    logger.info("DB engine name {}".format(name))
    db_engines = None

def df_to_sql(df:pd.DataFrame , table:str , eng_name =None ,if_exists = 'append',**other_kwargs):
    """
    insert data in table
    :param df:
    :param table:
    :param eng_name:
    :param if_exists:
    :param other_kwargs:
    :return:
    """
    coon = get_connection(eng_name)
    df.to_sql(table , coon ,if_exists=if_exists,**other_kwargs)

def get_connection(eng_name=None):
    """get db engine connection

    :param eng_nam:
    :return:
    """
    engine = get_engine(eng_name)
    return engine.connect()


import sqlalchemy
sqlalchemy.create_engine