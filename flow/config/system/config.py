import functools
import os
import yaml
from typing import List
#from flow.config.system.log import logger
from flow.utils.globalpath import parent_path,project_name
pth=project_name()
project_path = parent_path('assessment')


def check_envvironment():
    """获取当前环境名称

    :return:
        float:环境名称
    """
    #env=os.environ.get('DEPLOYMODE')
    env = 'dev'
    val_range = ['dev','stg']
    if env not in val_range:
        msg='need env mode DEPLOYMODE, must be one of {}'.format(val_range)
        #ogger.error(msg)
        raise ValueError(msg)
    return env

def get_config_file():
    """获取环境的配置文件

    :return:
        float:地址名称
    """
    env=check_envvironment()
    print(os.path.join(parent_path('assessment')))
    parent_dir = os.path.join(parent_path('assessment'),'project','config')
    print('parent_dir',parent_dir)
    conf_file = os.path.join(parent_dir,'settings-{ENV}.yml'.format(ENV=env))
    return conf_file

@functools.lru_cache()
def get_setting(partition,default=None):
    """读取环境变量的配置

    :param partition（float）: 地址名称
    :param default:
    :return:
            float:环境名称
    """
    conf_file_path = get_config_file()
    file = open(conf_file_path,'r',encoding='utf-8')
    file_data = file.read()
    file.close()
    data = yaml.load(file_data)
    for flag in partition.split(":"):
        data = data[flag]
    return data

def get_proj_name():
    """获取环境的配置文件

    :return:
        float：地址名称
    """
    return get_setting('project_name')


_web_app = None
_module_routers = []

def register_app(app):
    """app名称注册全局变量

    :param app（float）:app名称
    :return:
        float：全局变量怼app名称
    """
    global _web_app
    if _web_app is not  None:
        raise Exception('please check you web frame !')
    _web_app = app

def get_app()->List[object]:
    global _web_app
    return _web_app


def register_routers(router ,*args,**kwargs):
    """生成前端框架名称

    :param router（float）: 框架名称
    :param args（float）: api配置
    :param kwargs（float）: api配置
    :return:
        float：module_routers名称
    """
    global _module_routers
    _module_routers.append([router , args , kwargs])

def get_routers():
    """全局module_routers 注册

    :return:
            float: 全局module_routers
    """
    global _module_routers
    return _module_routers

def create_api(name , url_prefix , *args , **kwargs):
    """创建前端api

    :param name(float):app名称
    :param url_prefix(float):url一级域名
    :param args(float):api配置
    :param kwargs(float): api配置
    :return:
            float:成功与否
    """
    try:
        from fastapi import FastAPI,APIRouter
        class FastAPIRouter(APIRouter):
            def route(self,rule,**options):
                self.redirect_slashes = options.get('strict_slashes',True)
                return self.api_route(path=rule,methods=options.get('methods'))
        router = FastAPIRouter()
        register_routers(router,prefix=url_prefix,*args,**kwargs)
    except ImportError:
        pass
    raise Exception('only fastapi')

