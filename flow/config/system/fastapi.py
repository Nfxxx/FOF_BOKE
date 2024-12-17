import importlib
import os
from fastapi import FastAPI, APIRouter
from flow.config.system import config
from flow.config.system.enmus import (API_VERSION)
from flow.config.system.log import logger


def get_conf_files(conf_path):
    """获取指定路径下所有文件名

    :param conf_path(str):etl的名称
    :return:
        list：传入文件路径下怼所有文件名
    """
    dir = []
    for root, dirs, files in os.walk(conf_path):
        dir += dirs
    return dir


def register_api_router(api_router):
    """向FASTAOI注册路由

    :param api_router（object）: APIRouter模块生成的对象
    :return:
    """
    api_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'app')
    module_list = get_conf_files(api_path)
    for module_name in module_list:
        module = importlib.import_module('{package}.api.api'.format(package="app"))
        logger.info('加载模块：%s' % module_name)
        if hasattr(module, config.create_api.__name__):
            logger.info('向FastAPI注册了路由')
        else:
            module_routers = config.get_routers()
            for router, args, kwargs in module_routers:
                api_router.include_router(router, prefix=kwargs['prefix'], tags=kwargs.get('tags'))


def create_app(config_obj: object = None, debug: bool = False):
    """创建FastAPI对象

    :param debug(bool): debug模式是否开启
    :return:
        app：FastAPI生成的对象
    """
    app = FastAPI(debug=debug)
    api_router = APIRouter()
    config.register_app(app)
    register_api_router(api_router)
    app.include_router(api_router, prefix='/{}'.format(API_VERSION))


if __name__ == '__main__':
    import uvicorn

    app = create_app(debug=True)
    uvicorn.run(app, host="0.0.0.0", port=5000)
