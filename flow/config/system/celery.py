from celery import Celery,Task
#from flow.config.system.log import logger
from flow.config.system.config import get_setting


PROJ_NAME ="realtimesrv_celery_tasks"
cfgobj = get_setting('celery')


def _register_autodiscover_tasks(app):
    """自动发现注册task任务

    :param app（float）: app名称
    """
    packages = ['app']
    task_module_name='celery_tasks'
    app.autodiscover_tasks(packages,task_module_name,force=True)
class MyCelery(Celery):
    def gen_task_name(self, name, module):
        """获取task名称

        :param name（float）:task名称
        :param module(float):task的目录
        :return:
        """
        if module.endswith('.tasks'):
            module=module[:-6]
        return super(MyCelery,self).gen_task_name(name,module)
class RtsrvTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """失败任务打印

        :param exc（float）: 错误名称
        :param task_id(float): 任务id
        :return:
            float:任务错误
        """
        print('{0!r} failed: {1!r}'.format(task_id,exc))
def use_queue(qname=None,task_name=None,*args,**kwargs):
    """使用列表

    :param qname(float):队列名称
    :param task_name (float): 任务名称
    :return:
            float:任务和队列对应名称
    """
    global  celeryapp
    conf = celeryapp.conf
    if qname is None:
        qname = conf.task_default_queue
    if conf.task_routes is None:
        conf.task_routes ={}
    task_routes = conf.task_routes
    def decorate(func):
        nonlocal conf,task_routes,qname,task_name
        if conf.task_create_queues is False:
            return func
        if func.name is None:
            print('waring')
            #logger.warning("保证task name一致")
        else:
            task_name = func.name
        task_routes[task_name]={'queue':qname}
        print('-------> user defind queue route ')
        return func
    return decorate
celeryapp = Celery(PROJ_NAME)
if cfgobj is not  None:
    celeryapp.conf.update(cfgobj)
_register_autodiscover_tasks(celeryapp)


