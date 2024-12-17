import logging
from logging.config import dictConfig
from os import makedirs
from os.path import dirname,exists,join
abs_path = dirname(__file__)
dir_path = join(abs_path,"..","..","logs")
if not exists(dir_path):
    makedirs(dir_path,exist_ok=True)
dictConfig(
    {"version":1,
     "formatters":{
         "default":{
             "format":"[%(asctime)s]-[%(module)s]-%(levelname)s: %(message)s",
         }
     },
     "handler":{
         "running_handler":{
             "class" : "logging.handlers.RotalingFileHandler",
             "formatter":"default",
             "level":"INFO",
             "filename":join(dir_path,"running.log"),
             "encoding":"utf-8",
             "maxBytes":1024*1024*100,
             "backupCount":5,

         },
         "critical_handler":{
             "class" : "logging.handlers.RotalingFileHandler",
             "formatter":"default",
             "level":"INFO",
             "filename":join(dir_path,"error.log"),
             "encoding":"utf-8",
             "maxBytes":1024*1024*100,
             "backupCount":1,

         },
     },
     "root":{"level":"INFO","handler":["running_handler","critical_handler"]},
     }
)
logger = logging.getLogger(__name__)
logger.info('setting system log')