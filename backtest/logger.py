import os
import logging
import logging.handlers
import sys
import time


class Logger(object):
    def __init__(self):
        self._name = "root"
        self._level = logging.INFO
        self._filename = "all.log"
        self._format = None
        self._logger = logging.getLogger(self._name)
        self._get_frame = getattr(sys, '_getframe')

    @property
    def level(self):
        return self._level

    @property
    def filename(self):
        return self._filename

    @property
    def name(self):
        return self._name

    def init_logger(self, name, level, filename):
        self._level = getattr(logging, level.upper())

        self._filename = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), name, filename)
        self._format = logging.Formatter(f"[%(mytime)s][%(levelname)s][{name}] %(myfile)s:%(myline)d => %(message)s")

        self._logger = logging.getLogger(name)
        self._name = name

        self._logger.setLevel(self._level)

        self._init_consolelog()
        self._init_filelog()

    def _init_consolelog(self):
        handler = logging.StreamHandler()
        handler.setLevel(self._level)
        handler.setFormatter(self._format)
        self._logger.addHandler(handler)

    def _init_filelog(self):
        handler = logging.handlers.TimedRotatingFileHandler(
            self._filename, when='midnight', interval=1, backupCount=360, encoding='utf8'
        )
        handler.setLevel(self._level)
        handler.setFormatter(self._format)
        self._logger.addHandler(handler)

    def _get_caller(self, depth=0):
        """
        获取调用者所在文件名和行号
        :return:
        """
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        msec = (time.time() - int(time.time())) * 1000
        mytime = '%s,%03d' % (now, msec)

        # user: 2->logger.debug: 1->logger._get_caller: 0
        frame = self._get_frame(depth + 2)
        filename = os.path.split(frame.f_code.co_filename)[1]
        return {"myfile": filename, "myline": frame.f_lineno, "mytime": mytime}

    def debug(self, msg, depth=0, *args, **kwargs):
        self._logger.debug(msg, *args, **kwargs, extra=self._get_caller(depth))

    def error(self, msg, depth=0, *args, **kwargs):
        self._logger.error(msg, *args, **kwargs, extra=self._get_caller(depth))

    def warning(self, msg, depth=0, *args, **kwargs):
        self._logger.warning(msg, *args, **kwargs, extra=self._get_caller(depth))

    def info(self, msg, *args, depth=0, **kwargs):
        self._logger.info(msg, *args, **kwargs, extra=self._get_caller(depth))


logger = Logger()
logger.init_logger("backtest", "DEBUG", "logs/lob.log")

