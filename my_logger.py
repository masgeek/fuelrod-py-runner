import logging

from colorlog import ColoredFormatter
from os import path, mkdir, environ
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(verbose=True)


class MyDb:
    db_engine = None
    db_url = environ.get('DB_URL', 'mysql://fuelrod:fuelrod@localhost/fuelrod')
    debug_db = environ.get('DEBUG_DB', False)

    def __new__(cls, *args, **kwargs):
        if cls.db_engine is None:
            cls.db_engine = create_engine(url=cls.db_url,
                                          echo=cls.debug_db,
                                          echo_pool=cls.debug_db,
                                          hide_parameters=not cls.debug_db)

        return cls.db_engine


class MyLogger:
    _logger = None
    log_level = environ.get('LOG_LEVEL', 'INFO')

    def __new__(cls, *args, **kwargs):
        if cls._logger is None:
            cls._logger = super().__new__(cls, *args, **kwargs)
            cls._logger = logging.getLogger("fuelrod")
            cls._logger.setLevel(cls.log_level)

            file_fmt = logging.Formatter(
                '%(asctime)s %(levelname)s %(thread)s [%(filename)s:%(lineno)s %(funcName)s] :: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')

            console_fmt = ColoredFormatter(
                fmt="%(asctime)s %(log_color)s%(levelname)-8s%(reset)s | [%(filename)s:%(lineno)s %(funcName)s] |"
                    " %(log_color)s%(message)s",
                datefmt='%Y-%m-%d %H:%M:%S',
                reset=True,
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold_red,bg_white',
                },
                secondary_log_colors={},
                style='%')

            now = datetime.datetime.now()
            dirname = "./logs"

            if not path.isdir(dirname):
                mkdir(dirname)

            base_file_name = dirname + "/log_" + now.strftime("%Y-%m-%d") + ".log"

            info_log_file = path.join(path.dirname(path.abspath(__file__)), base_file_name)

            file_handler = logging.FileHandler(info_log_file)

            stream_handler = logging.StreamHandler()

            file_handler.setFormatter(file_fmt)
            stream_handler.setFormatter(console_fmt)

            cls._logger.addHandler(stream_handler)
            cls._logger.addHandler(file_handler)

        return cls._logger
