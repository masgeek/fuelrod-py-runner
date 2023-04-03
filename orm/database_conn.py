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
