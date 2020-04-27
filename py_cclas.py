import multiprocessing
import os

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_watchdog import WatchDogObServer

# class CCLAS():
#     def __init__(self):
#         self.config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
#         self.logger = LoggerFactory(config=self.config).getLogger()
#
#     def start(self):
#         wObserver = WatchDogObServer(config=self.config, logger=self.logger)
#         wObserver.start()

if __name__ == '__main__':
    if os.sys.platform.startswith('win'):
        multiprocessing.freeze_support()

    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    wObserver = WatchDogObServer(config=config, logger=logger)
    wObserver.start()
