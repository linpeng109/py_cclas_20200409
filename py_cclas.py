import multiprocessing
import os

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_watchdog import WatchDogObServer

# WatchDog for CCLS Desktop 版本入口
if __name__ == '__main__':
    if os.sys.platform.startswith('win'):
        multiprocessing.freeze_support()

    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    wObserver = WatchDogObServer(config=config, logger=logger)
    wObserver.start()
