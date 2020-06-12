import logging
import sys
from logging.handlers import RotatingFileHandler

import psutil

from py_config import ConfigFactory


class LoggerFactory():
    def __init__(self, config: ConfigFactory):
        self.config = config

    def getLogger(self):
        fileHandlerDict = dict(self.config.items('logger'))
        fileHandlerDict['maxBytes'] = int(fileHandlerDict['maxBytes'])
        fileHandlerDict['backupCount'] = int(fileHandlerDict['backupCount'])
        fileHandler = RotatingFileHandler(**fileHandlerDict)
        formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y%b%d-%H:%M:%S")
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(formatter)
        logger = logging.getLogger()
        logger.addHandler(fileHandler)
        logger.addHandler(streamHandler)
        logger.setLevel(self.config.getint('default', 'logger_level'))
        return logger


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    logger.debug('Hello world!')
    for i in range(100):
        cpuper = psutil.cpu_percent()
        mem = psutil.virtual_memory()
        line = f'cpu:{cpuper}% mem:{mem} '
        logger.info(line)
