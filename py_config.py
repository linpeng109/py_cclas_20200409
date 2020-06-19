import configparser
import os
from configparser import ConfigParser


class ConfigFactory():
    def __init__(self, config_file_name: str):
        self.configFile = os.path.join(os.getcwd(), config_file_name)

    class _Configparser(ConfigParser):
        def optionxform(self, optionstr):
            return optionstr

    def getConfig(self):
        config_parser = self._Configparser()
        # 配置文件中使用变量调用
        config_parser._interpolation = configparser.ExtendedInterpolation()
        config_parser.read(filenames=self.configFile, encoding='utf-8')
        return config_parser


if __name__ == '__main__':
    cfg = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    dic = dict(cfg.items('sheetname'))
    print(dic)
