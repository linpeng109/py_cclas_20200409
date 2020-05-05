import xlrd

from py_config import ConfigFactory
from py_logging import LoggerFactory


class XrldParser():
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def xlrdParser(self, filename):
        target = ['HBY', 'JDY', 'QTY', 'SCN', 'XJY', 'SES2017', 'Report']
        _f = xlrd.open_workbook(filename=filename)
        sheet_names = _f.sheet_names()
        result = []
        for item in target:
            if item in sheet_names:
                result.append(item)
        return result


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    xlrdParser = XrldParser(config=config, logger=logger)
    filenames = [r'E:\cclasdir\SES2017-HCS01.xls',
                 r'E:\cclasdir\20190605As-AAS01.xlsx',
                 r'E:\cclasdir\20200418As-AFS01.xlsx',
                 r'E:\cclasdir\20200418Hg-AFS02.xlsx',
                 r'E:\cclasdir\2020swyhy.xlsx',
                 r'E:\cclasdir\2020环保.xlsx',
                 r'E:\cclasdir\2020生物氧化.xlsx',
                 r'E:\cclasdir\2020细菌氧化.xlsx',
                 r'E:\cclasdir\2020细菌氧化_备份.xlsx']
    for filename in filenames:
        result = xlrdParser.xlrdParser(filename=filename)
        for item in result:
            print(item)
