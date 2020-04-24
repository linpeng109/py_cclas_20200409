from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class HCSParser(Parser):
    def getHCSDF(self, filename: str, sheet_name: str):
        dict = {'sheet_name': 0, 'header': None}
        hcsDf = pd.read_excel(filename, **dict)
        # 删除表标题
        hcsDf.drop(index=[0, 1], inplace=True)
        # 按照0列排列升序
        hcsDf.sort_index(0, ascending=False, inplace=True)
        hcsDf.fillna('', inplace=True)
        # 删除空列
        hcsDf.dropna(axis=1, how='any', inplace=True)
        self.logger.debug(hcsDf)
        # newfilename = self.__getNewFilename(filename=hcsExcelFileName, type='hcs')
        # encoding = self.config.get('hcs', 'encoding')
        # hcsDf.to_csv(newfilename, index=None, header=None, encoding=encoding, line_terminator='\r\n')
        return hcsDf


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    hcsParser = HCSParser(config=config, logger=logger)
    filename = r'E:\cclasdir\SES2017-HCS01.xls'
    hcsDf = hcsParser.getHCSDF(filename=filename)
    logger.debug(hcsDf)
