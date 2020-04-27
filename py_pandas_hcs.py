import pandas as pd

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
        # 更改列名
        columns = {0: 'SAMPLEID', 1: 'test_id', 2: 'sample_q', 3: 'sample_id', 4: 'c_1', 5: 's_1', 6: 'c_2', 7: 's_2',
                   8: 'sp', 9: 'cp', 10: 'c_a', 11: 's_b', 12: 'DATE', 13: 'al', 14: 'status', 15: 'c_avg', 16: 's_avg',
                   17: 'c_b', 18: 's_b', 19: 'c_rsd', 20: 's_rsd'}
        hcsDf.rename(columns=columns, inplace=True)

        hcsDf['DATE'] = pd.to_datetime(hcsDf['DATE'], format='%Y/%m/%d', errors='ignore')

        hcsDf.to_csv('a.csv', encoding='gbk')
        # hcsDf.fillna('', inplace=True)
        # 删除空列
        # hcsDf.dropna(axis=1, how='any', inplace=True)
        # self.logger.debug(hcsDf)
        # newfilename = self.__getNewFilename(filename=hcsExcelFileName, type='hcs')
        # encoding = self.config.get('hcs', 'encoding')
        # hcsDf.to_csv(newfilename, index=None, header=None, encoding=encoding, line_terminator='\r\n')
        return hcsDf


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    hcsParser = HCSParser(config=config, logger=logger)
    filename = r'E:\cclasdir\SES2017-HCS01.xls'
    sheet_name = '样品测量数据'
    hcsDf = hcsParser.getHCSDF(filename=filename, sheet_name=sheet_name)
    logger.debug(hcsDf)
