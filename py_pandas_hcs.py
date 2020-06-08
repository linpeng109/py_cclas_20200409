import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class HCSParser(Parser):
    def getHCSDF(self, filename: str, sheet_name):
        dict = {'sheet_name': sheet_name, 'header': None}
        hcs_df = pd.read_excel(filename, **dict)
        # 删除表标题：样品参数、标准值、测试值……
        hcs_df.drop(axis=0, index=[0], inplace=True)
        # 获取列名
        header = hcs_df.iloc[0]
        # 切割有效数据
        hcs_df = hcs_df.loc[1:]
        # 列重新命名
        hcs_df.columns = header
        # 删除表标题：碳、硫……
        hcs_df.drop(axis=0, index=[1], inplace=True)
        # 按照序号列（第0列）排列升序
        hcs_df.sort_index(0, ascending=False, inplace=True)
        hcs_df.fillna('', inplace=True)
        self.logger.debug(hcs_df)
        newfilename = self.getNewFilename(filename=filename, type='hcs')
        encoding = self.config.get('hcs', 'encoding')
        hcs_df.to_csv(newfilename, index=None, encoding=encoding, line_terminator='\r\n')
        return hcs_df


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    hcsParser = HCSParser(config=config, logger=logger)
    filename = r'E:\cclasdir\SES2017-HCS01.xls'
    sheet_name = 0
    hcsDf = hcsParser.getHCSDF(filename=filename, sheet_name=sheet_name)
    logger.debug(hcsDf)
