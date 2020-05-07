import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class XJYParser(Parser):

    def getXJYDF(self, filename: str, sheet_name: str):
        # 读取数据
        dict = {'sheet_name': sheet_name, 'header': None, }
        xjyDF = pd.read_excel(io=filename, **dict)
        print(xjyDF)
        # 检查列名是否重复或者空值
        xjyDF=self.get_valid_dataframe(xjyDF)
        # 删除表头
        xjyDF.drop(axis=0, index=[0, 1], inplace=True)
        # 填充缺失项
        xjyDF['DATE'].fillna(method='ffill', inplace=True)
        xjyDF['TIME'].fillna(method='ffill', inplace=True)
        # 处理日期和时间列
        try:
            xjyDF['DATE'] = pd.to_datetime(xjyDF['DATE'], format='%m/%d/%Y')
            xjyDF['TIME'] = pd.to_datetime(xjyDF['TIME'], format='%H:%M:%S')
        except ValueError as error:
            logger.error(error)
        # 删除空行
        xjyDF.dropna(axis=0, how='all', inplace=True)
        # 过滤nan
        xjyDF.fillna('', inplace=True)
        # 重建索引
        xjyDF.reset_index(drop=True, inplace=True)
        return xjyDF


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    xjyParser = XJYParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020细菌氧化.xlsx'
    sheet_name = '01'
    method = 'SY001'

    xjyDF = xjyParser.getXJYDF(filename=filename, sheet_name=sheet_name)
    increamentDF = xjyParser.getIncreamentDF(srcDF=xjyDF, filename=filename, sheet_name=sheet_name)
    reports = xjyParser.buildReport(dataframe=increamentDF, sheet_name='XJY', method=method)
    xjyParser.outputReport(reports=reports)
    xjyParser.reportFileHandle(filename=filename, sheet_name='XJY')
