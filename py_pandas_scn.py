import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class SCNParser(Parser):

    def getSCNDF(self, filename: str, sheet_name: str):
        # 读取数据
        dict = {'sheet_name': sheet_name, 'header': None, }
        scnDF = pd.read_excel(io=filename, **dict)
        # 检查列名是否重复或者空值
        elementList = scnDF.iloc[0:1].values.tolist()[0]
        scnDF.columns = elementList
        self.checkColumnsIsContainsDuplicateOrNan(dataFrame=scnDF)
        scnDF.drop(axis=0, index=[0, 1], inplace=True)
        # 填充缺失项
        scnDF['DATE'].fillna(method='ffill', inplace=True)
        scnDF['TIME'].fillna(method='ffill', inplace=True)
        # 处理日期和时间列
        try:
            # scnDF['DATE'] = pd.to_datetime(scnDF['DATE'], format='%Y/%m/%d')
            scnDF['DATE'] = scnDF['DATE'].dt.strftime('%Y-%m-%d')
            # scnDF['TIME'] = pd.to_datetime(scnDF['TIME'], format='%Y-%m-%d %H:%M:%S')
            scnDF['TIME'] = scnDF['TIME'].dt.strftime('%H:%M')
        except ValueError as error:
            logger.error(error)
        # 删除空行
        scnDF.dropna(axis=0, how='all', inplace=True)
        # 过滤nan
        scnDF.fillna('', inplace=True)
        # 重建索引
        scnDF.reset_index(drop=True, inplace=True)
        return scnDF


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    scnParser = SCNParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020生物氧化表格2.xlsx'
    sheet_name = 'SCN'
    method = 'SY001'

    scnDF = scnParser.getSCNDF(filename=filename, sheet_name=sheet_name)
    increamentDF = scnParser.getIncreamentDF(srcDF=scnDF, filename=filename, sheet_name=sheet_name)
    reports = scnParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method, startEleNum=4)
    scnParser.outputReport(reports=reports)
    scnParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
