from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class QTYParser(Parser):

    def getQTYDF(self, filename: str, sheet_name: str):
        # 读取数据
        dict = {'sheet_name': sheet_name, 'header': None, }
        qtyDF = pd.read_excel(io=filename, **dict)
        # 检查列名是否重复或者空值
        qtyDF = self.get_valid_dataframe(qtyDF)
        # 填充缺失项
        qtyDF['DATE'].fillna(method='ffill', inplace=True)
        # 删除表头
        qtyDF.drop(axis=0, index=[0, 1], inplace=True)
        # 处理日期和时间列
        try:
            qtyDF['DATE'] = pd.to_datetime(qtyDF['DATE'], format='%Y-%m-%d')
            qtyDF['DATE']=qtyDF['DATE'].dt.strftime('%Y-%m-%d')
            qtyDF['TIME'] = pd.to_datetime(qtyDF['TIME'], format='%H:%M:%S')
            qtyDF['TIME']=qtyDF['TIME'].dt.strftime('%H:%M')
        except ValueError as error:
            self.logger.error(error)
        # 删除空行
        qtyDF.dropna(axis=0, how='all', inplace=True)
        # 过滤nan
        qtyDF.fillna('', inplace=True)
        # 重建索引
        qtyDF.reset_index(drop=True, inplace=True)
        return qtyDF


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    qtyParser = QTYParser(logger=logger, config=config)

    filename = 'e:/cclasdir/2020swyhy.xlsx'
    sheet_name = 'QTY'
    method = 'SY001'

    qtyDF = qtyParser.getQTYDF(filename=filename, sheet_name=sheet_name)
    increamentDF = qtyParser.getIncreamentDF(srcDF=qtyDF, filename=filename, sheet_name=sheet_name)
    reports = qtyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method)
    qtyParser.outputReport(reports=reports)
    qtyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
