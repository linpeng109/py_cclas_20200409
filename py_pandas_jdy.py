import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class JDYParser(Parser):

    def getJDYDF(self, filename: str, sheet_name: str):
        # 读取数据
        dict = {'sheet_name': sheet_name, 'header': None, }
        jdyDF = pd.read_excel(io=filename, **dict)

        # 检查列名是否重复或为空值
        # self.checkColumnsIsContainsDuplicateOrNan(jdyDF)
        jdyDF = self.get_valid_dataframe(jdyDF)

        # 填充空缺值
        jdyDF['DATE'].fillna(method='ffill', inplace=True)
        jdyDF['TIME'].fillna(method='ffill', inplace=True)
        # 删除表头
        jdyDF.drop(axis=0, index=[0, 1], inplace=True)
        # 处理日期和时间列
        try:
            jdyDF['DATE'] = pd.to_datetime(jdyDF['DATE'], format='%Y/%m/%d')
            jdyDF['DATE'] = jdyDF['DATE'].dt.strftime('%Y-%m-%d')
            jdyDF['TIME'] = pd.to_datetime(jdyDF['TIME'], format='%H:%M:%S')
            jdyDF['TIME'] = jdyDF['TIME'].dt.strftime('%H:%M')
        except ValueError as error:
            self.logger.error(error)

        # 删除空行
        jdyDF.dropna(axis=0, how='all', inplace=True)
        # 过滤nan
        jdyDF.fillna('', inplace=True)
        # 重新建立索引
        jdyDF.reset_index(drop=True, inplace=True)
        return jdyDF


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    jdyParser = JDYParser(config=config, logger=logger)

    filename = 'e:/cclasdir/SY/2020生物氧化BIOX Samples.xlsx'
    sheet_name = 'JDY'
    method = 'SY001'

    jdyDF = jdyParser.getJDYDF(filename=filename, sheet_name=sheet_name)
    logger.debug(jdyDF)
    increamentDF = jdyParser.getIncreamentDF(srcDF=jdyDF, filename=filename, sheet_name=sheet_name)
    reports = jdyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method)
    jdyParser.outputReport(reports=reports)
    jdyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
