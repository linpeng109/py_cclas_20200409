import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class HBYParser(Parser):
    # 获取数据
    def getHBYDF(self, filename: str, sheet_name: str):
        dict = {'sheet_name': sheet_name, 'header': None, }
        # 读取数据
        hbyDF = pd.read_excel(io=filename, **dict)
        # 获取第一行数据
        elementList = hbyDF.iloc[0:1].values.tolist()[0]
        # 更改列名
        hbyDF.columns = elementList
        # 检查列名是否重复或者包括空值
        self.checkColumnsIsContainsDuplicateOrNan(hbyDF)
        # 填充缺失项,向下填充
        hbyDF['DATE'].fillna(method='ffill', inplace=True)
        # 删除表头
        hbyDF.drop(axis=0, index=[0, 1], inplace=True)
        # 处理日期和时间列
        try:
            hbyDF['DATE'] = pd.to_datetime(hbyDF['DATE'], format='%Y.%m.%d', errors="raise")
            hbyDF['DATE'] = hbyDF['DATE'].dt.strftime('%Y-%m-%d')
        except ValueError as error:
            self.logger.error(error)
        # 删除空行
        hbyDF.dropna(axis=0, how='all', inplace=True)
        # 过滤nan
        hbyDF.fillna('', inplace=True)
        # 重建索引
        hbyDF.reset_index(drop=True, inplace=True)
        return hbyDF


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    hbParser = HBYParser(config=config, logger=logger)

    filename = 'e:/cclasdir/2020环保.xlsx'
    sheet_name = 'HBY'
    method = 'SY001'

    hbyDF = hbParser.getHBYDF(filename=filename, sheet_name=sheet_name)
