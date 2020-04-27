import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser
from py_path import Path


class AFSParser(Parser):

    def getAFSDF(self, filename: str, sheet_name: str):
        # 获取参数
        date = Path.splitFullPathFileName(filename)['main'][0:8]
        element = Path.splitFullPathFileName(filename)['main'][8:10].upper()
        # 读取数据
        dict = {'sheet_name': sheet_name, 'header': None, }
        afsDF = pd.read_excel(io=filename, **dict)
        # 获取数据表大小
        shape = afsDF.shape
        # 删除表头
        afsDF.drop(axis=0, index=[0, 1, 2], inplace=True)
        # 插入DATE列
        col_name = afsDF.columns.tolist()
        col_name.insert(0, 'DATE')
        afsDF = afsDF.reindex(columns=col_name)
        # 截取部分数据（DATE列、第1和最后一列）
        afsDF = (afsDF.loc[0: shape[0], ['DATE', 1, shape[1] - 1]])
        # 填充日期数据
        afsDF['DATE'].fillna(date, inplace=True)
        # 处理日期和时间列
        afsDF['DATE'] = pd.to_datetime(afsDF['DATE'], format='%Y%m%d')
        # 更改列名
        afsDF = afsDF.rename(columns={1: 'SAMPLEID', shape[1] - 1: element.upper()})
        # 删除空行
        afsDF.dropna(axis=0, how='all', inplace=True)
        # 过滤nan
        afsDF.fillna('', inplace=True)
        # 重建索引
        afsDF.reset_index(drop=True, inplace=True)
        return afsDF


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    asfParser = AFSParser(config=config, logger=logger)

    # filename = 'e:/cclasdir/20200418As-AFS01.xlsx'
    filename = 'e:/cclasdir/20200418Hg-AFS02.xlsx'
    sheet_name = '样品测量数据'
    method = 'AFS02'

    asfDF = asfParser.getAFSDF(filename=filename, sheet_name=sheet_name)
    reports = asfParser.buildReport(dataframe=asfDF, sheet_name='ASF', method=method, startEleNum=2)
    asfParser.outputReport(reports=reports)
