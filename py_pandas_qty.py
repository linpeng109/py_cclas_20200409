import pandas as pd

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

        # 删除表头
        qtyDF.drop(axis=0, index=[0, 1], inplace=True)

        # 策略一：填充DATE中的缺失项
        # qtyDF['DATE'].fillna(method='ffill', inplace=True)

        # 处理日期和时间列
        try:
            qtyDF['DATE'] = pd.to_datetime(qtyDF['DATE'], errors='coerce', format='%Y-%m-%d')
            qtyDF['DATE'] = qtyDF['DATE'].dt.strftime('%Y-%m-%d')
            qtyDF['TIME'] = pd.to_datetime(qtyDF['TIME'], errors='coerce', format='%H:%M:%S')
            qtyDF['TIME'] = qtyDF['TIME'].dt.strftime('%H:%M')
        except ValueError as error:
            self.logger.error(error)

        # 策略一：删除DATE为空的行（数据不上传）
        qtyDF = qtyDF[qtyDF['DATE'].notna()]
        # 策略二：删除TIME为空的行（数据不上传）
        qtyDF = qtyDF[qtyDF['TIME'].notna()]
        # 策略三：删除SAMPLE为空的行（数据不上传）
        qtyDF = qtyDF[qtyDF['SAMPLE'].notna()]
        # 策略四：删除SAMPLEID为空的行（数据不上传）
        qtyDF = qtyDF[qtyDF['SAMPLEID'].notna()]

        # 将SAMPLEID字段转换为字符串
        # qtyDF['SAMPLEID'] = qtyDF['SAMPLEID'].apply(str)
        # qtyDF['SAMPLEID'] = qtyDF['SAMPLEID'].apply(lambda x: x.split('.')[0])

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

    filename = 'E:/cclasdir/SY/test2.xlsx'
    sheet_name = 'QTY'
    method = 'SY001'

    qtyDF = qtyParser.getQTYDF(filename=filename, sheet_name=sheet_name)
    print(qtyDF['SAMPLEID'])
    # increamentDF = qtyParser.getIncreamentDF(srcDF=qtyDF, filename=filename, sheet_name=sheet_name)
    # reports = qtyParser.buildReport(dataframe=increamentDF, sheet_name=sheet_name, method=method)
    # qtyParser.outputReport(reports=reports)
    # qtyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)
