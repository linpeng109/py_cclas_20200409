import os
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_path import Path


class Parser():
    # 初始化
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config

    # 输入输出文件路径转换
    def filePathNameConverter(self, filename: str, sheet_name: str, prefix: str):
        new_path = self.config.get('json', 'outpath')
        Path.outpathIsExist(new_path)
        fileinfo = Path.splitFullPathFileName(filename)
        new_file_name = (
                new_path + fileinfo.get('sep') + prefix + '_' + '_' + fileinfo.get('main') + '.' + sheet_name)
        return new_file_name

    # 序列化
    def toSeries(self, dataFrame: DataFrame, filename: str):
        df = dataFrame.to_json(path_or_buf=filename, force_ascii=False)
        return df

    # 反序列化
    def fromSeries(self, filename: str):
        try:
            df = pd.read_json(path_or_buf=filename, encoding='gbk')
        except ValueError:
            df = DataFrame()
        return df

    # 检查列名是否重复或包含NAN
    def checkColumnsIsContainsDuplicateOrNan(self, dataFrame: DataFrame):
        columnsList = dataFrame.columns.tolist()
        colDF = DataFrame(columnsList)
        isNull = colDF.isnull().sum().sum()
        isDuplicate = colDF.duplicated().sum().sum()
        if isNull > 0:
            raise TypeError('数据转换失败：化验元素项包含空值')
        if isDuplicate > 0:
            raise TypeError('数据转换失败：化验元素项包含重复值')

    # 获取比较不同
    def getIncreamentDF(self, srcDF: DataFrame, filename: str, sheet_name: str):

        ('===srcDF===')
        # self.logger.debug(srcDF.dtypes)
        # self.logger.debug(srcDF)

        newFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='new')
        self.toSeries(dataFrame=srcDF, filename=newFile)
        newDF = self.fromSeries(filename=newFile)
        self.logger.debug('===newDF===')
        self.logger.debug(newDF.dtypes)
        self.logger.debug(newDF)

        oldFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='old')
        oldDF = self.fromSeries(oldFile)
        self.logger.debug('===oldDF===')
        self.logger.debug(oldDF.dtypes)
        self.logger.debug(oldDF)

        # 按照新数据集合结构生成集合容器
        tmp_df = DataFrame(columns=newDF.columns.tolist())
        # 将旧数据集合\新数据集合都装入容器获取新旧集合的并集
        tmp_df = pd.concat([tmp_df, oldDF])
        # 填充nan值
        tmp_df.fillna('', inplace=True)
        # 删除重复
        tmp_df.drop_duplicates(keep=False)
        self.logger.debug('===tmpDF===')
        tmp_file = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='tmp')
        self.toSeries(dataFrame=tmp_df, filename=tmp_file)
        tmp_df = self.fromSeries(filename=tmp_file)
        self.logger.debug(tmp_df.dtypes)
        self.logger.debug(tmp_df)

        increamentDF = pd.concat([tmp_df, newDF, tmp_df])
        # 数据合并后必须填充空值，否则比较有误
        increamentDF.fillna('', inplace=True)
        # 删除重复部分
        increamentDF = increamentDF.drop_duplicates(keep=False)
        self.logger.debug('===IncreamentDF===')
        self.logger.debug(increamentDF.info())
        self.logger.debug(increamentDF)

        return increamentDF

    # 生成数据报告列表
    def buildReport(self, dataframe: DataFrame, sheet_name: str, method: str, startEleNum: int):
        reports = []
        for row in dataframe.itertuples():
            # 获取特定格式的日期和时间值
            year_month = datetime.strftime(getattr(row, 'DATE'), '%y%m')
            month_day = datetime.strftime(getattr(row, 'DATE'), '%m%d')
            # 若不存在TIME字段，则以00替代
            try:
                hour = '-' + getattr(row, 'TIME').split(':')[0]
            except AttributeError:
                hour = ''
            sampleid = '-' + str(getattr(row, 'SAMPLEID'))
            # 获取列数
            colsNum = len(dataframe.columns)
            # 非空列数
            not_null_cols_num = 0
            report = ''
            for j in range(startEleNum, colsNum):
                # 只添加非空值的数据项
                if (str(row[j + 1]).replace(' ', '') != ''):
                    report = report + ('%-10s%-10.8s' % (dataframe.columns[j], row[j + 1]))
                    not_null_cols_num = not_null_cols_num + 1
            # 如果存在化验元素则生成报告
            if not_null_cols_num > 0:
                # 输出格式化
                report = ('%-20s%-10s%-20s%02d%s' %
                          (sheet_name + year_month,
                           method,
                           month_day + hour + sampleid,
                           not_null_cols_num,
                           report))
                reports.append(report)
        return reports

    # 写出单行数据文件
    def outputReport(self, reports: list):
        self.logger.debug('===reports===')
        for i in range(len(reports)):
            outpath = self.config.get('cclas', 'outputdir')
            if os.path.isdir(outpath) != True:
                os.mkdir(outpath)
            filename = '%s/%s_%s_%05d.txt' % (
                outpath,
                reports[i][0:20].replace(' ', ''),
                reports[i][30:50].replace(' ', ''),
                i + 1)
            with open(filename, 'w+') as file:
                file.write(reports[i])
                file.close()
            self.logger.debug(reports[i])

    # 处理文件
    def reportFileHandle(self, filename: str, sheet_name: str):
        oldFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='old')
        tmpFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='tmp')
        newFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='new')
        if os.path.isfile(tmpFile):
            os.remove(tmpFile)
        if os.path.isfile(oldFile):
            os.remove(oldFile)
        if os.path.isfile(newFile):
            os.rename(newFile, oldFile)
