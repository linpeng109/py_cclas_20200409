# coding=gbk
import base64
import decimal
import os
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from py_config import ConfigFactory
from py_logging import LoggerFactory
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
            # df = pd.read_json(path_or_buf=filename)
        except ValueError :
            print(ValueError)
            print('++++++')
            df = DataFrame()
        return df

    # 获取有效数据
    def get_valid_dataframe(self, srcDF: DataFrame):
        # 获取列名
        _elements = srcDF.iloc[0:1].values.tolist()[0]
        # 去除列名中的空格
        # _r = []
        # for _ele in _elements:
        #     _r.append(str(_ele).strip())
        # _elements = _r
        # 列重新命名
        srcDF.columns = _elements
        # 列名去重复
        _elements = list(set(_elements))
        # 列名去除nan值项
        elements = [x for x in _elements if x == x]
        self.logger.debug(elements)
        # 有效数据切割
        srcDF = srcDF.loc[0:, elements]
        # 返回有效数据
        return srcDF

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

        self.logger.debug('===srcDF===')
        self.logger.debug(srcDF.dtypes)
        self.logger.debug(srcDF)

        # 新数据转存处理：用于比较必须转存后再取出，否则不能保证数据比较的一致性
        newFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='new')
        self.toSeries(dataFrame=srcDF, filename=newFile)
        newDF = self.fromSeries(filename=newFile)
        self.logger.debug('===newDF===')
        self.logger.debug(newDF.dtypes)
        self.logger.debug(newDF)

        # 旧数据读出
        oldFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='old')
        print("==========="+oldFile)
        oldDF = self.fromSeries(oldFile)
        self.logger.debug('===oldDF===')
        self.logger.debug(oldDF.dtypes)
        self.logger.debug(oldDF)

        # 按照新数据集合结构生成新的集合容器
        tmp_df = DataFrame(columns=newDF.columns.tolist())

        # 将旧数据集合\新数据集合都装入容器获取新旧集合的并集
        tmp_df = pd.concat([tmp_df, oldDF])

        # 填充nan值
        tmp_df.fillna('', inplace=True)

        # 删除重复：得到扩容后数据变化集合，用于比较
        tmp_df = tmp_df.drop_duplicates()

        tmp_file = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='tmp')
        self.toSeries(dataFrame=tmp_df, filename=tmp_file)
        tmp_df = self.fromSeries(filename=tmp_file)
        self.logger.debug('===tmpDF===')
        self.logger.debug(tmp_df.dtypes)
        self.logger.debug(tmp_df)

        # 将比较数据和新数据合并，删除重复部分，得到增加或改变部分
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
    def buildReport(self, dataframe: DataFrame, sheet_name: str, method: str):
        reports = []
        for row in dataframe.itertuples():
            # 数据报告
            element_report = ''

            # 非空列数
            not_null_cols_num = 0

            # 初始化构建元素
            year_month = '----'
            month_day = '----'
            hour = ''
            sample = ''
            sampleid = ''

            # 构建结构元素
            for element_name in dataframe.columns:

                # 1 获取特定格式的日期和时间值
                if element_name in ['DATE']:
                    year_month = datetime.strftime(getattr(row, element_name), '%y%m')
                    month_day = datetime.strftime(getattr(row, element_name), '%m%d')

                # 2 获取TIME，若不存在TIME字段，则以''替代
                if element_name in ['TIME']:
                    hour = '-' + str(getattr(row, 'TIME')).split(':')[0]

                # 3 获取simpleid
                if element_name in ['SAMPLEID']:
                    sampleid = '-' + str(getattr(row, element_name))

                # 4 获取SAMPLE字段
                if element_name in ['SAMPLE', 'SAMPLES']:
                    sample_handle = self.config.get('sy', 'sample_handle')
                    if sample_handle == 'plan':
                        sample = str(getattr(row, element_name))
                    elif sample_handle == 'base64':
                        sample = self.base64_encode(str(getattr(row, element_name)))
                    else:
                        sample = ''

            # 5 获取元素数据
            for element_name in dataframe.columns:
                # 检测是否是化验数据项
                if element_name in ['DATE', 'TIME', 'SAMPLEID', 'SAMPLE', 'SAMPLES']:
                    pass
                else:
                    # 去除两端空格
                    element_value = str(getattr(row, element_name))
                    element_value.strip()
                    # 只添加非空值的数据项
                    if element_value:
                        # 数据精确到小数点后4位，总数不能超过10位，包括小数点
                        try:
                            element_report = element_report + (
                                    '%-10s%-10.4f' % (element_name, decimal.Decimal(element_value)))
                        except Exception as e:
                            element_report = element_report + ('%-10s%-10s' % (element_name, element_value))
                            pass

                        not_null_cols_num = not_null_cols_num + 1

            # 如果存在有效（非空的）化验元素并且sampleid不是空则生成报告and sampleid.strip() != ''
            if not_null_cols_num > 0:
                # 输出格式化
                if sample:
                    report = ('%-20s%-10s%-20s%-40s%s%02d%s' %
                              (sheet_name + year_month,
                               method,
                               month_day + hour + sampleid,
                               sample,
                               '|',  # 在非空元素数之前，加入‘|’
                               not_null_cols_num,
                               element_report))
                else:
                    report = ('%-20s%-10s%-20s%02d%s' %
                              (sheet_name + year_month,
                               method,
                               month_day + hour + sampleid,
                               not_null_cols_num,
                               element_report))

                reports.append(report)

        return reports

    # 写出单行数据文件
    def outputReport(self, reports: list):
        self.logger.debug('===reports===')
        for i in range(len(reports)):
            outpath = self.config.get('sy', 'outputdir')
            if os.path.isdir(outpath) != True:
                os.mkdir(outpath)
            main_file_name_part_1 = Path.get_validate_filename(reports[i][0:20].replace(' ', ''))
            main_file_name_part_2 = Path.get_validate_filename(filename=reports[i][30:50].replace(' ', ''))
            full_path_filename = '%s/%s_%s_%05d.txt' % (
                outpath,
                main_file_name_part_1,
                main_file_name_part_2,
                i + 1)
            # 必须使用utf8编码，否则部分文本出现乱码，原因不明!
            with open(full_path_filename, mode='wt', encoding='gbk') as file:
                # self.logger.error('==============%s' % type(reports[i]))
                # self.logger.error('==============%s' % chardet.detect(reports[i].encode('gbk')))
                file.write(str(reports[i]))
                file.close()

            # 以下写法结果同上，必须使用utf8编码，否则部分文本出现乱码，原因不明!
            # df = pd.DataFrame({
            #     'col1': [reports[i]]
            # })
            # df.to_csv(path_or_buf=full_path_filename, encoding='utf8', index=False, header=False)

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

    # 使用base64方式汉字转换
    def base64_encode(self, input_str: str):
        input_str = input_str.encode('utf-8')
        result = base64.b64encode(input_str)
        return str(result, encoding='utf-8')

    # 获取新文件名
    def getNewFilename(self, filename, type='default'):
        newpath = self.config.get(type, 'outpath')
        Path.outpathIsExist(newpath)
        fileinfo = Path.splitFullPathFileName(filename)
        newfilename = (newpath + fileinfo.get('sep') + fileinfo.get('main') + '_OK' + '.csv')
        return newfilename


if __name__ == '__main__':
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    parser = Parser(config=config, logger=logger)
    input_str = '中文测试'
    result = parser.base64_encode(input_str)
    print((result))
