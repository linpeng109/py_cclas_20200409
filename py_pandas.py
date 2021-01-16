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
    # ��ʼ��
    def __init__(self, config, logger):
        self.logger = logger
        self.config = config

    # ��������ļ�·��ת��
    def filePathNameConverter(self, filename: str, sheet_name: str, prefix: str):
        new_path = self.config.get('json', 'outpath')
        Path.outpathIsExist(new_path)
        fileinfo = Path.splitFullPathFileName(filename)
        new_file_name = (
                new_path + fileinfo.get('sep') + prefix + '_' + '_' + fileinfo.get('main') + '.' + sheet_name)
        return new_file_name

    # ���л�
    def toSeries(self, dataFrame: DataFrame, filename: str):
        df = dataFrame.to_json(path_or_buf=filename, force_ascii=False)
        return df

    # �����л�
    def fromSeries(self, filename: str):
        try:
            df = pd.read_json(path_or_buf=filename, encoding='gbk')
            # df = pd.read_json(path_or_buf=filename)
        except ValueError :
            print(ValueError)
            print('++++++')
            df = DataFrame()
        return df

    # ��ȡ��Ч����
    def get_valid_dataframe(self, srcDF: DataFrame):
        # ��ȡ����
        _elements = srcDF.iloc[0:1].values.tolist()[0]
        # ȥ�������еĿո�
        # _r = []
        # for _ele in _elements:
        #     _r.append(str(_ele).strip())
        # _elements = _r
        # ����������
        srcDF.columns = _elements
        # ����ȥ�ظ�
        _elements = list(set(_elements))
        # ����ȥ��nanֵ��
        elements = [x for x in _elements if x == x]
        self.logger.debug(elements)
        # ��Ч�����и�
        srcDF = srcDF.loc[0:, elements]
        # ������Ч����
        return srcDF

    # ��������Ƿ��ظ������NAN
    def checkColumnsIsContainsDuplicateOrNan(self, dataFrame: DataFrame):
        columnsList = dataFrame.columns.tolist()
        colDF = DataFrame(columnsList)
        isNull = colDF.isnull().sum().sum()
        isDuplicate = colDF.duplicated().sum().sum()
        if isNull > 0:
            raise TypeError('����ת��ʧ�ܣ�����Ԫ���������ֵ')
        if isDuplicate > 0:
            raise TypeError('����ת��ʧ�ܣ�����Ԫ��������ظ�ֵ')

    # ��ȡ�Ƚϲ�ͬ
    def getIncreamentDF(self, srcDF: DataFrame, filename: str, sheet_name: str):

        self.logger.debug('===srcDF===')
        self.logger.debug(srcDF.dtypes)
        self.logger.debug(srcDF)

        # ������ת�洦�����ڱȽϱ���ת�����ȡ���������ܱ�֤���ݱȽϵ�һ����
        newFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='new')
        self.toSeries(dataFrame=srcDF, filename=newFile)
        newDF = self.fromSeries(filename=newFile)
        self.logger.debug('===newDF===')
        self.logger.debug(newDF.dtypes)
        self.logger.debug(newDF)

        # �����ݶ���
        oldFile = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='old')
        print("==========="+oldFile)
        oldDF = self.fromSeries(oldFile)
        self.logger.debug('===oldDF===')
        self.logger.debug(oldDF.dtypes)
        self.logger.debug(oldDF)

        # ���������ݼ��Ͻṹ�����µļ�������
        tmp_df = DataFrame(columns=newDF.columns.tolist())

        # �������ݼ���\�����ݼ��϶�װ��������ȡ�¾ɼ��ϵĲ���
        tmp_df = pd.concat([tmp_df, oldDF])

        # ���nanֵ
        tmp_df.fillna('', inplace=True)

        # ɾ���ظ����õ����ݺ����ݱ仯���ϣ����ڱȽ�
        tmp_df = tmp_df.drop_duplicates()

        tmp_file = self.filePathNameConverter(filename=filename, sheet_name=sheet_name, prefix='tmp')
        self.toSeries(dataFrame=tmp_df, filename=tmp_file)
        tmp_df = self.fromSeries(filename=tmp_file)
        self.logger.debug('===tmpDF===')
        self.logger.debug(tmp_df.dtypes)
        self.logger.debug(tmp_df)

        # ���Ƚ����ݺ������ݺϲ���ɾ���ظ����֣��õ����ӻ�ı䲿��
        increamentDF = pd.concat([tmp_df, newDF, tmp_df])
        # ���ݺϲ����������ֵ������Ƚ�����
        increamentDF.fillna('', inplace=True)
        # ɾ���ظ�����
        increamentDF = increamentDF.drop_duplicates(keep=False)
        self.logger.debug('===IncreamentDF===')
        self.logger.debug(increamentDF.info())
        self.logger.debug(increamentDF)

        return increamentDF

    # �������ݱ����б�
    def buildReport(self, dataframe: DataFrame, sheet_name: str, method: str):
        reports = []
        for row in dataframe.itertuples():
            # ���ݱ���
            element_report = ''

            # �ǿ�����
            not_null_cols_num = 0

            # ��ʼ������Ԫ��
            year_month = '----'
            month_day = '----'
            hour = ''
            sample = ''
            sampleid = ''

            # �����ṹԪ��
            for element_name in dataframe.columns:

                # 1 ��ȡ�ض���ʽ�����ں�ʱ��ֵ
                if element_name in ['DATE']:
                    year_month = datetime.strftime(getattr(row, element_name), '%y%m')
                    month_day = datetime.strftime(getattr(row, element_name), '%m%d')

                # 2 ��ȡTIME����������TIME�ֶΣ�����''���
                if element_name in ['TIME']:
                    hour = '-' + str(getattr(row, 'TIME')).split(':')[0]

                # 3 ��ȡsimpleid
                if element_name in ['SAMPLEID']:
                    sampleid = '-' + str(getattr(row, element_name))

                # 4 ��ȡSAMPLE�ֶ�
                if element_name in ['SAMPLE', 'SAMPLES']:
                    sample_handle = self.config.get('sy', 'sample_handle')
                    if sample_handle == 'plan':
                        sample = str(getattr(row, element_name))
                    elif sample_handle == 'base64':
                        sample = self.base64_encode(str(getattr(row, element_name)))
                    else:
                        sample = ''

            # 5 ��ȡԪ������
            for element_name in dataframe.columns:
                # ����Ƿ��ǻ���������
                if element_name in ['DATE', 'TIME', 'SAMPLEID', 'SAMPLE', 'SAMPLES']:
                    pass
                else:
                    # ȥ�����˿ո�
                    element_value = str(getattr(row, element_name))
                    element_value.strip()
                    # ֻ��ӷǿ�ֵ��������
                    if element_value:
                        # ���ݾ�ȷ��С�����4λ���������ܳ���10λ������С����
                        try:
                            element_report = element_report + (
                                    '%-10s%-10.4f' % (element_name, decimal.Decimal(element_value)))
                        except Exception as e:
                            element_report = element_report + ('%-10s%-10s' % (element_name, element_value))
                            pass

                        not_null_cols_num = not_null_cols_num + 1

            # ���������Ч���ǿյģ�����Ԫ�ز���sampleid���ǿ������ɱ���and sampleid.strip() != ''
            if not_null_cols_num > 0:
                # �����ʽ��
                if sample:
                    report = ('%-20s%-10s%-20s%-40s%s%02d%s' %
                              (sheet_name + year_month,
                               method,
                               month_day + hour + sampleid,
                               sample,
                               '|',  # �ڷǿ�Ԫ����֮ǰ�����롮|��
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

    # д�����������ļ�
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
            # ����ʹ��utf8���룬���򲿷��ı��������룬ԭ����!
            with open(full_path_filename, mode='wt', encoding='gbk') as file:
                # self.logger.error('==============%s' % type(reports[i]))
                # self.logger.error('==============%s' % chardet.detect(reports[i].encode('gbk')))
                file.write(str(reports[i]))
                file.close()

            # ����д�����ͬ�ϣ�����ʹ��utf8���룬���򲿷��ı��������룬ԭ����!
            # df = pd.DataFrame({
            #     'col1': [reports[i]]
            # })
            # df.to_csv(path_or_buf=full_path_filename, encoding='utf8', index=False, header=False)

            self.logger.debug(reports[i])

    # �����ļ�
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

    # ʹ��base64��ʽ����ת��
    def base64_encode(self, input_str: str):
        input_str = input_str.encode('utf-8')
        result = base64.b64encode(input_str)
        return str(result, encoding='utf-8')

    # ��ȡ���ļ���
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
    input_str = '���Ĳ���'
    result = parser.base64_encode(input_str)
    print((result))
