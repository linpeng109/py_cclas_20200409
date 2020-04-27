import multiprocessing
import time
import winsound
from datetime import datetime

import xlrd
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver as Observer

from py_pandas_afs import AFSParser
from py_pandas_hby import HBYParser
from py_pandas_hcs import HCSParser
from py_pandas_jdy import JDYParser
from py_pandas_qty import QTYParser
from py_pandas_scn import SCNParser
from py_pandas_xjy import XJYParser
from py_path import Path


class WatchDogObServer():

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.jdyParser = JDYParser(config=config, logger=logger)
        self.scnParser = SCNParser(config=config, logger=logger)
        self.hbyParser = HBYParser(config=config, logger=logger)
        self.qtyParser = QTYParser(config=config, logger=logger)
        self.xjyParser = XJYParser(config=config, logger=logger)
        self.afsParser = AFSParser(config=config, logger=logger)
        self.hcsParser = HCSParser(config=config, logger=logger)

    def on_modified(self, event):
        self.logger.debug(event)

    def on_any_event(self, event):
        self.logger.debug(event)

    # 当创建文件时触发
    def on_created(self, event):

        filename = event.src_path
        _f = xlrd.open_workbook(filename)
        sheet_names = _f.sheet_names()
        sheetName2Worker = {'HBY': 'hbyWorker',
                            'XJY': 'xjyWorker',
                            'JDY': 'jdyWorker',
                            'SCN': 'scnWorker',
                            'QTY': 'qtyWorker',
                            'SES2017': 'hcsWorker',
                            '样品测量数据': 'asfWorker',
                            'Report': 'aasWorker'}
        targets = list(sheetName2Worker.keys())
        workers = []
        for sheet_name in sheet_names:
            if sheet_name in targets:
                workers.append('%s' % (sheetName2Worker.get(sheet_name)))

        winsound.Beep(500,800)

        for worker in workers:
            multiprocessing.Process(target=eval('self.' + worker)(event))

    def scnWorker(self, event):
        filename = event.src_path
        sheet_name = 'SCN'
        method = 'SY001'

        scnDF = self.scnParser.getSCNDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.scnParser.getIncreamentDF(srcDF=scnDF, filename=filename, sheet_name=sheet_name)
        reports = self.scnParser.buildReport(dataframe=increamentDF, sheet_name='SCN', method=method,
                                             startEleNum=4)
        self.scnParser.outputReport(reports=reports)
        self.scnParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def jdyWorker(self, event):
        filename = event.src_path
        sheet_name = 'JDY'
        method = 'SY001'

        jdyDF = self.jdyParser.getJDYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.jdyParser.getIncreamentDF(srcDF=jdyDF, filename=filename, sheet_name=sheet_name)
        reports = self.jdyParser.buildReport(dataframe=increamentDF, sheet_name='JDY', method=method,
                                             startEleNum=4)
        self.jdyParser.outputReport(reports=reports)
        self.jdyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def hbyWorker(self, event):
        filename = event.src_path
        sheet_name = 'HBY'
        method = 'SY001'

        hbyDF = self.hbyParser.getHBYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.hbyParser.getIncreamentDF(srcDF=hbyDF, filename=filename, sheet_name=sheet_name)
        reports = self.hbyParser.buildReport(dataframe=increamentDF, sheet_name='HBY', method=method,
                                             startEleNum=3)
        self.hbyParser.outputReport(reports=reports)
        self.hbyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def qtyWorker(self, event):
        filename = event.src_path
        sheet_name = 'QTY'
        method = 'SY001'

        qtyDF = self.qtyParser.getQTYDF(filename=filename, sheet_name=sheet_name)
        increamentDF = self.qtyParser.getIncreamentDF(srcDF=qtyDF, filename=filename, sheet_name=sheet_name)
        reports = self.qtyParser.buildReport(dataframe=increamentDF, sheet_name='QTY', method=method,
                                             startEleNum=4)
        self.qtyParser.outputReport(reports=reports)
        self.qtyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def xjyWorker(self, event):
        filename = event.src_path
        # 取得当前月份01、02……
        sheet_list = []
        current_month = datetime.today().month
        sheet_list.append('%02d' % current_month)
        if current_month > 1:
            last_month = current_month - 1
        sheet_list.append('%02d' % last_month)
        method = 'SY001'
        for sheet_name in sheet_list:
            xjyDF = self.xjyParser.getXJYDF(filename=filename, sheet_name=sheet_name)
            increamentDF = self.xjyParser.getIncreamentDF(srcDF=xjyDF, filename=filename, sheet_name=sheet_name)
            reports = self.xjyParser.buildReport(dataframe=increamentDF, sheet_name='XJY', method=method,
                                                 startEleNum=4)
            self.xjyParser.outputReport(reports=reports)
            self.xjyParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def afsWorker(self, event):
        filename = event.src_path
        sheet_name = '样品测量数据'
        # 从文件名中获取method
        method = Path.splitFullPathFileName(filename)['main'][11:]
        asfDF = self.asfParser.getAFSDF(filename=filename, sheet_name=sheet_name)
        reports = self.asfParser.buildReport(dataframe=asfDF, sheet_name='ASF', method=method, startEleNum=2)
        self.asfParser.outputReport(reports=reports)
        self.afsParser.reportFileHandle(filename=filename, sheet_name=sheet_name)

    def hcsWorker(self, hcsExcelFileName):
        dict = {'sheet_name': 0, 'header': None}
        hcsDf = pd.read_excel(hcsExcelFileName, **dict)
        # 删除表标题
        hcsDf.drop(index=[0, 1], inplace=True)
        # 按照0列排列升序
        hcsDf.sort_index(0, ascending=False, inplace=True)
        hcsDf.fillna('', inplace=True)
        # 删除空列
        hcsDf.dropna(axis=1, how='any', inplace=True)
        self.logger.debug(hcsDf)
        # newfilename = self.__getNewFilename(filename=hcsExcelFileName, type='hcs')
        # encoding = self.config.get('hcs', 'encoding')
        # hcsDf.to_csv(newfilename, index=None, header=None, encoding=encoding, line_terminator='\r\n')
        return hcsDf

    def aaslWorker(self, aasExcelFilename):
        dict = {'sheet_name': 0, 'header': None}
        aasDf = pd.read_excel(aasExcelFilename, **dict)
        # 删除表头
        aasDf.drop(axis=0, index=[0], inplace=True)
        aasDf.fillna('', inplace=True)
        self.logger.debug(aasDf)
        # newfilename = self.__getNewFilename(filename=aasExcelFilename, type='aas')
        # encoding = self.config.get('aas', 'encoding')
        # aasDf.to_csv(newfilename, index=None, header=None, encoding=encoding, line_terminator='\r\n')
        return aasDf

    def start(self):
        path = self.config.get('watchdog', 'path')
        patterns = self.config.get('watchdog', 'patterns').split(';')
        ignore_directories = self.config.getboolean('watchdog', 'ignore_directories')
        ignore_patterns = self.config.get('watchdog', 'ignore_patterns').split(';')
        case_sensitive = self.config.getboolean('watchdog', 'case_sensitive')
        recursive = self.config.getboolean('watchdog', 'recursive')

        event_handler = PatternMatchingEventHandler(patterns=patterns,
                                                    ignore_patterns=ignore_patterns,
                                                    ignore_directories=ignore_directories,
                                                    case_sensitive=case_sensitive)
        event_handler.on_created = self.on_created

        observer = Observer()
        observer.schedule(path=path, event_handler=event_handler, recursive=recursive)

        observer.start()

        self.logger.info('WatchDog Observer for CCLAS is startting.....')
        self.logger.info('patterns=%s' % patterns)
        self.logger.info('path=%s' % path)
        self.logger.info('delay=%s' % str(self.config.getfloat('watchdog', 'delay')))
        # self.logger.info('beep=%s' % str(self.config.getboolean('watchdog', 'beep')))
        try:
            while observer.is_alive():
                time.sleep(self.config.getfloat('watchdog', 'delay'))
        except KeyboardInterrupt:
            observer.stop()
            self.logger.info('WatchDog Observer is stoped.')
        observer.join()
