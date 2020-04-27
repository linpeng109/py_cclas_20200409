import xlrd


class Xlrd:

    def get_sheet_names(self, filename):
        _f = xlrd.open_workbook(filename)
        sheet_names = _f.sheet_names()
        return sheet_names

    def routerParser(self, filename: str):
        router = {'HBY': 'hbyWorker',
                  'XJY': 'xjyWorker',
                  'JDY': 'jdyWorker',
                  'SCN': 'scnWorker',
                  'QTY': 'qtyWorker',
                  'SES2017': 'hcsWorker',
                  '样品测量数据': 'asfWorker',
                  'Report': 'aasWorker'}
        targets = list(router.keys())
        # print(targets)
        workers = list(router.values())
        results = []
        sheet_names = xlrd_process.get_sheet_names(filename)
        for sheet_name in sheet_names:
            if sheet_name in targets:
                results.append('%s->%s->%s' % (filename, sheet_name, router.get(sheet_name)))
        return results


if __name__ == '__main__':
    xlrd_process = Xlrd()

    filelist = [r'E:\cclasdir\20190605As-AAS01.xlsx',
                r'E:\cclasdir\20200418As-AFS01.xlsx',
                r'E:\cclasdir\20200418Hg-AFS02.xlsx',
                r'E:\cclasdir\2020swyhy.xlsx',
                r'E:\cclasdir\2020环保.xlsx',
                r'E:\cclasdir\2020生物氧化.xlsx',
                r'E:\cclasdir\2020细菌氧化.xlsx',
                r'E:\cclasdir\SES2017-HCS01.xls']
    for filename in filelist:
        results = xlrd_process.routerParser(filename=filename)
        for result in results:
            print(result)
