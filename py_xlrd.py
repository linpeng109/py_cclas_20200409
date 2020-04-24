import xlrd


class Xlrd:

    def get_sheet_names(self, filename):
        _f = xlrd.open_workbook(filename)
        sheet_names = _f.sheet_names()
        return sheet_names


if __name__ == '__main__':
    xlrd_process = Xlrd()
    sheet_names = xlrd_process.get_sheet_names(r'E:\cclasdir\2020生物氧化.xlsx')
    print(sheet_names)
    result = 'jdy' in sheet_names
    print(result)
