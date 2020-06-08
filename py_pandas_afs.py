import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class AFSParser(Parser):

    def getAFSDF(self, filename: str, sheet_name: str):
        dict = {'sheet_name': sheet_name, 'header': None}
        afs_df = pd.read_excel(filename, **dict)
        afs_df.fillna('', inplace=True)
        afs_df.drop(index=[0, 1], inplace=True)
        self.logger.debug(afs_df)
        newfilename = self.getNewFilename(filename=filename, type='afs')

        encoding = self.config.get('afs', 'encoding')
        afs_df.to_csv(newfilename, index=None, header=None, encoding=encoding, line_terminator='\r\n')
        return afs_df


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    asfParser = AFSParser(config=config, logger=logger)

    # filename = 'e:/cclasdir/20200418As-AFS01.xlsx'
    filename = 'e:/cclasdir/20200418Hg-AFS02.xlsx'
    sheet_name = '样品测量数据'

    asfDF = asfParser.getAFSDF(filename=filename, sheet_name=sheet_name)
