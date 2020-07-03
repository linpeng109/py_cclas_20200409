import pandas as pd

from py_config import ConfigFactory
from py_logging import LoggerFactory
from py_pandas import Parser


class AASParser(Parser):

    def getAASDF(self, filename: str, sheet_name):
        dict = {'sheet_name': sheet_name, 'header': None}
        aas_df = pd.read_excel(filename, **dict)
        # 过滤
        aas_df[0] = aas_df[0].astype(str)
        # aas_df = aas_df[aas_df[0].str.contains('UNK-|REP-|STD-')]
        contain_filter = self.config.get('aas', 'filter')
        aas_df = aas_df[aas_df[0].str.contains(contain_filter)]
        aas_df.fillna('', inplace=True)
        self.logger.debug(aas_df)
        newfilename = self.getNewFilename(filename=filename, type='aas')
        encoding = self.config.get('aas', 'encoding')
        aas_df.to_csv(newfilename, index=None, header=None, encoding=encoding, line_terminator='\r\n')
        return aas_df


if __name__ == '__main__':
    # 初始化
    config = ConfigFactory(config_file_name='py_cclas.ini').getConfig()
    logger = LoggerFactory(config=config).getLogger()
    aasParser = AASParser(config=config, logger=logger)

    filename = 'e:/cclasdir/20190605As-AAS01.xlsx'
    sheet_name = 0

    asfDF = aasParser.getAASDF(filename=filename, sheet_name=sheet_name)
