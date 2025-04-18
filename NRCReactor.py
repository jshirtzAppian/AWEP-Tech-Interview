import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import argparse
import os
from sqlalchemy import text
import AWUtil as AW
import logging
import requests
logger = logging.getLogger()

#https://www.nrc.gov/reading-rm/doc-collections
#https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/2025/2025powerstatus.txt

class NRCReactorStatus():
    
    def __init__(self, backfillFrom=None, backfillTo=None):

        self.baseUrl = 'https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/'
        self.baseDir = r'D:\ScraperData\Reactor\NRC'
        self.stagingFolder = os.path.join(self.baseDir, 'staging')
        self.stgTable = 'AWCollect.dbo.NRCReactorStatusStg'
        self.stgProc = 'AWCollect.dbo.NRCReactorStatusStgUpload'
        self.filenamePrefix = 'NRCReactorStatus'
        self.fileExt = 'txt'
        if backfillFrom is None:
            self.backfillFrom = datetime.datetime.now()
        else:
            self.backfillFrom = backfillFrom
        if backfillTo is None:
            self.backfillTo = datetime.datetime.now() #+ relativedelta(years=1)
        else:
            self.backfillTo = backfillTo

        self.fromDelta = relativedelta(years=1)
        self.engine = AW.AWAlchemyEngine('AWCollect')

    def parseReport(self, filePath):

        try:
            with open(filePath, 'r') as f:
                lines = f.readlines()
            df = pd.DataFrame([line.split('|') for line in lines[1:]], columns=lines[0].strip().split('|'))
            #clean up data
            df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
            df = df.replace(r'^\s*$', None, regex=True)
            df = df.dropna(how='all')
            df['ReportDt'] = pd.to_datetime(df['ReportDt'], format='mixed')
            df = df.drop_duplicates(subset=['ReportDt', 'Unit'], keep='last')
        except Exception as e:
            logger.error(f'Error reading file {filePath}: {e}')
            return None
        
        return df


    def downloadReport(self, stagingPath, year):

        url = f'{self.baseUrl}{year}/{year}powerstatus.txt'
        r = requests.get(url)
        if r.status_code == 200:
            filePath = os.path.join(stagingPath, f'NRCReactorStatus{year}.txt')
            with open(filePath, 'wb') as f:
                f.write(r.content)
            logger.info(f'Downloaded {url} to {filePath}')
        else:
            logger.error(f'Error downloading {url}: {r.status_code}')
            return None


    def collectMore(self):
        return self.backfillFrom <= self.backfillTo
    

    def insertDataToDb(self, df):
        try:
            db = self.stgTable.split('.')[0]
            schema = self.stgTable.split('.')[1]
            tableName = self.stgTable.split('.')[-1]
            with self.engine.begin() as cnxn:
                cnxn.execute(text(f'TRUNCATE TABLE {self.stgTable}'))
            with self.engine.connect() as conn:
                df.to_sql(tableName, conn, index=False, if_exists='append', schema=f'{db}.{schema}', method=None)
            return True
        
        except Exception as e:
            logger.error(f"Error inserting data: {e}")
            return False
        

    def runProcedure(self):
        try:
            with self.engine.begin() as cnxn:
                cnxn.execute(text(f'EXEC {self.stgProc}'))
            return True
        except Exception as e:
            logger.error(f"Error running procedure: {e}")
            return False
    

    def collect(self):

        while self.collectMore():
            year = self.backfillFrom.year
            stagingPath = os.path.join(self.stagingFolder) 
            os.makedirs(stagingPath, exist_ok=True)
            self.downloadReport(stagingPath, year)
            self.backfillFrom += self.fromDelta


    def process(self):
        for filename in os.listdir(self.stagingFolder):
            filePath = os.path.join(self.stagingFolder, filename)
            if os.path.isfile(filePath):
                df = self.parseReport(filePath)
                if df is not None:
                    success = self.insertDataToDb(df) and self.runProcedure()
                    if success:
                        year = filename.replace(self.filenamePrefix, '').replace(self.fileExt, '')
                        final_path = os.path.join(f"{self.baseDir}/{year}", filename)
                        os.makedirs(os.path.dirname(final_path), exist_ok=True)
                        os.rename(filePath, final_path)
                    else:
                        error_path = os.path.join(f"{self.baseDir}/error", filename)
                        os.makedirs(os.path.dirname(error_path), exist_ok=True)
                        os.rename(filePath, error_path)


if __name__ == '__main__':
    dateFormat = '%Y-%m-%d'
    parser = argparse.ArgumentParser('NRCReactorStatus')
    parser.add_argument('-bf', '--backfillFrom', required=False, default=None, type=lambda dtStr:datetime.datetime.strptime(dtStr, dateFormat), help=dateFormat)
    parser.add_argument('-bt', '--backfillTo', required=False, default=None, type=lambda dtStr:datetime.datetime.strptime(dtStr, dateFormat), help=dateFormat)
    args = parser.parse_args()

    collector = NRCReactorStatus(args.backfillFrom, args.backfillTo)
    collector.collect()
    collector.process()