from ingest import ExtractFromParquet, Transform, LoadToCsv

import pandas
import numpy
import time

from loguru import logger

class TransformExecutions(Transform):
    def __init__(self) -> None:
        super().__init__()

    def transform(self):
        """This transform method takes in the self.data dict of pandas.DataFrame 
        and transforms the data as the user specifies, before finally setting self.data 
        as a single pandas.DataFrame as output

        It loads 3 data sets (executions, marketdata, refdata) and combines these to 
        a final dataset 

        Args:
            -

        Returns:
            -

        """

        # Check Metrics
        executions_count = self.data['executions']['Trade_id'].nunique()
        logger.info(f'Number of unique executions loaded: {executions_count}')

        unique_venues = self.data['executions']['Venue'].nunique()
        logger.info(f'from {unique_venues} venues')

        dates = pandas.to_datetime(self.data['executions']['TradeTime']).dt.strftime('%Y/%m/%d').unique()
        logger.info(f'across these dates {dates}')

        continuous_executions_count = self.data['executions']['Trade_id'][self.data['executions']['Phase'] == 'CONTINUOUS_TRADING'].nunique()
        self.data['executions'] = self.data['executions'][self.data['executions']['Phase'] == 'CONTINUOUS_TRADING']
        logger.info(f'''Number of 'CONTINUOUS_TRADING' executions loaded: {continuous_executions_count}''')

        # clean data
        self.data['refdata']['id'] = self.data['refdata']['id'].astype(int).astype(str)
        self.data['marketdata']['listing_id'] = self.data['marketdata']['listing_id'].astype(int).astype(str)

        # enriching Data
        self.data['executions']['side'] = numpy.where(self.data['executions']['Quantity'] < 0, 2, 1)
        self.data['executions'] = pandas.merge(self.data['executions'], self.data['refdata'], how='left', left_on=['ISIN','Currency'], right_on=['ISIN','Currency'])
        self.data['marketdata'] = self.data['marketdata'].rename(columns={"best_bid_price": "best_bid", "best_ask_price": "best_ask"})
        
        # sort data
        self.data['executions'] = self.data['executions'].sort_values(by=['TradeTime'])
        self.data['marketdata'] = self.data['marketdata'].sort_values(by=['event_timestamp'])

        # add key metrics and time offsets
        self.data['executions']['TradeTime'] = pandas.to_datetime(self.data['executions']['TradeTime'])
        self.data['executions']['TradeTime_1s'] = pandas.to_datetime(self.data['executions']['TradeTime']) + pandas.Timedelta(seconds=1)
        self.data['executions']['TradeTime_min_1s'] = pandas.to_datetime(self.data['executions']['TradeTime']) - pandas.Timedelta(seconds=1)

        self.data['executions'] = pandas.merge_asof(self.data['executions'], self.data['marketdata'][['event_timestamp','best_bid', 'best_ask', 'listing_id']], left_by=['id'], right_by=['listing_id'], left_on=['TradeTime'], right_on=['event_timestamp'], tolerance=pandas.Timedelta('1s'), direction='backward')
        self.data['executions'] = pandas.merge_asof(self.data['executions'], self.data['marketdata'][['event_timestamp','best_bid', 'best_ask', 'listing_id']], left_by=['id'], right_by=['listing_id'], left_on=['TradeTime_1s'], right_on=['event_timestamp'], tolerance=pandas.Timedelta('1s'), direction='backward', suffixes=('','_1s'))
        self.data['executions'] = pandas.merge_asof(self.data['executions'], self.data['marketdata'][['event_timestamp','best_bid', 'best_ask', 'listing_id']], left_by=['id'], right_by=['listing_id'], left_on=['TradeTime_min_1s'], right_on=['event_timestamp'], tolerance=pandas.Timedelta('1s'), direction='backward', suffixes=('','_min_1s'))

        self.data['executions']['mid_price'] = (self.data['executions']['best_bid'] + self.data['executions']['best_ask'])/2
        self.data['executions']['mid_price_1s'] = (self.data['executions']['best_bid_1s'] + self.data['executions']['best_ask_1s'])/2
        self.data['executions']['mid_price_min_1s'] = (self.data['executions']['best_bid_min_1s'] + self.data['executions']['best_ask_min_1s'])/2
        self.data['executions']['slippage'] = numpy.where(self.data['executions']['side'] == 1, 
                                                                        (self.data['executions']['best_ask'] - self.data['executions']['Price'])/(self.data['executions']['best_ask'] - self.data['executions']['best_bid'])
                                                                    , (self.data['executions']['Price'] - self.data['executions']['best_bid'])/(self.data['executions']['best_ask'] - self.data['executions']['best_bid'])
                                                                    )
        
        # collate final dataset
        self.data = self.data['executions']


class IngestExecutionData(ExtractFromParquet, TransformExecutions, LoadToCsv):
    def __init__(self) -> None:
        super().__init__()
        self.data = {}


if __name__ == '__main__':

    tik = time.time()

    ingestion = IngestExecutionData()
    ingestion.extract('inputs/marketdata.parquet', 'marketdata')
    ingestion.extract('inputs/executions.parquet', 'executions')
    ingestion.extract('inputs/refdata.parquet', 'refdata')
    ingestion.transform()
    ingestion.load('outputs/outputs.csv')
    
    logger.info(f'Run Time: {round(time.time() - tik, 2)} seconds')
