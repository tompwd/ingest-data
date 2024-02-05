from ingest import ExtractFromParquet, Transform, LoadToCsv

import pandas

if __name__ == '__main__':

    class TransformExecutions(Transform):
        def __init__(self) -> None:
            super().__init__()

        def transform(self) -> pandas.DataFrame:
            self.data['value'] = self.data['value']*2
            return self.data
        
    class IngestExecutionData(ExtractFromParquet, TransformExecutions, LoadToCsv):
        def __init__(self) -> None:
            super().__init__()


    ingestion = IngestExecutionData()
    ingestion.extract('data.csv')
    ingestion.transform()
    ingestion.load()
