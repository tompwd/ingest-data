import pandas

from abc import ABC, abstractmethod

class Extract(ABC):
    
    @abstractmethod
    def extract(self) -> None:
        """This Extract method loads data from a source and returns a 
        pandas dataframe to be used for further processing

        Args:
            -

        Returns:
            a pandas.DataFrame with the data extracted from source

        """
        pass

class ExtractFromParquet(Extract):
    def __init__(self) -> None:
        super().__init__()

    def extract(self, file, dataset_name) -> None:
        self.data[dataset_name] = pandas.read_parquet(file)
