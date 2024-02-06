import pandas

from abc import ABC, abstractmethod

class Extract(ABC):
    
    @abstractmethod
    def extract(self) -> None:
        """This Extract method loads data from a source and assigns a 
        pandas dataframe to be used for further processing in the self.data dictionary

        Args:
            -

        Returns:
            -

        """
        pass

class ExtractFromParquet(Extract):
    def __init__(self) -> None:
        super().__init__()

    def extract(self, file: str, dataset_name: str) -> None:
        """This Extract method loads data from a source and assigns a 
        pandas dataframe to be used for further processing in the self.data dictionary

        Args:
            - file : name of the .parquet file to be loaded
            - dataset_name : key of the dataset in the self.data dictionary

        Returns:
            -

        """
        self.data[dataset_name] = pandas.read_parquet(file)
