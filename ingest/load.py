import pandas

from abc import ABC, abstractmethod

class Load(ABC):
    
    @abstractmethod
    def load(self, data: pandas.DataFrame):
        """This load method takes in a pandas.DataFrame and 
        loads data into a destination

        Args:
            data: pandas.DataFrame - This is the data to be loaded

        Returns:
            -

        """
        pass

class LoadToCsv(Load):
    def __init__(self) -> None:
        super().__init__()

    def load(self, filename):
        self.data.to_csv(filename, index=False)
