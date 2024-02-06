import pandas

from abc import ABC, abstractmethod

class Load(ABC):
    
    @abstractmethod
    def load(self):
        """This load method takes in the self.data pandas.DataFrame and 
        loads data into a destination

        Args:
            - 

        Returns:
            -

        """
        pass

class LoadToCsv(Load):
    def __init__(self) -> None:
        super().__init__()

    def load(self, filename):
        """This load method takes in the self.data pandas.DataFrame and 
        loads data into a destination

        Args:
            - filename : filename for the csv which is written, 
                         this can include the path 
            
        Returns:
            -

        """
        self.data.to_csv(filename, index=False)
