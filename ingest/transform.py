from abc import ABC, abstractmethod

class Transform(ABC):
    
    @abstractmethod
    def transform(self) -> None:
        """This transform method takes in a pandas.DataFrame or a list of 
        pandas.DataFrame and transforms the data as the user specifies,
        before returning a pandas.DataFrame as output

        Args:
            inputs: list - This is the data to be transformed

        Returns:
            a pandas.DataFrame with the transformed data

        """
        pass



