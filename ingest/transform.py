from abc import ABC, abstractmethod

class Transform(ABC):
    
    @abstractmethod
    def transform(self) -> None:
        """This transform method takes in the self.data dict of pandas.DataFrame 
        and transforms the data as the user specifies, before finally setting self.data 
        as a single pandas.DataFrame as output

        Args:
            -

        Returns:
            -

        """
        pass



