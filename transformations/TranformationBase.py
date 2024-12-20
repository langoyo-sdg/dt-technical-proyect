import logging
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class TransformationBase(ABC):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__) 
    
    @abstractmethod
    def apply(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Abstract method to apply a transformation.
        """
        pass