"""
Base class used to generate other classes. Each class that inherits ETL will need to use the methods below
with the @abstractmethod decorator.
"""

from abc import ABC, abstractmethod


class ETL(ABC):
    @abstractmethod
    def extract_data(self):
        pass

    @abstractmethod
    def transform_data(self, data):
        pass

    @abstractmethod
    def load_data(self, data):
        pass
