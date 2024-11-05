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
