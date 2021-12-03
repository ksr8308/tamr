from abc import ABC, abstractmethod


class DataSampler(ABC):

    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_sampled_data(self):
        pass
