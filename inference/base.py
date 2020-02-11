import abc

class ITextCatModel(abc.ABC):
    @abc.abstractmethod
    def predict(self, text_list):
        pass
