import abc

class ITextCatModel(abc.ABC):
    @abc.abstractmethod
    def predict(self, text_list):
        '''
        Inputs:
            text_list: A list of strings.

        Returns a list of dicts, where each dict must contain the key 'score'
        with a float value.

            e.g.
                [ {'score': 0.324} , {'score': 3.434}, ... ]
        '''
        pass
