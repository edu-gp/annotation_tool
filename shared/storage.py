import os
import json
from pathlib import Path

class DiskStorage:
    def __init__(self):
        self.dir = os.getcwd() + '/.storage'
        Path(self.dir).mkdir(parents=True, exist_ok=True)

    def write(self, key, value):
        assert key
        with open(f'{self.dir}/{key}', 'w') as f:
            f.write(json.dumps(value))

    def read(self, key, raise_error=False):
        try:
            with open(f'{self.dir}/{key}', 'r') as f:
                return json.loads(f.read())
        except FileNotFoundError as e:
            if raise_error:
                raise e
            else:
                return None

    def read_all(self, prefix=''):
        all_keys = os.listdir(self.dir)
        results = []
        for key in all_keys:
            if key.startswith(prefix):
                with open(f'{self.dir}/{key}', 'r') as f:
                    results.append( json.loads(f.read()) )
        return results

# TODO:
# class RedisStorage:
#     pass

if __name__ == '__main__':
    s = DiskStorage('/tmp/test_storage')

    s.write('string', 'sdfds\naf')
    assert s.read('string') == 'sdfds\naf'

    s.write('dict', {'a': 1})
    assert s.read('dict') == {'a': 1}

    s.write('num', 3.1415)
    assert s.read('num') == 3.1415

    s.write('arr', ['a','b'])
    assert s.read('arr') == ['a', 'b']

    s.write('foo', None)
    assert s.read('foo') == None

    assert s.read('does_not_exist') == None

    print("all tests passed")