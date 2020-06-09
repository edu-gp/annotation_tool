import os
import re
import hashlib
import json
import ijson
from ijson.common import ObjectBuilder
from tqdm import tqdm


def raw_companies_iterator(json_fname):
    """
    This is a slow Pitchbook json parser that reads with a stream.

    This is based on ijson/common.py:item
    https://github.com/isagalaev/ijson/blob/e252a50db34b71cc2b5e0b9a77cd76dee8e95005/ijson/common.py#L130
    """
    with open(json_fname, 'r') as f:
        prefixed_events = ijson.parse(f)

        prefix = None

        try:
            while True:
            #for _ in range(10): # debug
                current, event, value = next(prefixed_events)
                #print("PREFIX: {} |".format(prefix), current, event, value) # debug

                # Our custom json format.
                #   '32.item.company_id' -> '32'
                #   '33' -> '32'
                res = re.match('^(\d+).*$', current)
                if res:
                    key = res.groups()[0]
                    if prefix is None:
                        # Assign prefix to be the company key.
                        prefix = key
                    elif key == prefix:
                        pass
                    else:
                        # Note: This might happen if a company with id 11 is followed with id 110
                        #       since they would have the same prefix. Am I right?
                        raise Exception("Prefix != Key, but object didn't finish parsing yet.")

                if current == prefix:
                    if event in ('start_map', 'start_array'):
                        builder = ObjectBuilder()
                        end_event = event.replace('start', 'end')
                        while (current, event) != (prefix, end_event):
                            builder.event(event, value)
                            current, event, value = next(prefixed_events)
                            #print("PREFIX: {} |".format(prefix), current, event, value) # debug
                        yield builder.value
                        prefix = None
                    else:
                        pass
        except StopIteration:
            pass


# --- This cache business is an optimization and optional --

def get_cache_dir(json_fname, cache_dir):
    # We could get the hash of the entire file, but it's fairly slow (6s).
    # Might as well just use the file name, it's probably unique.
    hash_md5 = hashlib.md5()
    hash_md5.update(json_fname.encode())
    return f"{cache_dir}/{hash_md5.hexdigest()}"

def cache_is_ready(cache_dir):
    return os.path.exists(f"{cache_dir}/.done")

def warm_cache(json_fname, cache_dir):
    if not cache_is_ready(cache_dir):
        print("Warming cache... this take ~15 mintues")

        os.makedirs(cache_dir, exist_ok=True)

        num_companies = 0

        # Read and write to cache.
        for company in tqdm(raw_companies_iterator(json_fname)):
            # Sometimes there are empty snapshots, even if company id exists. eg. "2": []
            if len(company) > 0:
                company_id = company[0]['company_id']
                with open(f'{cache_dir}/{company_id}.json', 'w') as f:
                    f.write(json.dumps(company))
                    num_companies += 1

        # Write ".done" flag to dir.
        with open(f'{cache_dir}/.done', 'w') as f:
            f.write('')

        # Write ".num_companies" to dir.
        with open(f'{cache_dir}/.num_companies', 'w') as f:
            f.write(str(num_companies))

    with open(f'{cache_dir}/.num_companies', 'r') as f:
        num_companies = int(f.read())

    return num_companies

def cached_companies_iterator(cache_dir):
    cached_files = os.listdir(cache_dir)
    cached_files = [f'{cache_dir}/{f}' for f in cached_files if f[0] != '.']

    for fname in cached_files:
        with open(fname, 'r') as f:
            data = json.loads(f.read())
            yield data


class PitchbookData:
    def __init__(self, json_fname, base_cache_dir=None):
        """
        json_fname is a file with the format:
            {
                "3": [...]
            }
        Where "3" is the company id, and it points to a list of snapshots.

        Inputs:
            json_fname: The json from Diego.
            cache_dir: If not None, we will cache the parsed data (for faster
                access next time)
        """
        self.json_fname = json_fname
        self.cache_dir = get_cache_dir(json_fname, base_cache_dir)

        if self.cache_dir is not None:
            # Note we need to warm up the entire cache as a one-time pass since
            #we don't know how many companies there are or their ids.
            self.num_companies = warm_cache(self.json_fname, self.cache_dir)

    def size(self):
        return self.num_companies

    def companies_iterator(self):
        """
        Note: Order of companies not guaranteed.
        """
        if cache_is_ready(self.cache_dir):
            for x in cached_companies_iterator(self.cache_dir):
                yield x
        else:
            for x in raw_companies_iterator(self.json_fname):
                yield x

    @classmethod
    def get_snapshots_id(cls, snapshots):
        """
        Returns the unique id of a list of snapshots, assuming the snapshots
        are already sorted in time.
        """
        unique_id = f"{snapshots[0]['company_id']}:{len(snapshots)}"
        return unique_id
