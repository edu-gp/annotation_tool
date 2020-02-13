import hashlib

def get_ar_id(fname, line_number):
    ar_id = f'{fname}:{line_number}'
    ar_id = hashlib.sha224(ar_id.encode()).hexdigest()
    return ar_id