import string
import random


def drop_none_values(data):
    if hasattr(data, 'items'):
        return {
            k: v for k, v in data.items() if v is not None
        }
    return [x for x in data if x is not None]


def get_random_string(N=12, allowed_chars=(string.ascii_letters + string.digits)):
    return ''.join(random.choice(allowed_chars) for _ in range(N))
