import requests
from tqdm.auto import tqdm
import time
from datetime import datetime


def get_data_from_databrics(url: str, method: str, verbose: bool = False) -> list[dict]:
    ##TODO: add filters params
    # get data from bitrix24
    data = []
    params = {}
    r = requests.get(url + method).json() # , params=params
    if 'result' in r.keys():
        data += r['result'].copy()
        if verbose:
            progress_bar = tqdm(total=r['total'], position=0, leave=True)
        time.sleep(0.5)
        while 'next' in r.keys():
            params['start'] = r['next']
            r = requests.get(url + method, params=params).json()
            data += r['result'].copy()
            if verbose:
                progress_bar.update(50)
            time.sleep(0.5)
    if verbose:
        progress_bar.close()
    return data


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TYPE_MAPPING = {
    'integer': lambda x: int(x),
    'string': lambda x: x,
    'datetime': lambda x: datetime.strptime(x + " 00:00:00", DATETIME_FORMAT)
}


def type_caster(v, filed_type):
    return TYPE_MAPPING[filed_type](v)
