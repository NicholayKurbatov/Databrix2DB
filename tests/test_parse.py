import sys
sys.path.append('.')

import pytest
import requests_mock
import yaml
from utils import get_data_from_databrics
from .templates import raw_databrics_data, ready_data


# CONFIGURE
with open('config.yaml', 'r', encoding="utf-8") as file:
    config = yaml.safe_load(file)
url = config['b24_key']
method = config['export_tables']['tasks']['databrix_method']


def test_get_data_from_bitrix():   
    with requests_mock.Mocker() as mock_request:
        mock_request.get(url + method, json=raw_databrics_data[0])
        mock_request.get(url + method + "?start=2", json=raw_databrics_data[1])
        data = get_data_from_databrics(
            url=url, method=method, verbose=False
        )
    assert data == ready_data, print(data)