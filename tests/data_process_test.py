import csv
import datetime
import logging

import numpy
import pandas as pd
import pytest

from common import read_timestamp, TIME, POWER, OPERATION
from data_csv_process import pd_csv_process, csv_process

TEST_RESOURCES = 'tests/resources'
S_FILE = '01_small_file.csv'
XL_FILE = '02_extra_large_file_device_os_bench_class_1_001.csv'  # Extra large file

logger = logging.getLogger()


@pytest.mark.benchmark(
    warmup=False
)
def test_read_csv_pd_parse_infer(request, benchmark):
    # read_csv improve parameters:
    #  memory_map : bool, default False
    #  low_memory : bool, default True
    #  iterator : bool, default False
    #  chunksize : int, optional
    #  date_parser: function, optional
    #  infer_datetime_format : bool, default False
    #  parse_dates : bool or list of int or names or list of lists or dict, default False
    df = benchmark.pedantic(
        pd.read_csv,
        args=(f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}',),
        kwargs={
            'parse_dates': ['Time'],
            'infer_datetime_format': True,
            'dtype': {'Power(mWatt)': numpy.float64, 'Operation': numpy.str_}
        },
        iterations=1,
        rounds=1
    )
    logger.info(df.tail(1).to_string().split())


@pytest.mark.benchmark(
    warmup=False
)
def test_read_csv_pd_parse_infer_function(request, benchmark):
    df = benchmark.pedantic(
        pd.read_csv,
        args=(f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}',),
        kwargs={
            'parse_dates': ['Time'],
            'infer_datetime_format': True,
            'dtype': {'Power(mWatt)': numpy.float64, 'Operation': numpy.str_},
            'date_parser': read_timestamp
        },
        iterations=1,
        rounds=1
    )
    logger.info(df.tail(1).to_string().split())


def read_csv_to_pd(request):
    f = open(f'{request.config.rootdir}/{TEST_RESOURCES}/{S_FILE}', 'r+')
    reader = csv.DictReader(f)
    df = pd.DataFrame.from_dict(reader)
    return df


@pytest.mark.benchmark(
    warmup=False
)
def test_read_csv_to_pd(request, benchmark):
    df = benchmark.pedantic(
        read_csv_to_pd,
        args=(request,),
        iterations=1,
        rounds=1
    )
    logger.info(df.tail(1).to_string().split())


@pytest.mark.benchmark(
    warmup=False
)
def test_read_csv_pd_dtype(request, benchmark):
    df = benchmark.pedantic(
        pd.read_csv,
        args=(f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}',),
        kwargs={
            'dtype': {'Power(mWatt)': numpy.float64, 'Operation': numpy.str_},
        },
        iterations=1,
        rounds=1
    )
    logger.info(df.tail(1).to_string().split())


@pytest.mark.benchmark(
    warmup=False
)
def test_pd_csv_process(request, benchmark):
    expected = [
        datetime.datetime(2019, 7, 3, 9, 47, 35, 927800),
        datetime.datetime(2019, 7, 3, 9, 47, 35, 936900),
        0.052979596,
        0.0091
    ]
    results = benchmark.pedantic(
        pd_csv_process,
        args=(f'{request.config.rootdir}/{TEST_RESOURCES}/{S_FILE}',),
        iterations=1,
        rounds=1
    )
    for item_ex, item_re in zip(expected, results):
        assert item_ex == item_re


@pytest.mark.benchmark(
    warmup=False
)
def test_pd_csv_process_benchmark(request, benchmark):
    # df = pd_csv_process(f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}')
    df = benchmark.pedantic(
        pd_csv_process,
        args=(f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}',),
        iterations=1,
        rounds=1
    )
    logger.info(df)


@pytest.mark.benchmark(
    warmup=False
)
def test_csv_process_benchmark(request, benchmark):
    df = pd.read_csv(
        f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}',
        dtype={TIME: numpy.str_, POWER: numpy.float64, OPERATION: numpy.str_}
    )
    file = open(f'{request.config.rootdir}/{TEST_RESOURCES}/{XL_FILE}')
    ts_first = read_timestamp(df.values[0][0])
    ts_xs = None
    ts_xf = None
    data = {
        'time_str': [], 'time': [], 'mw': [], 'op': [], 'time_xs': [],
        'time_00': [], 'us': [], 'td_dt_00': [], 'pos_and_marks': []
    }
    # df = csv_process(data, file, ts_first, ts_xs, ts_xf)
    df = benchmark.pedantic(
        csv_process,
        args=(data, file, ts_first, ts_xs, ts_xf),
        iterations=1,
        rounds=1
    )
    file.close()
    logger.info(df)
