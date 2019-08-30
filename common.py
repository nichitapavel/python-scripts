import csv
import datetime
import logging
import multiprocessing
import os
import sys
from optparse import OptionParser

import psutil
from flyingcircus.base import readline

from custom_exceptions import UnsupportedNumberOfCores

logging.basicConfig(
    level=logging.INFO,
    format='[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
    datefmt='%Y/%m/%d-%H:%M:%S'
)


class IDs:
    DEVICE = 'device'
    TYPE = 'type'
    OS = 'os'
    BENCH = 'benchmark'
    SIZE = 'size'
    THREADS = 'threads'
    ITERATION = 'iteration'
    TIME = 'time'
    TIME_NPB = 'time_npb'
    ENERGY = 'joules'
    MOPS = 'mops'
    CSV = 'csv'
    LOG = 'log'


ResultItems = [IDs.TIME, IDs.TIME_NPB, IDs.ENERGY, IDs.MOPS]
DataFilterItems = [IDs.TYPE, IDs.DEVICE, IDs.OS, IDs.BENCH, IDs.SIZE, IDs.THREADS]

# Pandas DTYPE to interpret columns as string, needed for merging metrics and processed files
PD_DTYPE = {IDs.TIME_NPB: str, IDs.MOPS: str, IDs.ENERGY: str, IDs.TIME: str}

# Devices used in testing
HIKEY970 = 'hikey970'
ODROIDXU4_A = 'odroidxu4a'
ODROIDXU4_B = 'odroidxu4b'
ROCK960 = 'rock960'

# Expected datetime formats
TS_LONG_FORMAT = '%Y/%m/%d-%H:%M:%S.%f'
TS_FORMAT = [
    TS_LONG_FORMAT,
    '%H:%M:%S.%f',
    '%H:%M:%S',
]

# CSV header names
CSV_TIME = 'Time'
CSV_POWER = 'Power(mWatt)'
CSV_OP = 'Operation'

# Cores
CORES = multiprocessing.cpu_count()


# Files naming scheme:
# [<type>_]XXXXXXXXX_<device>_<os>_<bench>_<class>_<threads>[_<iteration>].<suffix>
# Where XXXXXXXXX is whatever, it must not contain '_' char
# <type> is the app mode, currently 'release' or 'debug'
# <device> is the name of the device, example: 'hikey970', 'odroidxu4a'...
# <os> is the operating system, either 'android' or 'linux'
# <bench> is the name of npb benchmark, usually has a length of 2 chars, examples: 'is', 'fe'...
# <class> is the size of the benchmark, has a length of 1 char, examples: 's', 'w', 'c'...
# <iteration> is used for energy data files (csv's) and denotes the number of iteration that was running
# <suffix> is the format of the file, usually '.log' and '.csv'

def csv_name_parsing(filename):
    csv_row = {}
    parts = filename.split('.')

    if parts[-1] == 'csv':
        parts_csv = parts[-2].split('_')
        csv_row = {}
        try:
            csv_row.update({'type': parts_csv[-8]})
        except IndexError:
            csv_row.update({'type': 'default'})
            pass
        csv_row.update({
            'device': parts_csv[-6],
            'os': parts_csv[-5],
            'benchmark': parts_csv[-4],
            'size': parts_csv[-3],
            'threads': parts_csv[-2],
            'iteration': parts_csv[-1]
        })
    elif parts[-1] == 'log':
        parts_log = parts[-2].split('_')
        csv_row = {}
        try:
            csv_row.update({'type': parts_log[-7]})
        except IndexError:
            csv_row.update({'type': 'default'})
            pass
        csv_row.update({
            'device': parts_log[-5],
            'os': parts_log[-4],
            'benchmark': parts_log[-3],
            'size': parts_log[-2],
            'threads': parts_log[-1],
        })
    return csv_row


def read_timestamp(timestamp):
    for item in TS_FORMAT:
        try:
            ts = datetime.datetime.strptime(timestamp, item)
            return ts
        except ValueError:
            pass
    return None


def is_valid_last_row(rows):
    if rows[-2][CSV_OP] is None or \
            len(rows[-2][CSV_TIME]) != len(rows[-2][CSV_TIME]) or \
            rows[-1][CSV_POWER] is None or \
            len(rows[-2][CSV_POWER]) != len(rows[-2][CSV_POWER]):
        return False
    return True


def check_last_row(file, logger):
    rows = []  # Save the last 2 rows here

    # Only iterate 2 times to get the last 2 rows
    for row in readline(file, reverse=True):
        if len(rows) < 2:
            rows.append(row)
        else:
            break
    if len(rows) < 2:
        logger.warning(f'[{file.name}][Possibly empty]')
    else:
        if len(rows[0]) != len(rows[1]):  # Is the last row valid?
            file.seek(file.tell() - len(rows[0]))
            file.truncate()
    file.seek(0)  # Set the current position in file at beginning


def first_timestamp(file):
    # TODO a line can contain NULL byte, this script does not control this use case
    file.readline()
    ts_first = read_timestamp(file.readline().split(',')[0])
    file.seek(0)  # Set the current position in file at beginning
    return ts_first


def set_cores(req_cores):
    """
    Function to set the number of cores to be used. If the requested number of cores exceeds what the
    system has an exception is raised, otherwise will set requested number of cores.
    In case the number of cores is not specified it will be set with these requirements:
    1. for systems with more than 4 cores, always will leave 2 cores for os and other computations, ie. a system
      with 16 cores will use a maximum 14 cores for your task.
    2. for system with less or equal to 4 cores all available cores will be used.
    :param req_cores: integer with the requested cores set by argument '-c/--cores'
    :return: number of cores to be used
    """
    sys_cores = os.cpu_count()
    if req_cores is None:
        if sys_cores > 4:
            return sys_cores - 2
        else:
            return sys_cores
    if req_cores > sys_cores or req_cores <= 0:
        raise UnsupportedNumberOfCores(
            f'Requested cores \'{req_cores}\' is not supported, available cores: {sys_cores}'
        )
    return req_cores


def log_to_file(filename):
    file_log = logging.FileHandler(filename, mode='w')
    file_log.setLevel(logging.INFO)
    file_log.setFormatter(
        logging.Formatter(
            '[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
            datefmt='%Y/%m/%d-%H:%M:%S'
        )
    )
    return file_log


def memory_usage():
    return psutil.Process().memory_full_info().vms // 1024 // 1024


def profile(m, filename, function, *args):
    start = datetime.datetime.now()
    mem_1 = memory_usage()
    str_prf = f'm1: {memory_usage()}MB'
    ret = function(*args)
    t = datetime.datetime.now() - start
    mem_2 = memory_usage()
    m.append(f'{filename}\t{function.__name__}\t{str_prf}\tm2: {mem_2}MB\t time: {t}\tmd: {mem_2 - mem_1}MB')
    return ret


def write_csv_dict_with_lists(filename, csv_data):
    with open(filename, 'w') as f:
        header = list(csv_data.keys())
        writer = csv.DictWriter(f, header)
        writer.writeheader()
        # upper range defined by any list from the csv_data, since they all are equal
        for i in range(0, len(csv_data[header[0]])):
            data_dict = {}
            for key in header:
                data_dict[key] = csv_data[key][i]
            writer.writerow(data_dict)


def write_csv_list_of_dict(filename, csv_data, logger, overwrite=False):
    try:
        if overwrite:
            open_mode = 'w'
        elif os.access(filename, os.F_OK):
            open_mode = 'a'
        else:
            open_mode = 'w'
        with open(filename, open_mode) as f:
            header = csv_data[0].keys()
            writer = csv.DictWriter(f, header)
            if f.mode == 'w':
                writer.writeheader()
            writer.writerows(csv_data)
    except IndexError:
        logger.warning('[NO PROCESSED DATA]')


def parse_args(logger):
    # Parsear linea de comandos
    parser = OptionParser('usage: python %prog [OPTIONS]')
    parser.add_option('-d', '--directory', action='store', type='string', dest='directory')
    parser.add_option('-c', '--cores', action='store', type='int', dest='cores')
    parser.add_option('-s', '--starts-with', action='store', type='string', dest='starts_with')
    (options, args) = parser.parse_args()
    if not options.directory:
        # This logger line will not be saved to file
        logger.error('[You must specify a working directory]')
        parser.print_help()
        sys.exit(-1)
    options.cores = set_cores(options.cores)
    return options


def sort_list_of_dict(l: list):
    l.sort(key=lambda s: (s['type'], s['device'], s['os'], s['benchmark'], s['size'], s['threads'], s['iteration']))
