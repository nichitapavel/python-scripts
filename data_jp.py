import logging
import os
import sys
from optparse import OptionParser

import pandas as pd

from common import log_to_file, profile, HIKEY970, ROCK960, ODROIDXU4
from data_csv_process import get_files
from stats import clean_data


def parse_args(logger):
    # Parsear linea de comandos
    parser = OptionParser('usage: python %prog [OPTIONS]')
    parser.add_option("-d", "--directory", action="store", type="string", dest="directory")
    (options, args) = parser.parse_args()
    if not options.directory:
        # This logger line will not be saved to file
        logger.error('[You must specify files and a directory with merge_data_XX.csv files]')
        parser.print_help()
        sys.exit(-1)
    return options


def combinations():
    devices = [HIKEY970, ODROIDXU4, ROCK960]
    oss = ['linux', 'android']
    benchmarks = ['bt', 'is', 'mg']
    sizes = ['b', 'w']
    threads = [1, 2, 4, 6, 8]
    filters = []
    for device in devices:
        for os in oss:
            for benchmark in benchmarks:
                for size in sizes:
                    for thread in threads:
                        filters.append([device, os, benchmark, size, thread])
    # Remove combinations of benchmark and size that we don't have
    remove = []
    for pair in [['bt', 'b'], ['is', 'w'], ['mg', 'w']]:
        for item in filters:
            if pair[0] == item[2] and pair[1] == item[3]:
                remove.append(item)
    for item in remove:
        filters.remove(item)
    # Remove combinations of device and threads that we don't have
    remove = []
    for pair in [[HIKEY970, 6], [ROCK960, 8], [ODROIDXU4, 6]]:
        for item in filters:
            if pair[0] == item[0] and pair[1] == item[4]:
                remove.append(item)
    for item in remove:
        filters.remove(item)
    # Remove combinations of os and thread that we don't have
    remove = []
    for pair in [['android', 6], ['android', 8]]:
        for item in filters:
            if pair[0] == item[1] and pair[1] == item[4]:
                remove.append(item)
    for item in remove:
        filters.remove(item)

    return filters


def main():
    options = parse_args(logger)
    os.chdir(options.directory)
    logger.addHandler(log_to_file('stats.log'))
    data = pd.DataFrame()
    files = get_files('merge_data_')
    filter_by = combinations()
    for cbn in filter_by:
        group = pd.DataFrame()
        for file in files:
            index = int(file[-6:-4])
            df = pd.read_csv(file)
            df = df[df['device'] == cbn[0]]
            df = df[df['os'] == cbn[1]]
            df = df[df['benchmark'] == cbn[2]]
            df = df[df['size'] == cbn[3]]
            df = df[df['threads'] == cbn[4]]
            df['file-number'] = index
            group = group.append(df, sort=False)
        group.reset_index(drop=True, inplace=True)
        group.index += 1
        group['old-iteration'] = group['iteration']
        group['iteration'] = group.index
        data = data.append(group, sort=False)
    data = clean_data(data)
    data = data.round(decimals=3)
    data.to_csv(path_or_buf=f'{options.directory}/merge_data.csv', index=False)


if __name__ == "__main__":
    logger = logging.getLogger('STATS_CSV')
    pd.options.display.width = 0
    mem = []
    profile(mem, 'test', main,)

    mem.sort()
    for item in mem:
        print(item)


# PANDA
# df_sub = df[ df['salary'] > 120000 ] -> select data where salary > 120 000
# data frame method:
#   mean, median, mod, mad, max, min, std (standard deviation), var, dropna (drop records with no values)
# object.describe -> does many of previous
# df_rank = df.groupby(['rank']) -> split data based on rank
# df.groupby(['rank'], sort=False) -> sort=False for speedup
# df_sorted = df.sort_values( by ='service') -> sort by value of column service
# flights[flights.isnull().any(axis=1)] -> Select the rows that have at least one missing value
# flights[['dep_delay','arr_delay']].agg(['min','mean','max']) -> compute multiple statistics at once
