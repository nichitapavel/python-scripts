import logging
import os
from optparse import OptionParser

import pandas as pd

from common import log_to_file, profile, HIKEY970, ROCK960
from data_csv_process import get_files


def parse_args(logger):
    # Parsear linea de comandos
    parser = OptionParser('usage: python %prog [OPTIONS]')
    parser.add_option("-d", "--directory", action="store", type="string", dest="directory")
    parser.add_option("-m", "--merge", action="store_true", dest="merge")
    (options, args) = parser.parse_args()
    # if not options.data_file or \
    #         not options.save_directory:
    #     # This logger line will not be saved to file
    #     logger.error('[You must specify files and a directory to save]')
    #     parser.print_help()
    #     sys.exit(-1)
    return options


def combinations():
    devices = [HIKEY970, 'odroidxu4', ROCK960]
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
                        filters.append(
                            [device, os, benchmark, size, thread]
                        )
    return filters


def main():
    options = parse_args(logger)
    os.chdir(options.directory)
    logger.addHandler(log_to_file('stats.log'))
    cwd = os.getcwd()
    if options.merge:
        files = []
        [files.extend(get_files(file)) for file in [HIKEY970, ROCK960, 'odroidxu4']]
        merge_data = pd.DataFrame()
        for file in files:
            merge_data = merge_data.append(pd.read_csv(file))
        merge_data.sort_values(by=['device', 'os', 'benchmark', 'size', 'threads'], inplace=True)
        merge_data.to_csv(path_or_buf=f'{options.directory}/merge_data.csv', index=False)
        exit(0)

    files = get_files('merge_data_')
    # filter_by = ['hikey970', 'linux', 'mg', 'b', 8]
    filter_by = combinations()
    for cbn in combinations():
        group = pd.DataFrame()
        for file in files:
            index = int(file[-6:-4])
            df = pd.read_csv(file)
            file_data = df[df['device'] == cbn[0]][df['os'] == cbn[1]][df['benchmark'] == cbn[2]][df['size'] == cbn[3]][df['threads'] == cbn[4]]
            file_data['file-number'] = index
            group = group.append(file_data)
            # df_groups = df.groupby(['device', 'os', 'benchmark', 'size', 'threads'])

        group.reset_index(drop=True, inplace=True)
        group.index += 1
        group['old-iteration'] = group['iteration']
        group['iteration'] = group.index
        group.to_csv(path_or_buf=f'{options.directory}/{cbn[0]}_{cbn[1]}_{cbn[2]}_{cbn[3]}_{cbn[4]}.csv', index=False)


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
