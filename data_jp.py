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
    (options, args) = parser.parse_args()
    # if not options.data_file or \
    #         not options.save_directory:
    #     # This logger line will not be saved to file
    #     logger.error('[You must specify files and a directory to save]')
    #     parser.print_help()
    #     sys.exit(-1)
    return options


def main():
    options = parse_args(logger)
    os.chdir(options.directory)
    logger.addHandler(log_to_file('stats.log'))
    cwd = os.getcwd()
    files = get_files('merge_data_')
    filter_by = ['hikey970', 'linux', 'bt', 'w', 2]
    for file in files:
        index = int(file[-6:-4])
        df = pd.read_csv(file)
        group = df[df['device'] == filter_by[0]][df['os'] == filter_by[1]][df['benchmark'] == filter_by[2]][df['size'] == filter_by[3]][df['threads'] == filter_by[4]]
        df_groups = df.groupby(['device', 'os', 'benchmark', 'size', 'threads'])
        for group in df_groups:
            print('ole')


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
