import logging
import os
import sys
from optparse import OptionParser
import matplotlib.pyplot as plt
import pandas as pd
from common import parse_args, log_to_file, IDs
from plotters import box_plot


def parse_args(logger):
    # Parsear linea de comandos
    parser = OptionParser("usage: %prog -d|--directory DIRECTORY")
    parser.add_option("--df", "--data-file", action="store", type="string", dest="data_file")
    parser.add_option("--sd", "--save-directory", action="store", type="string", dest="save_directory")
    (options, args) = parser.parse_args()
    if not options.data_file or \
            not options.save_directory:
        # This logger line will not be saved to file
        logger.error('[You must specify files and a directory to save]')
        parser.print_help()
        sys.exit(-1)
    return options


def main():
    options = parse_args(logger)
    os.chdir(options.save_directory)
    logger.addHandler(log_to_file('stats.log'))
    cwd = os.getcwd()

    df = pd.read_csv(options.data_file)
    df = df.dropna()
    df = df[df[IDs.ITERATION] > 10]
    df_groups = df.groupby([IDs.TYPE, IDs.DEVICE, IDs.OS, IDs.BENCH])

    for item in df_groups:
        uniq = item[1][IDs.THREADS].drop_duplicates()
        idict_time, idict_energy, idict_mops = {}, {}, {}
        for j in uniq:
            idict_time[j] = item[1][item[1][IDs.THREADS] == j][IDs.TIME].values.tolist()
            idict_energy[j] = item[1][item[1][IDs.THREADS] == j][IDs.ENERGY].values.tolist()
            idict_mops[j] = item[1][item[1][IDs.THREADS] == j][IDs.MOPS].values.tolist()
        print(item[1].head(1))
        box_plot(f'{cwd}_{"_".join(item[0])}', IDs.TIME, idict_time)
        box_plot(f'{cwd}_{"_".join(item[0])}', IDs.ENERGY, idict_energy)
        box_plot(f'{cwd}_{"_".join(item[0])}', IDs.MOPS, idict_mops)
        bp = item[1].boxplot(figsize=(20, 8), by=[IDs.THREADS], column=[IDs.TIME, IDs.ENERGY, IDs.MOPS])
        plt.close()

    logger.info(f'[{options.data_file}]')


if __name__ == "__main__":
    logger = logging.getLogger('STATS_CSV')
    pd.options.display.width = 0
    main()


# PANDA
# df_sub = df[ df['salary'] > 120000 ] -> select data where salary > 120 000
# data frame method: mean, median, mod, mad, max, min, std (standard deviation), var, dropna (drop records with no values)
# object.describe -> does many of previous
# df_rank = df.groupby(['rank']) -> split data based on rank
# df.groupby(['rank'], sort=False) -> sort=False for speedup
# df_sorted = df.sort_values( by ='service') -> sort by value of column service
# flights[flights.isnull().any(axis=1)] -> Select the rows that have at least one missing value
# flights[['dep_delay','arr_delay']].agg(['min','mean','max']) -> compute multiple statistics at once
