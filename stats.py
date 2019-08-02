import logging
import os
import sys
from optparse import OptionParser
import matplotlib.pyplot as plt
import pandas as pd
from common import parse_args, log_to_file, IDs, profile, DataFilterItems, ResultItems
from plotters import box_plot
import seaborn as sns


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


def clean_data(df):
    """
    Clean data from Null (empty) or 0.0 values
    :param df: DataFrame read with panda from a csv (usually and in my case)
    :return: DataFrame without Null (empty) or 0.0 values
    """
    df = df.dropna()
    df = df[df[IDs.ITERATION] > 10]
    for item in ResultItems:
        df = df[df[item] != 0]
    return df


def box_plotting(cwd, df, options, x_axis_groupby):
    """
    Creates boxplots with data from :df and saves them in :cwd.
    :df has type,device,os,benchmark and threads, using :x_axis_groupby
    we group data by excluding :x_axis_groupby, and after we use it
    for the x axis.
    :param cwd: string, full path to current working directory
    :param df: pandas DataFrame, data read from a csv file (usually and in my case)
    :param options: optparse.Values object, parsed arguments, used for logging
    :param x_axis_groupby: string, the x axis of the boxplot
    :return:
    """
    df_groups = df.groupby(list(set(DataFilterItems) - set([x_axis_groupby])))
    for group in df_groups:
        unique = group[1][x_axis_groupby].drop_duplicates()
        dicts = {}
        for i in ResultItems:
            dicts[i] = {}
            for j in unique:
                dicts[i][j] = group[1][group[1][x_axis_groupby] == j][i].values.tolist()
        for keys, values in dicts.items():
            name = f'{x_axis_groupby}_{"_".join(str(x) for x in group[0])}'
            box_plot(cwd, name, keys, values)
            logger.info(f'[{options.data_file}][BOXPLOT][{name}]')
            plt.close()


def groups_plotting(name, data, options, x_group, x_axis):
    name_time = f'{name}_{IDs.TIME}_{x_axis}_{x_group}.png'
    logger.info(f'[{options.data_file}][BOXPLOT][{name_time}]')
    bp = sns.boxplot(x=x_axis, y=IDs.TIME, hue=x_group, data=data)
    bp.get_figure().savefig(name_time)
    bp.get_figure().clf()
    name_energy = f'{name}_{IDs.ENERGY}_{x_axis}_{x_group}.png'
    logger.info(f'[{options.data_file}][BOXPLOT][{name_energy}]')
    bp = sns.boxplot(x=x_axis, y=IDs.ENERGY, hue=x_group, data=data)
    bp.get_figure().savefig(name_energy)
    bp.get_figure().clf()
    name_mops = f'{name}_{IDs.MOPS}_{x_axis}_{x_group}.png'
    logger.info(f'[{options.data_file}][BOXPLOT][{name_mops}]')
    bp = sns.boxplot(x=x_axis, y=IDs.MOPS, hue=x_group, data=data)
    bp.get_figure().savefig(name_mops)
    bp.get_figure().clf()


def box_plotting_groups(cwd, df, options, x_axis_groupby):
    df_groups = df.groupby(list(set(DataFilterItems) - set(x_axis_groupby)))
    for group in df_groups:
        name = f'{"_".join(str(x) for x in group[0])}'
        groups_plotting(name, group[1], options, x_axis_groupby[0], x_axis_groupby[1])
        groups_plotting(name, group[1], options, x_axis_groupby[1], x_axis_groupby[0])


def main():
    options = parse_args(logger)
    os.chdir(options.save_directory)
    logger.addHandler(log_to_file('stats.log'))
    cwd = os.getcwd()

    df = pd.read_csv(options.data_file)
    df = clean_data(df)

    box_plotting(cwd, df, options, x_axis_groupby=IDs.THREADS)
    box_plotting(cwd, df, options, x_axis_groupby=IDs.OS)
    box_plotting(cwd, df, options, x_axis_groupby=IDs.DEVICE)
    box_plotting(cwd, df, options, x_axis_groupby=IDs.TYPE)
    logger.info(f'[{options.data_file}]')

    box_plotting_groups(cwd, df, options, [IDs.DEVICE, IDs.THREADS])
    box_plotting_groups(cwd, df, options, [IDs.DEVICE, IDs.BENCH])
    box_plotting_groups(cwd, df, options, [IDs.DEVICE, IDs.TYPE])
    box_plotting_groups(cwd, df, options, [IDs.DEVICE, IDs.OS])

    box_plotting_groups(cwd, df, options, [IDs.THREADS, IDs.BENCH])
    box_plotting_groups(cwd, df, options, [IDs.THREADS, IDs.TYPE])
    box_plotting_groups(cwd, df, options, [IDs.THREADS, IDs.OS])

    box_plotting_groups(cwd, df, options, [IDs.BENCH, IDs.TYPE])
    box_plotting_groups(cwd, df, options, [IDs.BENCH, IDs.OS])

    box_plotting_groups(cwd, df, options, [IDs.TYPE, IDs.OS])

    df = sns.load_dataset('tips')
    sns.boxplot(x = "day", y = "total_bill", hue = "smoker", data = df, palette = "Set1")


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
# data frame method: mean, median, mod, mad, max, min, std (standard deviation), var, dropna (drop records with no values)
# object.describe -> does many of previous
# df_rank = df.groupby(['rank']) -> split data based on rank
# df.groupby(['rank'], sort=False) -> sort=False for speedup
# df_sorted = df.sort_values( by ='service') -> sort by value of column service
# flights[flights.isnull().any(axis=1)] -> Select the rows that have at least one missing value
# flights[['dep_delay','arr_delay']].agg(['min','mean','max']) -> compute multiple statistics at once
