import logging
import os
import sys
from multiprocessing.pool import Pool
from optparse import OptionParser

import matplotlib.pyplot as plt
import numpy
import pandas as pd
import seaborn as sns

from common import parse_args, log_to_file, IDs, profile, DataFilterItems, ResultItems, write_csv_list_of_dict, CORES
from plotters import box_plot


def parse_args(logger):
    # Parsear linea de comandos
    parser = OptionParser('usage: python %prog [OPTIONS]')
    parser.add_option("--df", "--data-file", action="store", type="string", dest="data_file")
    parser.add_option("--sd", "--save-directory", action="store", type="string", dest="save_directory")
    parser.add_option("--only-stats", action="store_true", dest="only_stats")
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
    df = df[df[IDs.ITERATION] > 10]  # Remove the warmup = first 10 iterations
    # Remove data that has a more than 1% difference between npb reported time and
    # time calculated from power metric files
    df = df[df['time_npb'] / df['time'] > 0.99]
    for item in ResultItems:
        df = df[df[item] != 0]
    return df


def set_data_labels(cp):
    bars = cp.ax.patches
    for bar in bars:
        bar_width = bar.get_width()
        text_x = bar_width / 2 + bar.get_x()
        bar_height = bar.get_height()
        if not numpy.isnan(bar_height):
            text_y = bar_height + bar_height * 0.01
            cp.ax.text(x=text_x, y=text_y, s=f'{bar_height:.2f}', ha='center', fontsize=9)


def cat_plotting(cwd, df, options, x_axis_groupby, type):
    df_groups = df.groupby(list(set(DataFilterItems) - set([x_axis_groupby])))
    values = ['mean', 'q2_median']
    with Pool(CORES) as p:
        results = [p.apply_async(
            catplot_for_parallel, (df_groups, options, type, value, x_axis_groupby)
        ) for value in values]
        for result in results:
            result.get()


def catplot_for_parallel(df_groups, options, type, value, x_axis_groupby):
    for group in df_groups:
        name = f'catplot_{value}_{type}_{x_axis_groupby}_{"_".join(str(x) for x in group[0])}'
        cp = sns.catplot(x=IDs.THREADS, y=value, data=group[1], height=6, kind="bar", palette="muted")
        set_data_labels(cp)
        cp.savefig(name)
        cp.fig.clf()
        plt.close()
        logger.info(f'[{options.data_file}][CATPLOT][{name}]')


def cat_plotting_group(cwd, df, options, x_axis_groupby, type):
    df_groups = df.groupby(list(set(DataFilterItems) - set(x_axis_groupby)))
    values = ['mean', 'q2_median']
    with Pool(CORES) as p:
        results = [p.apply_async(
            catplot_group_for_parallel, (df_groups, options, type, value, x_axis_groupby)
        ) for value in values]
        for result in results:
            result.get()


def catplot_group_for_parallel(df_groups, options, type, value, x_axis_groupby):
    for group in df_groups:
        name = f'catplot_{value}_{type}_{"_".join(x_axis_groupby)}_{"_".join(str(x) for x in group[0])}'
        title = f'{type}_{"_".join(group[0])}'
        cp = sns.catplot(x=IDs.THREADS, y=value, hue=x_axis_groupby[1], data=group[1], height=6, kind="bar",
                         palette="muted")
        set_data_labels(cp)
        cp.ax.set_title(title)
        cp.savefig(name)
        cp.fig.clf()
        plt.close()
        logger.info(f'[{options.data_file}][CATPLOT][HUE][{name}]')


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
    with Pool(CORES) as p:
        results = [p.apply_async(boxplot_for_parallel, (cwd, group, options, x_axis_groupby)) for group in df_groups]
        for result in results:
            result.get()


def boxplot_for_parallel(cwd, group, options, x_axis_groupby):
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
    with Pool(CORES) as p:
        results = [p.apply_async(boxplot_group_for_parallel, (group, options, x_axis_groupby)) for group in df_groups]
        for result in results:
            result.get()


def boxplot_group_for_parallel(group, options, x_axis_groupby):
    name = f'{"_".join(str(x) for x in group[0])}'
    groups_plotting(name, group[1], options, x_axis_groupby[0], x_axis_groupby[1])
    groups_plotting(name, group[1], options, x_axis_groupby[1], x_axis_groupby[0])


def update_dict(stats, data_dict, type_of_data):
    new_dict = data_dict.copy()
    for index in stats.index:
        new_dict.update({
            index: stats[type_of_data][index]
        })
    return new_dict


def create_and_write_stats(df):
    df_groups = df.groupby(DataFilterItems)
    stats_results = {IDs.TIME: [], IDs.TIME_NPB: [], IDs.ENERGY: [], IDs.MOPS: []}
    for group in df_groups:
        data_dict = {
            IDs.TYPE: group[0][0],
            IDs.DEVICE: group[0][1],
            IDs.OS: group[0][2],
            IDs.BENCH: group[0][3],
            IDs.SIZE: group[0][4],
            IDs.THREADS: group[0][5].item()
        }
        stats = group[1][ResultItems].agg(['count', 'mean', 'std', 'var', 'mad', 'min', 'max'])
        stats = stats.append(group[1][ResultItems]
                             .quantile([0.25, 0.5, 0.75])
                             .rename(index={0.25: 'q1', 0.5: 'q2_median', 0.75: 'q3'})
                             )
        for result in ResultItems:
            stats_dict = update_dict(stats, data_dict, result)
            stats_results[result].append(stats_dict)
    for result in ResultItems:
        write_csv_list_of_dict(f'stats_{result}.csv', stats_results[result], logger, overwrite=True)

    return stats_results


def main():
    options = parse_args(logger)
    os.chdir(options.save_directory)
    logger.addHandler(log_to_file('stats.log'))
    cwd = os.getcwd()

    df = pd.read_csv(options.data_file)
    df = clean_data(df)
    stats = create_and_write_stats(df)

    if not options.only_stats:
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

        for resultItem in ResultItems:
            pd_stats = pd.DataFrame(stats[resultItem])
            cat_plotting(cwd, pd_stats, options, IDs.THREADS, resultItem)
            cat_plotting_group(cwd, pd_stats, options, [IDs.THREADS, IDs.OS], resultItem)
            cat_plotting_group(cwd, pd_stats, options, [IDs.THREADS, IDs.TYPE], resultItem)

    # df = sns.load_dataset('tips')
    # sns.boxplot(x = "day", y = "total_bill", hue = "smoker", data = df, palette = "Set1")


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
