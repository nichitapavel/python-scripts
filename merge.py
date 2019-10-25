import csv
import logging
import os
import sys
from collections import OrderedDict
from optparse import OptionParser

import pandas as pd

from common import write_csv_list_of_dict, parse_args, sort_list_of_dict, DataFilterItems, IDs

logging.basicConfig(
    level=logging.INFO,
    # filename='thread-flask-pminfo.log',
    format='[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
    datefmt='%Y/%m/%d-%H:%M:%S'
)


def parse_args(logger):
    # Parsear linea de comandos
    parser = OptionParser('usage: python %prog [OPTIONS]')
    parser.add_option("--df", "--data-file", action="store", type="string", dest="data_file")
    parser.add_option("--mf", "--metrics-file", action="store", type="string", dest="metrics_file")
    parser.add_option("--sd", "--save-directory", action="store", type="string", dest="save_directory")
    (options, args) = parser.parse_args()
    if not options.data_file or \
            not options.metrics_file or \
            not options.save_directory:
        # This logger line will not be saved to file
        logger.error('[You must specify files and a directory to save]')
        parser.print_help()
        sys.exit(-1)
    return options


def read_csv_to_dict(csv_file: str) -> 'list,list':
    """
    Read a csv file and transform it into a OrderDict
    :param csv_file: string with the path of file to read (including file name)
    :return: A list of keys (header of csv), A list of OrderedDict (data of csv)
    """
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        keys = reader.fieldnames
        data = [d for d in reader]
    return keys, data


def merge_on_intersect_dicts(data: list, metrics: list) -> list:
    """
    Merges two dicts into one using the common elements, returns the new list of OrderedDict
    Rows that are not present in both dicts are not added to the new list.
    Both dicts are modified, make a copy if you want to do something else with them.
    :param data: a list of OrderedDict, data set 1
    :param metrics: a list of OrderedDict, data set 2
    :return: a list of OrderedDict
    """
    data_keys = data[0].keys()
    metrics_keys = metrics[0].keys()
    intersect = list(set(data_keys) & set(metrics_keys))
    merged_data = []
    i = 0
    while i < len(metrics):
        mlp_item = metrics[i]
        j = 0
        while j < len(data):
            dcp_item = data[j]
            # Do fields match?
            if 0 == [dcp_item[key] == mlp_item[key] for key in intersect].count(False):
                merged_dict = mlp_item.copy()
                merged_dict.update(dcp_item)
                merged_data.append(merged_dict)
                data.remove(dcp_item)
                metrics.remove(mlp_item)
                i -= 1
                j -= 1
            j += 1
        i += 1
    return merged_data


def append_data(data: list, diff: list, merged_data: list):
    """
    Appends to 'merged_data' rows from 'data' updated with empty valued fields from 'diff'.
    Follows keys order from 'merged_data'.
    :param data: list of OrderedDicts
    :param diff: list of keys present in 'merged_data' but not in 'data'
    :param merged_data: list of OrderedDicts
    :return:
    """
    for item in data:
        item.update({x: '' for x in diff})
        # Follow keys order of 'merged_data'
        or_dict = OrderedDict()
        for k in merged_data[0].keys():
            or_dict[k] = item[k]
        merged_data.append(or_dict)


def merge_dicts(data: str, metrics: str):
    """
    Reads two csv files, merges data on equal values using intersected fields,
    add fields from first to second and vice-versa,  adds empty valued fields
    to data rows not present in one of the csv's and adds these updated rows
    to merged data. Finally alphabetically sorts the merged data and saved it
    to 'merged_data.csv' file in 'directory'
    :param data: csv file with processed data from energy files
    :param metrics: csv file with metrics from log file
    :param directory: string with path where to save the merged csv file
    :return:
    """
    data_keys, data_csv = read_csv_to_dict(data)
    metrics_keys, metrics_csv = read_csv_to_dict(metrics)
    merged_data = merge_on_intersect_dicts(metrics=metrics_csv, data=data_csv)
    merged_keys = list(set(data_keys) | set(metrics_keys))
    diff = list(set(merged_keys) - set(metrics_keys))
    append_data(metrics_csv, diff, merged_data)
    diff = list(set(merged_keys) - set(data_keys))
    append_data(data_csv, diff, merged_data)
    sort_list_of_dict(merged_data)
    return merged_data


def main():
    # options = parse_args(logger)
    options = parse_args(logger)

    with open(options.data_file, 'r') as f:
        reader = csv.DictReader(f)
        dcp_keys = reader.fieldnames
        dcp_data = [d for d in reader]

    with open(options.metrics_file, 'r') as f:
        reader = csv.DictReader(f)
        mlp_keys = reader.fieldnames
        mlp_data = [d for d in reader]

    os.chdir(options.save_directory)

    merged_data = []
    i = 0
    while i < len(dcp_data):
        dcp_item = dcp_data[i]
        j = 0
        while j < len(mlp_data):
            mlp_item = mlp_data[j]
            if list(dcp_item.values())[0:-2] == list(mlp_item.values())[0:-2]:
                merged_dict = dcp_item.copy()
                merged_dict.update(mlp_item)
                merged_data.append(merged_dict)
                dcp_data.remove(dcp_item)
                mlp_data.remove(mlp_item)
                i -= 1
                j -= 1
                break
            j += 1
        i += 1

    diff_dcp_mlp = list(set(dcp_keys) - set(mlp_keys))
    diff_mlp_dcp = list(set(mlp_keys) - set(dcp_keys))
    for dcp_item in dcp_data:
        dcp_item.update({x: '' for x in diff_mlp_dcp})
        merged_data.append(dcp_item)

    for mlp_item in mlp_data:
        mlp_item.update({x: '' for x in diff_dcp_mlp})
        merged_data.append(mlp_item)

    sort_list_of_dict(merged_data)
    write_csv_list_of_dict('merge_data.csv', merged_data, logger, overwrite=True)


def merge_pd(data_file: str, metrics_file: str):
    try:
        data_csv = pd.read_csv(data_file)
        metrics_csv = pd.read_csv(metrics_file)
        merge_on = DataFilterItems.copy()
        merge_on.append(IDs.ITERATION)
        merged_data = data_csv.merge(metrics_csv, on=merge_on, sort=True, how='outer')
        merged_data.to_csv(path_or_buf='merge_data_pd.csv', index=False)
        return merged_data
    except Exception:
        return False


if __name__ == "__main__":
    # global logger
    logger = logging.getLogger('MERGE')
    main()
