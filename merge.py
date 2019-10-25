import csv
import logging
import os
import sys
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


def read_csv_to_dic(csv_file: str) -> 'list,list':
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
        merged_data = data_csv.merge(
            metrics_csv,
            on=merge_on,
            sort=True, how='outer'
        )
        # merged_data = metrics_csv.merge(right=data_csv, how='inner', sort=True, validate='one_to_one')
        merged_data.to_csv(path_or_buf='merge_data_pd.csv', index=False)
        return merged_data
    except Exception:
        return False


if __name__ == "__main__":
    # global logger
    logger = logging.getLogger('MERGE')
    main()
