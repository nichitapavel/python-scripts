import logging
import os
from multiprocessing import Manager, Pool

from common import csv_name_parsing, log_to_file, profile, write_csv_list_of_dict, parse_args, sort_list_of_dict

logging.basicConfig(
    level=logging.INFO,
    # filename='thread-flask-pminfo.log',
    format='[%(process)d][%(asctime)s.%(msecs)03d][%(name)s][%(levelname)s]%(message)s',
    datefmt='%Y/%m/%d-%H:%M:%S'
)


def metrics_file_process(cwd: str, file: str) -> [dict]:
    """
    Opens 'file', reads all it's lines and collects data with each
    run of a benchmark in a dict.
    :param cwd: string, current working directory
    :param file: string, name of file to process
    :return: a list of dict
    """
    logger.info(f'[{cwd}][{file}]')
    data = []
    name_parsed = csv_name_parsing(file)
    name_parsed['size'] = None
    with open(file, 'r') as f:
        lines = f.readlines()
        i = 0
        while i < len(lines):
            if 'Run:' in lines[i]:
                i, metric_dict = block_process_run(i, lines, name_parsed)
                data.extend(metric_dict)
            i += 1
    # mark this file as processed
    open(f'read-{file}', 'w').close()
    return data


def block_process_run(i: int, lines: list, name_parsed: dict) -> 'int,dict':
    """
    Process data between lines with 'Run: n' and 'Run: n+1'.
    This function has a sketchy implementation and philosophy in general,
    main candidate to refactor.
    :param i: int current position in 'lines'
    :param lines: list of strings with all lines from the file
    :param name_parsed: a dict with key and value predefined
    :return: int i: position for previous line before 'Run: n+1', dict
    """
    iteration = int(lines[i].split()[2])
    iter_dict = name_parsed.copy()
    iter_dict.update({'iteration': f'{iteration:03}'})
    # Let go to the next line, it should have 'Size: x'
    i += 1
    dicts = []
    # try/except block for when we get to the appearance of last 'Run: x'
    # since after this block there will be no more of 'Run: x' it will raise
    # an IndexError. No more available lines to process.
    try:
        while 'Run:' not in lines[i]:
            i, ret_dicts = block_process_size(i, lines, iter_dict)
            dicts.append(ret_dicts)
            i += 1
            # While in 'Run: n' block we must go to the next line
        # We are at the line with 'Run: n+1', we must set the position to the previous
        # line so that outer function can read it and launch this function again
        i -= 1
    except IndexError:
        pass
    return i, dicts


def block_process_size(i: int, lines: list, name_parsed: dict) -> 'int,dict':
    metric_dict = name_parsed.copy()
    time_npb = ''
    mops = ''
    size = lines[i].split()[2].lower()  # In case there is no npb data
    # Let go to the next line, it should have 'Size: x'
    i += 1
    # try/except block for when we get to the appearance of last 'Run: x'
    # since after this block there will be no more of 'Run: x' it will raise
    # an IndexError. No more available lines to process.
    try:
        while 'Run' not in lines[i] and 'Size:' not in lines[i]:
            if 'Class' in lines[i]:
                metric_dict['size'] = lines[i].split()[3].lower()
            if 'Time in ' in lines[i]:
                time_npb = float(lines[i].split()[5])
            elif 'Mops ' in lines[i]:
                mops = float(lines[i].split()[4])
            # While in 'Run: n' block we must go to the next line
            i += 1
        # We are at the line with 'Run: n+1', we must set the position to the previous
        # line so that outer function can read it and launch this function again
        i -= 1
    except IndexError:
        pass
    # Check if there is no npb data
    if metric_dict['size'] is None:
        metric_dict['size'] = size
    metric_dict.update({'time_npb': time_npb, 'mops': mops})
    return i, metric_dict


def get_files(filter):
    """
    Filter files from current directory.
    :return: a list of string, each string is a file name from current directory
    """
    files = os.listdir(os.curdir)
    files_ret = []
    for filename in files:
        # Basically, dont call your data files metrics.log or start names with 'read-',
        # is used as a meta cache system.
        if filename.endswith('.log') and \
                f'read-{filename}' not in files and \
                filename != 'metrics.log' and \
                not filename.startswith('read-'):
            if filter is not None:
                if filename.startswith(filter):
                    files_ret.append(filename)
            else:
                files_ret.append(filename)
    return files_ret


def main():
    options = parse_args(logger)
    os.chdir(options.directory)
    logger.addHandler(log_to_file('metrics.log'))

    cwd = os.getcwd()
    files = get_files(options.starts_with)
    processed_data = []

    with Pool(options.cores) as p:
        results = [p.apply_async(metrics_file_process, (cwd, file)) for file in files]
        for result in results:
            processed_data.extend(result.get())

    sort_list_of_dict(processed_data)
    write_csv_list_of_dict('metrics_data.csv', processed_data, logger)


if __name__ == "__main__":
    logger = logging.getLogger('METRICS')
    with Manager() as manager:
        mem = manager.list()
        profile(mem, 'global', main)

        mem.sort()
        for item in mem:
            print(item)
