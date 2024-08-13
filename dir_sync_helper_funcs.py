import os
import logging
import shutil
import argparse

logger = logging.getLogger(__name__)


def dir_path(path: str) -> str:
    '''
    The function validates that the provided path is a directory. If it's not, it raises an `argparse.ArgumentTypeError`.

    :param path: The path to validate.
    :return: The same path if it is a valid directory.
    '''
    if os.path.isdir(path):
        return path
    raise argparse.ArgumentTypeError(f"{path} is not a valid directory path.")


def file_path(path: str) -> str:
    '''
    The function checks whether the provided path is a file. If it's not, it raises an `argparse.ArgumentTypeError`.

    :param path: The path to validate.
    :return: The same path if it is a valid file.
    '''
    if os.path.isfile(path):
        return path
    raise argparse.ArgumentTypeError(f"{path} is not a valid file path.")


def parse_arguments():
    """
    The function parses CL arguments for directory synchronization.

    :return: A namespace object containing parsed arguments.
    """
    parser = argparse.ArgumentParser(description='Directory synchronization')
    parser.add_argument("-s", "--source", action="store", dest="source_dir_path", type=dir_path)
    parser.add_argument("-r", "--replica", action="store", dest="replica_dir_path", type=dir_path)
    parser.add_argument("-l", "--log", action="store", dest="log_file_path", type=file_path)
    parser.add_argument("-i", "--interval", action="store", dest="interval", type=float,
                        help="Program running interval [minutes]")
    return parser.parse_args()


def get_source_dir_cont(item_path: str, source_path: str = None) -> dict:
    """
    The function traverses the source directory and its subdirectories.

    :param item_path: The path to the directory to be processed.
    :param source_path: The base path for relative paths.
    :return: A dictionary where keys are relative paths to files and directories and values are the action to be performed.
    """
    if not source_path:
        source_path = item_path
    result = {}
    for entry in os.scandir(item_path):
        result[os.path.relpath(entry.path, source_path)] = 'create'
        if entry.is_dir():
            result.update(get_source_dir_cont(entry.path, source_path))
    return result


def get_repl_dir_cont(item_path: str, source_dict: dict, replica_path: str = None) -> dict:
    """
    The function traverses the replica directory and its subdirectories and compares their content with source directory.

    :param item_path: The path to the directory to be processed.
    :param source_dict: The dictionary with relative paths and actions from source directory.
    :param replica_path: The base path for relative paths.
    :return: A dictionary where keys are relative paths to files and directories and values are the action to be performed.
    """
    if not replica_path:
        replica_path = item_path
    result = {}
    for entry in os.scandir(item_path):
        rel_path = os.path.relpath(entry.path, replica_path)
        if rel_path in source_dict:
            result[rel_path] = source_dict[rel_path] = 'compare'
        else:
            result[rel_path] = 'delete'
        if entry.is_dir():
            result.update(get_repl_dir_cont(entry.path, source_dict, replica_path))
    return result


def delete_item(path: str) -> None:
    """
    The function deletes the specified file or directory.

    :param path: The path to the file or directory.
    :return: None
    """
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        logger.info(f"{path} was successfully deleted.")
    except IOError as e:
        logger.error(f"{path} delete failed: {e}")


def copy_item(src_path: str, dest_path: str) -> None:
    """
    The function copies the specified file or directory from source directory to destination directory.

    :param src_path: The path to the source file or directory.
    :param dest_path: The path to the destination file or directory.
    :return: None
    """
    try:
        if os.path.isdir(src_path):
            os.mkdir(dest_path)
            logger.info(f"Folder {src_path} was successfully created in {dest_path}.")
        else:
            shutil.copy2(src_path, dest_path)
            logger.info(f"File {src_path} was successfully copied to {dest_path}.")
    except IOError as e:
        logger.error(f"Creating the copy of {src_path} failed: {e}")


def sync(source: dict, source_dir_path: str, replica: dict, replica_dir_path: str) -> None:
    """
    This function iterates over both the source and replica dictionaries, deletes files or directories in the replica
    that are not present in the source and copies files or directories in the replica that are present in the source.

    :param source: The dictionary with relative paths and actions from the source directory.
    :param source_dir_path: The path of the source directory.
    :param replica: The dictionary with relative paths and actions from the replica directory.
    :param replica_dir_path: The path of the replica directory.
    :return:
    """
    for key, value in replica.items():
        item_path = os.path.join(replica_dir_path, key)
        if value == 'delete' and os.path.exists(item_path):
            delete_item(item_path)

    for key, value in source.items():
        if value == 'create':
            copy_item(os.path.join(source_dir_path, key), os.path.join(replica_dir_path, key))


def distribute_files(files: list, num_cpus: int) -> list:
    """
    The function sorts the files by size and distributes them across the specified number of CPUs.

    :param files: List of file paths to be distributed.
    :param num_cpus: The number of CPUs available for distribution.
    :return: A list of lists, where each list contains file paths assigned to a specific CPU.
    """
    files = sorted(files, key=os.path.getsize, reverse=True)
    cpu_files = [[] for _ in range(num_cpus)]
    cpu_sizes = [0] * num_cpus

    for file in files:
        min_cpu = cpu_sizes.index(min(cpu_sizes))
        cpu_files[min_cpu].append(file)
        cpu_sizes[min_cpu] += os.path.getsize(file)
    return cpu_files


def get_list_of_files_to_cmp(input_dict: dict, path: str) -> list:
    """
    The function returns a list of file paths to be compared.

    :param input_dict: The dictionary with relative paths and actions.
    :param path: The path of the directory.
    :return: list of files
    """
    result = []
    for key, value in input_dict.items():
        abs_path = os.path.join(path, key)
        if os.path.isfile(abs_path) and value == 'compare':
            result.append(abs_path)
    return result
