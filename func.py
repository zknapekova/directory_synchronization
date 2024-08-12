import os
import sys
import logging
import shutil

logger = logging.getLogger(__name__)


def check_paths(*paths) -> None:
    for path, name in paths:
        if not os.path.exists(path):
            logger.error(f"{name} path not found: {path}")
            sys.exit()


def get_source_dir_cont(item_path: str, source_path: str = None) -> dict:
    if not source_path:
        source_path = item_path
    result = {}
    for entry in os.scandir(item_path):
        result[os.path.relpath(entry.path, source_path)] = 'create'
        if entry.is_dir():
            result.update(get_source_dir_cont(entry.path, source_path))
    return result


def get_repl_dir_cont(item_path: str, source_dict: dict, replica_path: str = None) -> dict:
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
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        logger.info(f"{path} was successfully deleted.")
    except BaseException as e:
        logger.error(f"{path} delete failed: {e}")


def copy_item(src_path: str, dest_path: str) -> None:
    try:
        if os.path.isdir(src_path):
            os.mkdir(dest_path)
        else:
            shutil.copy2(src_path, dest_path)
        logger.info(f"{src_path} was successfully copied to {dest_path}.")
    except BaseException as e:
        logger.error(f"Creating the copy of {src_path} failed: {e}")


def sync(source: dict, source_dir_path: str, replica: dict, replica_dir_path: str) -> None:
    for key, value in replica.items():
        item_path = os.path.join(replica_dir_path, key)
        if value == 'delete' and os.path.exists(item_path):
            delete_item(item_path)

    for key, value in source.items():
        if value == 'create':
            copy_item(os.path.join(source_dir_path, key), os.path.join(replica_dir_path, key))


def distribute_files(files, num_cpus):
    files = sorted(files, key=os.path.getsize, reverse=True)
    cpu_files = [[] for _ in range(num_cpus)]
    cpu_sizes = [0] * num_cpus

    for file in files:
        min_cpu = cpu_sizes.index(min(cpu_sizes))
        cpu_files[min_cpu].append(file)
        cpu_sizes[min_cpu] += os.path.getsize(file)
    return cpu_files


def get_list_of_files_to_cmp(input_dict: dict, path: str) -> int:
    result = []
    for key, value in input_dict.items():
        abs_path = os.path.join(path, key)
        if os.path.isfile(abs_path) and value == 'compare':
            result.append(abs_path)
    return result
