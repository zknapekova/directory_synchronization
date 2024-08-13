import logging
from dir_sync_helper_funcs import get_source_dir_cont, get_repl_dir_cont, distribute_files, sync, \
    get_list_of_files_to_cmp
from multiprocessing import cpu_count
from async_multiprocessing_file_sync import MultiprocessingAsync


def directory_sync(source_dir_path, replica_dir_path, log_file_path) -> None:
    """
    The function performs directory synchronization between the source and the replica location.

    :param source_dir_path: The path to the source dir.
    :param replica_dir_path: The path to the replica dir
    :param log_file_path: The path to the log file.
    :return:None
    """
    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO,
                        handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()])
    logger = logging.getLogger(__name__)
    logger.info("Synchronization started.")

    all_items_source = get_source_dir_cont(source_dir_path)
    all_items_replica = get_repl_dir_cont(replica_dir_path, all_items_source)

    # create missing files and folders and delete redundant ones from the replica folder
    sync(all_items_source, source_dir_path, all_items_replica, replica_dir_path)

    # order remaining files by size and distribute their content hashing evenly across available CPU
    num_cpu_to_use = max(cpu_count() - 2, 1)
    list_of_files_to_cmp = get_list_of_files_to_cmp(all_items_source, source_dir_path)
    result = distribute_files(list_of_files_to_cmp, num_cpu_to_use)

    # create processes for parallel execution
    processes = []
    for i in range(len(result)):
        processes.append(
            MultiprocessingAsync(result[i], source_dir_path, replica_dir_path))

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    logger.info("Synchronization ended.")
