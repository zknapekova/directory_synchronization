import logging
import time
from func import check_paths, get_source_dir_cont, get_repl_dir_cont, distribute_files, \
    get_list_of_files_to_cmp, sync
from multiprocessing import cpu_count
from classes import MultiprocessingAsync

source_dir_path = "C:\\Users\\zknap\\Desktop\\veeam\\source"
replica_dir_path = "C:\\Users\\zknap\\Desktop\\veeam\\replica"
log_file_path = "C:\\Users\\zknap\\Desktop\\veeam\\logs\\logs.txt"

def directory_synch(source_dir_path, replica_dir_path, log_file_path):
    start = time.time()

    logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=logging.INFO,
                        handlers=[logging.FileHandler(log_file_path), logging.StreamHandler()])
    logger = logging.getLogger(__name__)
    logger.info("Synchronization has started.")

    # check if all paths exist
    check_paths(
        (source_dir_path, "Source directory"),
        (replica_dir_path, "Replica directory"),
        (log_file_path, "Log file")
    )

    all_items_source = get_source_dir_cont(source_dir_path)
    all_items_replica = get_repl_dir_cont(replica_dir_path, all_items_source)

    # create missing files and folders and delete redundant ones from the replica folder
    sync(all_items_source, source_dir_path, all_items_replica, replica_dir_path)

    # compare files with the same name
    num_cpu_to_use = max(cpu_count() - 1, 1)
    list_of_files_to_cmp = get_list_of_files_to_cmp(all_items_source, source_dir_path)
    result = distribute_files(list_of_files_to_cmp, num_cpu_to_use)

    processes = []
    for i in range(len(result)):
        processes.append(
            MultiprocessingAsync(result[i], source_dir_path, replica_dir_path))

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    end = time.time()
    print(end - start)


if __name__ == "__main__":
    directory_synch(source_dir_path, replica_dir_path, log_file_path)

