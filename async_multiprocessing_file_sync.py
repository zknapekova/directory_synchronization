import asyncio
from hashlib import blake2b
from multiprocessing import Process
import os
import logging
from dir_sync_helper_funcs import copy_item

logger = logging.getLogger(__name__)


class MultiprocessingAsync(Process):
    def __init__(self, files, source_dir_path, replica_dir_path):
        super(MultiprocessingAsync, self).__init__()
        self.files = files
        self.source_dir_path = source_dir_path
        self.replica_dir_path = replica_dir_path

    @staticmethod
    def hash_file_content(file_path: str) -> str:
        """
        The method calculates the hash using blake2b of a file's content.

        :param file_path: The path to file to be hashed.
        :return: A string representing the hash of the file content.
        """
        with open(file_path, 'rb') as f:
            data = f.read()
        return blake2b(data).hexdigest()

    async def process_file(self, file_path: str) -> None:
        """
        The method calculates the hash of the source file and its replica counterpart, compares the hashes
        and if they differ, the source file is copied to the replica directory.

        :param file_path: The path of the file to be processed.
        :return: None
        """
        key_rel_path = os.path.relpath(file_path, self.source_dir_path)
        try:
            source_hashed_file = self.hash_file_content(file_path)
            replica_hashed_file = self.hash_file_content(os.path.join(self.replica_dir_path, key_rel_path))
            if source_hashed_file != replica_hashed_file:
                copy_item(file_path, os.path.join(self.replica_dir_path, key_rel_path))
        except IOError as e:
            logger.error(f"Reading and hashing of file {file_path} failed: {e}")

    async def consecutive_steps(self) -> None:
        """
        The method asynchronously executes the processing for all files in the assigned list.

        :return:None
        """
        tasks = [self.process_file(file) for file in self.files]
        await asyncio.gather(*tasks)

    def run(self):
        asyncio.run(self.consecutive_steps())
