import os
import main

def test_sync_directories():
        source_dir_path = "C:\\Users\\zknap\\Desktop\\veeam\\test_source"
        replica_dir_path = "C:\\Users\\zknap\\Desktop\\veeam\\test_replica"
        log_file_path = "C:\\Users\\zknap\\Desktop\\veeam\\logs\\logs.txt"

        with open(os.path.join(source_dir_path, 'test_file.txt'), 'w') as f:
            f.write('test content after copying')
        with open(os.path.join(replica_dir_path, 'test_file.txt'), 'w') as f:
            f.write('test content22222')

        with open(os.path.join(replica_dir_path, 'test_file_to_delete.txt'), 'w') as f:
            f.write('asdsfdf')

        os.makedirs(os.path.join(source_dir_path, 'folder1', 'nested_folder'))

        with open(os.path.join(os.path.join(source_dir_path, 'folder1'), 'large_file.bin'), 'wb') as f:
                f.write(os.urandom(1024 * 1024))

        main.directory_synch(source_dir_path, replica_dir_path, log_file_path)

        assert os.path.exists(os.path.join(replica_dir_path, 'test_file.txt'))
        assert not os.path.isfile(os.path.join(replica_dir_path, 'test_file_to_delete.txt'))

        with open(os.path.join(replica_dir_path, 'test_file.txt'), 'r') as f:
                assert f.read() == 'test content after copying'

        assert os.path.join(os.path.join(replica_dir_path, 'folder1'), 'large_file.bin')


