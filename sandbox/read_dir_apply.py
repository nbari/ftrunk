# traverse home directory and create sha256 hash of files

import hashlib
import os
import time

from multiprocessing import Pool


def list_files(path):
    directories = []
    files = []

    def append_files(x):
        files.append(x)

    pool = Pool()

    src = os.path.abspath(os.path.expanduser(path))
    for root, dirs_o, files_o in os.walk(src):
        for name in dirs_o:
            directories.append(os.path.join(root, name))
        for name in files_o:
            file_path = os.path.join(root, name)
            if os.path.isfile(file_path):
                pool.apply_async(
                    sha256_for_file,
                    args=(file_path,),
                    callback=append_files)

    pool.close()
    pool.join()

    return directories, files


def sha256_for_file(path, block_size=4096):
    try:
        with open(path, 'rb') as rf:
            h = hashlib.sha256()
            for chunk in iter(lambda: rf.read(block_size), b''):
                h.update(chunk)
        return h.hexdigest(), path
    except IOError:
        return None, path

if __name__ == '__main__':
    start_time = time.time()

    #d, f = list_files('../root')
    d, f = list_files('~')
    print len(f)

    print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
