# traverse root directory, and list directories as dirs and files as files

import hashlib
import os
import sys
import time

from hurry.filesize import size
from multiprocessing import Pool


def list_files(path):
    directories = []
    files = []
    src = os.path.abspath(os.path.expanduser(path))
    for root, dirs_o, files_o in os.walk(src):
        for name in dirs_o:
            directories.append(os.path.join(root, name))
        for name in files_o:
            file_path = os.path.join(root, name)
            if os.path.isfile(file_path):
                files.append(file_path)
    return directories, files

def sha256_for_file(path, block_size=4096):
    h = hashlib.sha256()
    with open(path, 'rb') as rf:
        for chunk in iter(lambda: rf.read(block_size), b''):
            h.update(chunk)
    return h.hexdigest(), path

def log(x):
    print x, len(x)

if __name__ == '__main__':
    start_time = time.time()

    d, f = list_files('../root')

    print d

#    d, f = list_files('~')
    #print len(f), sys.getsizeof(f), size(sys.getsizeof(f))
    #print len(d), sys.getsizeof(d), size(sys.getsizeof(d))


#    for i in d:
#        print i
    print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
