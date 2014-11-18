# traverse root directory, and list directories as dirs and files as files

import os
import hashlib


def list_files(startpath):
    for path, dirs, files in os.walk(unicode(startpath)):
        level = path.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print '{}{}/'.format(indent, os.path.basename(path))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            fname = os.path.join(path, f)
            fname_hash = sha256_for_file(fname)
            print '{}{}'.format(subindent, f), ': ',  fname_hash


def sha256_for_file(path, block_size=256 * 128):
    '''
    Block size directly depends on the block size of your filesystem
    to avoid performances issues
    Here I have blocks of 4096 octets (Default NTFS)
    '''
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            sha256.update(chunk)
    return sha256.hexdigest()

if __name__ == '__main__':
    list_files('root')
