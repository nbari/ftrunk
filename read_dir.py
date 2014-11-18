# traverse root directory, and list directories as dirs and files as files

import os
import hashlib


def list_files(startpath):
    for path, dirs, files in os.walk(unicode(startpath)):
        level = path.replace(startpath, '').count(os.sep)
        indent = u'\xb7' * 2 *  (level)
        print '%s%s/' % (indent, os.path.basename(path))
        subindent = u'\xb7' * 2 * (level + 1)
        for f in files:
            filename = os.path.join(path, f)
            if os.path.isfile(filename):
                try:
                    h = sha256_for_file(filename)
                except Exception as e:
                    print e
                else:
                    print '%s%s: %s' % (subindent, filename, h)


def sha256_for_file(path, block_size=4096):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            h.update(chunk)
    return h.hexdigest()

if __name__ == '__main__':
    list_files('root')
