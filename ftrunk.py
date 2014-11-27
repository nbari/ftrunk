import bz2
import hashlib
import json
import multiprocessing
import os
import sqlite3
import tempfile
import time

from argparse import ArgumentParser
from crypt import Crypt
from shutil import copyfileobj
from multiprocessing.pool import ThreadPool


class Ftrunk(object):

    def __init__(self, path, tname=None):
        self.path = path
        self.trunkname = tname if tname else os.path.basename(path)
        self.version = int(time.time())
        self.connection = sqlite3.connect('%s.ftrunk' % self.trunkname)
        self.connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
        c = self.connection.cursor()
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA temp_store=MEMORY')
        c.execute('PRAGMA journal_mode=MEMORY')
        query = """CREATE TABLE IF NOT EXISTS trunk (
            hash text,
            file text,
            size integer,
            version integer,
            UNIQUE(hash, version) ON CONFLICT REPLACE
        )"""
        c.execute(query)
        query = """CREATE TABLE IF NOT EXISTS config (
            key text,
            value text,
            version integer,
            PRIMARY KEY(key)
        )"""
        c.execute(query)
        c.execute('INSERT OR REPLACE INTO config values(?, ?, ?)',
                  ('root', self.trunkname, self.version))
        #c.execute('SELECT EXISTS (SELECT 1 FROM trunk)')
        self.connection.commit()
        self.trunk = {}
        self.ftrunk_dir = os.path.expanduser('~/.ftrunk')
        if not os.path.isdir(self.ftrunk_dir):
            os.mkdir(self.ftrunk_dir, 0o700)

    def get(self, key):
        c = self.connection.cursor()
        c.execute("select * from files where key = ?", (key, ))
        value = c.fetchone()
        if not value:
            raise KeyError(key)
        return value[1]

    def backup(self, filename, filehash):
        backup_dir = os.path.join(
            self.ftrunk_dir,
            filehash[:2],
            filehash[2:4],
            filehash[4:6])
        backup_file_path = os.path.join(backup_dir, filehash)
        if os.path.exists(backup_file_path):
            print 'Bye I already have the file'
            return
        # if no file create the parent dir
        os.makedirs(backup_dir)

        # tuple with file descriptor and tmp file
        fd, tmp = tempfile.mkstemp(suffix='.tmp', dir=self.ftrunk_dir)

        with open(filename, 'rb') as file_input:
            with bz2.BZ2File(tmp, 'wb', compresslevel=9) as output:
                copyfileobj(file_input, output)

        x = Crypt('password')
        with open(tmp, 'rb') as in_file, open(backup_file_path, 'wb') as out_file:
            x.encrypt(in_file, out_file)

        os.close(fd)
        os.remove(tmp)

    def save(self):
        c = self.connection.cursor()
        c.executemany(
            'INSERT INTO trunk VALUES (?, ?, ?, ?)',
            [(k, v[0], v[1], self.version) for k, v in self.trunk.iteritems()])
        return self.connection.commit()


def sha256_and_size(path, block_size=4096):
    h = hashlib.sha256()
    s = 0
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            s += len(chunk)
            h.update(chunk)
    return (h.hexdigest(), s)

trunk_list = []


def create(root_path, filename):
    try:
        h, size = sha256_and_size(filename)
    except Exception as e:
        print e
    else:
        return (h, filename[len(root_path):], size)

def create2(xargs):
    root_path, filename = xargs
    try:
        h, size = sha256_and_size(filename)
    except Exception as e:
        print e
    else:
        return (h, filename[len(root_path):], size)

def update_trunk(rs):
    print rs
    trunk_list.append(rs)


def backup(src_dir, trunk_name, passphrase):
    print src_dir, trunk_name, passphrase

    pool = multiprocessing.Pool()
#    pool = ThreadPool()

    for path, _, files in os.walk(src_dir):
        current_path = os.path.join(path)[len(src_dir):]
        if current_path:
            trunk_list.append((current_path, None, 0))
        for f in files:
            filename = os.path.join(path, f)
            if os.path.isfile(filename):
                #pool.apply_async(
                #    create,
                #    args=(src_dir, filename),
                #    callback=update_trunk)
                args = (src_dir, filename)
                print args
                pool.map_async(create2, args)

    pool.close()
    pool.join()

    for x in trunk_list:
        print x

    print '\n' + 'Elapsed time: ' + str(time.time() - start_time)


if __name__ == '__main__':
    start_time = time.time()

    parser = ArgumentParser(description="create or restore file trunks")
    parser.add_argument(
        'src',
        help='directory containing files to be backed or *.frunk file to be \
restored when using option -r')
    parser.add_argument(
        '-d', '--destination',
        help='directory where the backup will be written or restored when \
using option -r')
    parser.add_argument(
        '-r', '--restore', action='store_true',
        help='restore backup')
    parser.add_argument(
        '-n', '--name', action='store',
        help='name of the .ftrunk file')
    parser.add_argument(
        '-p', '--passphrase',
        help='passphrase to be used for encrypting or decrypting when \
restoring, if not set, a random one is created')

    args = parser.parse_args()

    # sanity src dir
    src = os.path.abspath(os.path.expanduser(args.src))
    if not os.path.isdir(src):
        exit('%s - Source directory does not exists' % src)

    if args.destination:
        dst = os.path.abspath(os.path.expanduser(args.destination))
        if not os.path.isdir(src):
            exit('--- pending create dir ---')

    if args.restore:
        pass
    else:
        name = args.name.split()[0] if args.name else None
        backup(src, name, args.passphrase)


#    ftrunk = Ftrunk(src, name)
#    ftrunk.read_dir(ftrunk.path)
#    ftrunk.save()
#    print time.time() - start_time
