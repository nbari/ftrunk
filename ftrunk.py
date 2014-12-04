import bz2
import hashlib
import json
import os
import sqlite3
import tempfile
import time

from argparse import ArgumentParser
from crypt import Crypt
from multiprocessing import Pool
from shutil import copyfileobj


def checksum512(path, block_size=4096):
    try:
        with open(path, 'rb') as f:
            h = hashlib.sha512()
            s = 0
            for chunk in iter(lambda: f.read(block_size), b''):
                s += len(chunk)
                h.update(chunk)
        return h.hexdigest(), path, s
    except Exception:
        return None


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

        # trunk table for storing files/directories
        query = """CREATE TABLE IF NOT EXISTS trunk (
            hash text,
            file text,
            size integer,
            version integer,
            UNIQUE(hash, version) ON CONFLICT REPLACE
        )"""
        c.execute(query)

        # config table
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

    def build(self):
        self.trunk['dirs'] = []
        self.trunk['files'] = {}

        def append_files(x):
            if x:
                f = self.trunk['files'].setdefault(x[0], [])
                if f:
                    f[0][0].append(x[1])
                else:
                    f.append(([x[1]], x[2]))

        # multiprocessing Pool (use all the cores)
        pool = Pool()

        for root, dirs_o, files_o in os.walk(self.path):
            for d in dirs_o:
                self.trunk['dirs'].append((os.path.join(root, d), None, 0))
            for f in files_o:
                file_path = os.path.join(root, f)
                if os.path.isfile(file_path):
                    pool.apply_async(
                        checksum512,
                        args=(file_path,),
                        callback=append_files)

        # wait until all process finish
        pool.close()
        pool.join()

        self.trunk['files'] = [(k, json.dumps(v[0][0]), v[0][1])
                               for k, v in self.trunk['files'].iteritems()]

        print self.trunk['dirs']
        print '\n' + 'Elapsed time: ' + str(time.time() - start_time)

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

        try:
            with open(filename, 'rb') as file_input:
                with bz2.BZ2File(tmp, 'wb', compresslevel=9) as output:
                    copyfileobj(file_input, output)

            x = Crypt('password')
            with open(tmp, 'rb') as in_file, open(backup_file_path, 'wb') as out_file:
                x.encrypt(in_file, out_file)
        finally:
            os.close(fd)
            os.remove(tmp)

    def save(self):
        c = self.connection.cursor()
        c.executemany(
            'INSERT INTO trunk VALUES (?, ?, ?, ?)',
            [(k, v[0], v[1], self.version) for k, v in self.trunk.iteritems()])
        return self.connection.commit()


def foo(xx):
    print xx


def backup(src_dir, trunk_name):
    print src_dir, trunk_name

    directories = []
    files = {}

    def append_files(x):
        if x:
            f = files.setdefault(x[0], [])
            if f:
                f[0][0].append(x[1])
            else:
                f.append(([x[1]], x[2]))

    pool = Pool()

    for root, dirs_o, files_o in os.walk(src_dir):
        for d in dirs_o:
            directories.append((os.path.join(root, d), None, 0))
        for f in files_o:
            file_path = os.path.join(root, f)
            if os.path.isfile(file_path):
                print file_path
                exit()
                pool.apply_async(
                    foo,
                    args=(file_path,),
                    callback=append_files)

    pool.close()
    pool.join()

    files = [(k, json.dumps(v[0][0]), v[0][1]) for k, v in files.iteritems()]
    print directories, files

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

    ft = Ftrunk(src, name)
    ft.build()
    print ft.version
