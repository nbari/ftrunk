import bz2
import hashlib
import json
import os
import sqlite3
import time
import random

from Crypto.Cipher import AES
from argparse import ArgumentParser
from base64 import b64encode, b64decode
from multiprocessing import Pool
from tempfile import SpooledTemporaryFile


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

    def __init__(self, db_, src_, dst_):
        self.db = db_
        self.src = src_
        self.dst = dst_
        self.version = int(time.time())

        c = self.db.cursor()

        # trunk table for storing files/directories
        query = """CREATE TABLE IF NOT EXISTS trunk (
            hash TEXT,
            file TEXT,
            size INTEGER,
            version INTEGER,
            UNIQUE(hash, version) ON CONFLICT REPLACE
        )"""
        c.execute(query)

        # table for storing hash/passwords
        query = """CREATE TABLE IF NOT EXISTS pass (
            hash TEXT,
            pass TEXT,
            UNIQUE(hash) ON CONFLICT REPLACE
        )"""
        c.execute(query)

        # config table
        query = """CREATE TABLE IF NOT EXISTS config (
            key TEXT,
            value TEXT,
            version INTEGER,
            PRIMARY KEY(key)
        )"""
        c.execute(query)

        c.executemany('INSERT OR REPLACE INTO config VALUES(?, ?, ?)',
                      [
                          ('src', self.src, self.version),
                          ('dst', self.dst, self.version)
                      ])

        #c.execute('SELECT EXISTS (SELECT 1 FROM trunk)')
        self.db.commit()

        self.trunk = {}

    def build(self):
        self.trunk['dirs'] = []
        self.trunk['files'] = {}

        def append_files(x):
            if x:
                f = self.trunk['files'].setdefault(x[0], [])
                if f:
                    f[0][0].append(x[1][len(self.src):])
                else:
                    f.append(([x[1][len(self.src):]], x[2]))

        # multiprocessing Pool (use all the cores)
        pool = Pool()

        for root, dirs_o, files_o in os.walk(self.src):
            for d in dirs_o:
                self.trunk['dirs'].append(
                    (os.path.join(root, d)[len(self.src):], None, 0))
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

        # trunk files structure
        # hash, list of file or files, size
        self.trunk['files'] = [(k, json.dumps(v[0][0]), v[0][1])
                               for k, v in self.trunk['files'].iteritems()]

    def backup(self, filename, filehash):
        """encrypt filename and save it on the backup dir
        if success
            return base64(password)
        else:
            return None
        """
        if not os.path.isfile(filename):
            print 'Not a file'
            return

        backup_dir = os.path.join(
            self.dst,
            filehash[:2],
            filehash[2:4],
            filehash[4:6])

        backup_file_path = os.path.join(backup_dir, filehash)

        if os.path.isfile(backup_file_path):
            print 'Bye I already have the file'
            return

        # create the backup_dir
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        try:
            with open(filename, 'rb') as in_file:
                with SpooledTemporaryFile(suffix='.tmp', dir=self.dst) \
                        as tmp_file:
                    compressor = bz2.BZ2Compressor(9)
                    for chunk in iter(lambda: in_file.read(4096), b''):
                        tmp_file.write(compressor.compress(chunk))
                    tmp_file.write(compressor.flush())

                    with open(backup_file_path, 'wb') as out_file:
                        try:
                            return self.encrypt(tmp_file, out_file)
                        except Exception as e:
                            print e
        except Exception as e:
            print e
            return

    def encrypt(self, in_file, out_file):
        # out_file: iv + AES encrypted file
        aes_key = os.urandom(32)
        iv = ''.join(chr(random.randint(0, 0xFF)) for i in range(16))
        chiper = AES.new(aes_key, AES.MODE_CBC, iv)
        bs = AES.block_size
        out_file.write(iv)
        in_file.seek(0)
        for chunk in iter(lambda: in_file.read(1024 * bs), b''):
            if len(chunk) % bs != 0:
                padding_length = (bs - len(chunk) % bs) or bs
                chunk += padding_length * chr(padding_length)
            out_file.write(chiper.encrypt(chunk))
        return b64encode(aes_key)

    def decrypt(self, key, in_filename, out_filename):
        with open(in_filename) as in_file:
            aes_key = b64decode(key)
            bs = AES.block_size
            iv = in_file.read(bs)
            cipher = AES.new(aes_key, AES.MODE_CBC, iv)
            with open(out_filename, 'wb') as out_file:
                for chunk in iter(lambda: in_file.read(1024 * bs), b''):
                    chunk = cipher.decrypt(chunk)
                    if len(chunk) % bs != 0:
                        out_file.write(chunk.rstrip(chunk[-1]))
                    out_file.write(chunk)

    def save_trunk(self, data):
        c = self.db.cursor()
        query = """INSERT INTO trunk(
            hash,
            file,
            size,
            version
        ) VALUES(?, ?, ?, %s)""" % (self.version)
        c.executemany(query, data)
        return self.db.commit()

    def save_password(self, key_hash, password):
        c = self.db.cursor()
        c.execute(
            'INSERT OR REPLACE INTO pass VALUES(?, ?)',
            (key_hash, password))
        return self.db.commit()


if __name__ == '__main__':
    start_time = time.time()

    parser = ArgumentParser(description="create or restore file trunks")
    parser.add_argument(
        'name',
        help='name of the file trunk')
    parser.add_argument(
        '-s', action='store', dest='src',
        help='source directory containing files to be backed or *.frunk file \
to be restored when using option -r')
    parser.add_argument(
        '-d', action='store', dest='dst',
        help='destination directory where the backup will be written \
or restored when using option -r')
    parser.add_argument(
        '-r', '--restore', action='store_true',
        help='restore backup')

    args = parser.parse_args()

    name = args.name.split()[0]
    trunks = os.path.expanduser('~/.ftrunk')
    if not os.path.isdir(trunks):
        os.mkdir(trunks, 0o700)

    db = os.path.join(trunks, '%s.ftrunk' % name)
    db = sqlite3.connect(db)
    db.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

    _c = db.cursor()

    # if ftrunk database exists, try to use it and just backup new files
    # without need to specify src, dst
    try:
        _c.execute('SELECT value FROM config where key="src"')
        src = _c.fetchone()[0]
        _c.execute('SELECT value FROM config where key="dst"')
        dst = _c.fetchone()[0]
    except Exception as e:
        src = dst = None
        if not args.src or not args.dst:
            exit('-s source and -d destination are required')

    # set src using -s arg or use the existing one declared on the trunk db
    if args.src:
        args.src = os.path.abspath(os.path.expanduser(args.src))
        if src and src != args.src:
            exit('Trunk already exists')
        src = args.src

    # check that src dir exists
    if not os.path.isdir(src):
        exit('%s - Source directory does not exists' % src)

    # set dst using -d arg or use the existing one declared on the trunk db
    if args.dst:
        args.dst = os.path.abspath(os.path.expanduser(args.dst))
        if dst and dst != args.dst:
            exit('Trunk already exists')
        dst = args.dst

    # check that dst dir exists
    if not os.path.isdir(dst):
        if not os.path.isdir(os.path.abspath(os.path.join(dst, os.pardir))):
            exit('%s - Destination directory does not exists' % dst)
        os.mkdir(dst, 0o700)

    if args.restore:
        exit('---- restore ---- pending')

    ft = Ftrunk(db, src, dst)
    ft.build()
    ft.save_trunk(ft.trunk['dirs'] + ft.trunk['files'])

    for f_ in ft.trunk['files']:
        f_hash, f_list, f_size = f_
        if f_size:
            f_list = json.loads(f_list)
            psw = ft.backup(
                os.path.join(ft.src, f_list[0].lstrip(os.sep)),
                f_hash)
            if psw:
                print psw
                ft.save_password(f_hash, psw)

    print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
