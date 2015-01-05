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

    def __init__(self, path, tname=None):
        self.path = path
        self.trunkname = tname if tname else os.path.basename(path)
        self.version = int(time.time())
        self.connection = sqlite3.connect('%s.ftrunk' % self.trunkname)
        self.connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
        c = self.connection.cursor()
        c.execute('PRAGMA temp_store=MEMORY')

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

#        self.trunk['files'] = [(k, json.dumps(v[0][0]), v[0][1])
#                               for k, v in self.trunk['files'].iteritems()]

    def backup(self, filename, filehash):
        backup_dir = os.path.join(
            self.ftrunk_dir,
            filehash[:2],
            filehash[2:4],
            filehash[4:6])

        backup_file_path = os.path.join(backup_dir, filehash)

        print backup_dir, backup_file_path

        if os.path.exists(backup_file_path):
            print 'Bye I already have the file'
            return

        # if no file create the parent dir
        os.makedirs(backup_dir)

        try:
            with open(filename, 'rb') as in_file:
                with SpooledTemporaryFile(suffix='.tmp', dir=self.ftrunk_dir) \
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
        except Exception:
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

    def save(self):
        c = self.connection.cursor()
        c.executemany(
            'INSERT INTO trunk VALUES (?, ?, ?, ?)',
            [(k, v[0], v[1], self.version) for k, v in self.trunk.iteritems()])
        return self.connection.commit()


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

    for file_k, file_v in ft.trunk['files'].iteritems():
        #        print file_k, file_v[0][0][0], file_v[0][1]
        x = ft.backup(file_v[0][0][0], file_k)
        print x
        exit()

    print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
