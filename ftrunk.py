import bz2
import hashlib
import json
import os
import sqlite3
import time

from shutil import copyfileobj


class Ftrunk(object):

    def __init__(self, path):
        self.path = os.path.abspath(os.path.expanduser(path))
        if not os.path.isdir(self.path):
            return
        self.trunkname = os.path.basename(path)
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

    def sha256_and_size(self, path, block_size=4096):
        h = hashlib.sha256()
        s = 0
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                s += len(chunk)
                h.update(chunk)
        return (h.hexdigest(), s)

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
        with open(filename, 'rb') as file_input:
            with bz2.BZ2File(backup_file_path, 'wb', compresslevel=9) as output:
                copyfileobj(file_input, output)

    def read_dir(self, path):
        for path, _, files in os.walk(path):
            current_path = os.path.join(path)[len(self.path):]
            if current_path:
                self.trunk[current_path] = (None, 0)
            for f in files:
                filename = os.path.join(path, f)
                if os.path.isfile(filename):
                    try:
                        h, size = self.sha256_and_size(filename)
                    except Exception as e:
                        print e
                    else:
                        exists = self.trunk.get(h, False)
                        if exists:
                            print 'hash: %s in trunk, size: %d' % (h, size)
                            try:
                                files = json.loads(exists)
                            except Exception:
                                files = [exists[0]]

                            files.append(filename[len(self.path):])
                            self.trunk[h] = (json.dumps(files), size)
                            # continue to avoid duplicates
                            continue
                        else:
                            self.trunk[h] = (filename[len(self.path):], size)

                        # backup the file only if size > 0
                        if size:
                            self.backup(filename, h)

    def save(self):
        c = self.connection.cursor()
        c.executemany(
            'INSERT INTO trunk VALUES (?, ?, ?, ?)',
            [(k, v[0], v[1], self.version) for k, v in self.trunk.iteritems()])
        return self.connection.commit()


if __name__ == '__main__':
    time_start = time.time()
    ftrunk = Ftrunk('root')
    ftrunk.read_dir(ftrunk.path)
    ftrunk.save()
    print time.time() - time_start
