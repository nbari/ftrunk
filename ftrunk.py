import hashlib
import json
import os
import sqlite3
import time


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

    def get(self, key):
        c = self.connection.cursor()
        c.execute("select * from files where key = ?", (key, ))
        value = c.fetchone()
        if not value:
            raise KeyError(key)
        return value[1]

    def sha256_for_file(self, path, block_size=4096):
        h = hashlib.sha256()
        s = 0
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                s += len(chunk)
                h.update(chunk)
        return (h.hexdigest(), s)

    def read_dir(self, path):
        for path, _, files in os.walk(path):
            current_path = os.path.join(path)[len(self.path):]
            if current_path:
                self.trunk[current_path] = (None, 0)
            for f in files:
                filename = os.path.join(path, f)
                if os.path.isfile(filename):
                    try:
                        h, size = self.sha256_for_file(filename)
                    except Exception as e:
                        print e
                    else:
                        filename = filename[len(self.path):]
                        exists = self.trunk.get(h, False)
                        print size
                        if exists:
                            print 'hash: %s in trunk, size: %d' % (h, size)
                            try:
                                files = json.loads(exists)
                            except Exception:
                                files = [exists]

                            files.append(filename)
                            self.trunk[h] = (json.dumps(files), size)
                        else:
                            self.trunk[h] = (filename, size)

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
