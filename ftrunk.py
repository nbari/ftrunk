import hashlib
import itertools
import operator
import os
import sqlite3
import time


class Ftrunk(object):

    def __init__(self, filename='map.ftrunk'):
        self.connection = sqlite3.connect(filename)
        self.connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
        c = self.connection.cursor()
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA temp_store=MEMORY')
        c.execute('PRAGMA journal_mode=MEMORY')
        query = """CREATE TABLE IF NOT EXISTS trunk (
            hash text,
            file text,
            PRIMARY KEY(hash)
        )"""
        c.execute(query)
        c.execute('SELECT EXISTS (SELECT 1 FROM trunk)')
        self.is_empty = False if c.fetchone()[0] else True
        self.connection.commit()
        self.lst = list()

    def get(self, key):
        c = self.connection.cursor()
        c.execute("select * from files where key = ?", (key, ))
        value = c.fetchone()
        if not value:
            raise KeyError(key)
        return value[1]

    def put(self, key, value):
        self.lst.append((key, value))

    def sha256_for_file(self, path, block_size=4096):
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                h.update(chunk)
        return h.hexdigest()

    def read_dir(self, path):
        print self.is_empty
        # for path, dirs, files in os.walk(os.path.expanduser(".")):
        # for path, dirs, files in os.walk(os.path.expanduser("~")):
        for path, _, files in os.walk(unicode(path)):
            current_path = os.path.join(path)
            if current_path in itertools.imap(
                    operator.itemgetter(0),
                    self.lst):
                print 'current_path in list'
            self.put(current_path, current_path)
            for f in files:
                filename = os.path.join(path, f)
                if os.path.isfile(filename):
                    try:
                        h = self.sha256_for_file(filename)
                    except Exception as e:
                        print e
                    else:
                        self.put(h, filename)
        c = self.connection.cursor()
        c.executemany("INSERT INTO trunk VALUES (?, ?)", self.lst)
        self.connection.commit()


if __name__ == '__main__':
    time_start = time.time()
    ftrunk = Ftrunk()
    ftrunk.read_dir('root')
    print time.time() - time_start
