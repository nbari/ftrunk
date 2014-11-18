import hashlib
import os
import shelve
import sqlite3
import sys
import time


class Database:

    def __init__(self, filename='data.sqlite'):
        self.connection = sqlite3.connect(filename)
        self.connection.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
        c = self.connection.cursor()
        c.execute('PRAGMA synchronous=OFF')
        c.execute('PRAGMA temp_store=MEMORY')
        c.execute('PRAGMA journal_mode=MEMORY')
        c.execute('create table if not exists files (key text, value text)')
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
        #c = self.connection.cursor()
        #c.execute("insert into files values (?, ?)", (key, value))
        #self.connection.commit()
        self.lst.append((key, value))


class ShelveDatabase:

    def __init__(self, filename="data.shelve"):
        self.db = shelve.open(filename)

    def get(self, key):
        return self.db[key]

    def put(self, key, value):
        self.db[key] = value
        self.db.sync()


def sha256_for_file(path, blocksize=256 * 128):
    '''
    Block size directly depends on the block size of your filesystem
    to avoid performances issues
    Here I have blocks of 4096 octets (Default NTFS)
    '''
    sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(blocksize), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def read(db):
    count = 0
    #for path, dirs, files in os.walk(os.path.expanduser(".")):
    for path, dirs, files in os.walk(os.path.expanduser("~")):
        for f in files:
            filename = os.path.join(path, f)
            if os.path.isfile(filename):
                try:
                    h = sha256_for_file(filename)
                except Exception as e:
                    print e
                else:
                    count += 1
                    db.put(h, filename)
                    if count % 100 == 0:
                        print "{:,d}".format(count)


if __name__ == '__main__':
    time_start = time.time()
    if len(sys.argv) == 1:
        # sqlite
        print 'sqlite3'
        db = Database()
        read(db)
        c = db.connection.cursor()
        c.executemany("insert into files values (?, ?)", db.lst)
        db.connection.commit()

    #    c = db.connection.cursor()
    #    for row in c.execute('select * from files'):
    #        assert row
    else:
        # shelve
        print 'shelve'
        db = ShelveDatabase()
        read(db)
    #    for i in db.db:
    #        assert db.get(i)

    print time.time() - time_start
