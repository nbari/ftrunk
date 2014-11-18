import shelve
import sqlite3


def test_sqlite3():
    conn = sqlite3.connect("debug.s3db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS [mydict] ("
                "[key] VARCHAR(255) PRIMARY KEY NOT NULL, "
                "[value] VARCHAR(255) NOT NULL)")
    for i in xrange(0, 1000000):
        cur.execute("INSERT INTO [mydict] (key, value) VALUES (?, ?)",
                    (str(i), str(i * 2)))
    conn.commit()
    cur.close()
    conn.close()


def test_shelve():
    d = shelve.open("debug.shelf")
    for i in xrange(0, 1000000):
        d[str(i)] = str(i * 2)
    d.close()

test_shelve()
