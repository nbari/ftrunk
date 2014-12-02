import hashlib
import time
import Queue
import threading

start_time = time.time()


def reader(queue):
    h = hashlib.sha256()
    while True:
        msg = queue.get()
        if msg == 'DONE':
            break
        h.update(msg)
        print h.hexdigest()
    print h.hexdigest()

q = Queue.Queue()
t = threading.Thread(target=reader, args=(q,))
t.daemon = True
t.start()

with open('/tmp/1gfile', 'rb') as f:
    for chunk in iter(lambda: f.read(4096), b''):
        q.put(chunk)
    q.put('DONE')

# main()
# print sha256sum('/tmp/1gfile')

print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
