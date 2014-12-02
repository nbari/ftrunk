import hashlib
import time

from multiprocessing import Pool, Process, Queue
from multiprocessing.pool import ThreadPool


start_time = time.time()


def reader(queue):
    h = hashlib.sha256()
    while True:
        msg = queue.get()
        if msg == 'DONE':
            break
        h.update(msg)
    print h.hexdigest()
#    return h.hexdigest()

queue = Queue()
processes = [Process(target=reader, args=(queue,)) for x in range(3)]
for p in processes:
    p.start()
#reader_p.start()

with open('/tmp/1gfile', 'rb') as f:
    for chunk in iter(lambda: f.read(4096), b''):
        queue.put(chunk)
    queue.put('DONE')

for p in processes:
    p.join()

# main()
# print sha256sum('/tmp/1gfile')

print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
