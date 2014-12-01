import hashlib
import time

from multiprocessing import Pool, Process, Queue
from multiprocessing.pool import ThreadPool


start_time = time.time()


#def sha256sum(filename, block_size=2 ** 12):
    #sha = hashlib.sha256()
    #with open(filename, 'rb') as f:
        #for chunk in iter(lambda: f.read(block_size), b''):
            #sha.update(chunk)
    #return sha.hexdigest()



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
reader_p = Process(target=reader, args=(queue,))
reader_p.daemon = True
reader_p.start()

with open('/tmp/1gfile', 'rb') as f:
    for chunk in iter(lambda: f.read(4096), b''):
        queue.put(chunk)
    queue.put('DONE')

reader_p.join()


# main()
#print sha256sum('/tmp/1gfile')

print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
