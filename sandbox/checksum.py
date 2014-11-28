import multiprocessing
import hashlib
import time


class ChecksumPipe(multiprocessing.Process):

    all_open_parent_conns = []

    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.summer = hashlib.sha256()
        self.child_conn, self.parent_conn = multiprocessing.Pipe(duplex=False)
        ChecksumPipe.all_open_parent_conns.append(self.parent_conn)
        self.result_queue = multiprocessing.Queue(1)
        self.daemon = True
        self.start()
        self.child_conn.close()  # This is the parent. Close the unused end.

    def run(self):
        for conn in ChecksumPipe.all_open_parent_conns:
            conn.close()  # This is the child. Close unused ends.
        while True:
            try:
#                print "Waiting for more data...", self
                block = self.child_conn.recv_bytes()
#               print "Got some data...", self
            except EOFError:
#                print "Finished work", self
                break
            self.summer.update(block)
        self.result_queue.put(self.summer.hexdigest())
        self.result_queue.close()
        self.child_conn.close()

    def update(self, block):
        self.parent_conn.send_bytes(block)

    def hexdigest(self):
        self.parent_conn.close()
        return self.result_queue.get()


def main():
    # Calculating the first checksum works
    with open('/tmp/test1g', 'rb') as rf:
        sha256 = ChecksumPipe()
        for chunk in iter(lambda: rf.read(4096), b''):
            sha256.update(chunk)
    print "sha256 is", sha256.hexdigest()

def sha256_for_file(path, block_size=4096):
    try:
        with open(path, 'rb') as rf:
            h = hashlib.sha256()
            pool = multiprocessing.Pool()
            for chunk in iter(lambda: rf.read(block_size), b''):
                #h.update(chunk)
                results = pool.map_async(h.update, chunk)
        print results
        #return h.hexdigest(), path
        return h.hexdigest(), path
    except IOError:
        return None, path

#main()
start_time = time.time()

#main()
print sha256_for_file('/tmp/test1g')

print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
