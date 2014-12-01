import multiprocessing
import hashlib
import time
import zlib

from multiprocessing import Pool
from multiprocessing.pool import ThreadPool


start_time = time.time()

def adler32sum(filename="big.txt", block_size=2 ** 16):
    checksum = 0
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            checksum = zlib.adler32(chunk, checksum)
            # Oxffffffff forces checksum to be in range 0 to 2**32-1
    return str(checksum & 0xffffffff)

def crc32sum(filename="big.txt", block_size=2 ** 16):
    checksum = 0
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            checksum = zlib.crc32(chunk, checksum)
            # Oxffffffff forces checksum to be in range 0 to 2**32-1
    return str(checksum & 0xffffffff)


def sha256sum(filename="big.txt", block_size=2 ** 16):
    sha = hashlib.sha256()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(block_size), b''):
            sha.update(chunk)
    return sha.hexdigest()



# main()
#print sha256sum('/tmp/10gfile')
print adler32sum('/tmp/10gfile')
#print crc32sum('/tmp/10gfile')

print '\n' + 'Elapsed time: ' + str(time.time() - start_time)
