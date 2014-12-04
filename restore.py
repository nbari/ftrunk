from base64 import b64decode
from crypt import Crypt

p = b64decode('paswd')
x = Crypt(p)

src = '/path/to/src'

with open(src, 'rb') as in_file, open('/tmp/out.bz2', 'wb') as out_file:
    x.decrypt(in_file, out_file)
