from base64 import b64decode
from crypt import Crypt

p = b64decode('=i')
x = Crypt(p)

src = '/Users/nbari/.ftrunk/eb/c5/99/ebc5999af324a1f4272e15795690cb979dbea921a56fbd4957f5f83055bb7c04363d35bf475856637f0ff0929400507f1e998b7c709dae1de53748ab5f3381ca'

with open(src, 'rb') as in_file, open('/tmp/out.bz2', 'wb') as out_file:
    x.decrypt(in_file, out_file)
