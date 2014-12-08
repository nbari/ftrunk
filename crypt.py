import random

from Crypto import Random
from Crypto.Cipher import AES


class Crypt(object):

    def __init__(self, password):
        self.password = password

    def encrypt(self, in_file, out_file):
        in_file.seek(0)
        bs = AES.block_size
        salt = Random.new().read(bs - len('Salted__'))
        key, iv = self.derive_key_and_iv(self.password, salt, 32, bs)
        iv = ''.join(chr(random.randint(0, 0xFF)) for i in range(16))
        cipher = AES.new(key, AES.MODE_CBC, iv)
        out_file.write('Salted__' + salt)
        finished = False
        while not finished:
            chunk = in_file.read(1024 * bs)
            if len(chunk) == 0 or len(chunk) % bs != 0:
                padding_length = (bs - len(chunk) % bs) or bs
                chunk += padding_length * chr(padding_length)
                finished = True
            out_file.write(cipher.encrypt(chunk))


    def decrypt(self, in_file, out_file):
        in_file.seek(0)
        bs = AES.block_size
        salt = in_file.read(bs)[len('Salted__'):]
        key, iv = self.derive_key_and_iv(self.password, salt, 32, bs)
        cipher = AES.new(key, AES.MODE_CBC, iv)
        next_chunk = ''
        finished = False
        while not finished:
            chunk, next_chunk = next_chunk, cipher.decrypt(
                in_file.read(
                    1024 * bs))
            if len(next_chunk) == 0:
                chunk = chunk.rstrip(chunk[-1])
                finished = True
            out_file.write(chunk)
