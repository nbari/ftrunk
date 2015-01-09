#!/usr/bin/env python

import bz2
import hashlib
import hmac
import json
import os
import random
import sqlite3
import sys
import time

from Crypto.Cipher import AES
from argparse import ArgumentParser
from base64 import b64encode, b64decode
from multiprocessing import Pool
from tempfile import SpooledTemporaryFile


def checksum512(path, block_size=4096):
    try:
        sys.stdout.write('\033[K')
        sys.stdout.write('\rProcessing: [%s]' % path)
        sys.stdout.flush()
        with open(path, 'rb') as f:
            h = hashlib.sha512()
            s = 0
            for chunk in iter(lambda: f.read(block_size), b''):
                s += len(chunk)
                h.update(chunk)
        return h.hexdigest(), path, s
    except Exception:
        return None


def encrypt(in_file, out_file):
    # out_file: iv + AES encrypted file
    #aes_key = os.urandom(32)
    aes_key = 'p' * 32
#    iv = ''.join(chr(random.randint(0, 0xFF)) for i in range(16))
    iv = 'i' * 16
    chiper = AES.new(aes_key, AES.MODE_CBC, iv)
    BS = 16
    out_file.write(iv)
    in_file.seek(0)
    sig = hmac.new(aes_key, iv, digestmod=hashlib.sha1)
    pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
    for chunk in iter(lambda: in_file.read(1024 * BS), b''):
        chunk = pad(chunk)
        chunk = chiper.encrypt(chunk)
        sig.update(chunk)
        out_file.write(chunk)
    return b64encode(aes_key + sig.digest())


def decrypt(key, in_filename, out_filename):
    with open(in_filename) as in_file:
        decoded_key = b64decode(key)
        aes_key = decoded_key[:32]
        expected_sig = decoded_key[32:]
        print aes_key, expected_sig.encode('hex')
        BS = 16
        iv = in_file.read(BS)
        sig = hmac.new(aes_key, iv, digestmod=hashlib.sha1)
        cipher = AES.new(aes_key, AES.MODE_CBC, iv)
        unpad = lambda s: s[:-ord(s[-1])]
        with open(out_filename, 'wb') as out_file:
            for chunk in iter(lambda: in_file.read(1024 * BS), b''):
                sig.update(chunk)
                chunk = cipher.decrypt(chunk)
                out_file.write(unpad(chunk))

            # if not hmac.compare_digest(expected_sig, sig.digest()):
            if expected_sig != sig.digest():
                raise ValueError('Bad file signature!')
            print sig.hexdigest()

with open('../README.md') as in_file:
    with open('README.md.enc', 'wb') as out_file:
        rs = encrypt(in_file, out_file)
b64_data = b64decode(rs)
aes_key = b64_data[:32]
sig = b64_data[32:]
print sig.encode('hex'), aes_key

decrypt(rs, 'README.md.enc', 'ok')
