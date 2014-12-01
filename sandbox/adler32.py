from zlib import adler32
from random import randint


def rand_data(l=4):
    return ''.join([chr(randint(0, 255)) for i in xrange(l)])
# end def rand_data(l = 4)


def generateAdler32Collision(data):
    y = list(data)
    adler_input = adler32(data)
    y[-1] = chr((ord(y[-1]) + 1) % 256)
    y[-3] = chr((ord(y[-3]) + 1) % 256)
    y[-2] = chr((ord(y[-2]) - 2) % 256)
    output = ''.join(y)
    adler_output = adler32(output)
    if adler_output == adler_input:
        return output
    # end if
    # print 'Looking for a different collision'
    y[-1] = chr((ord(data[-1]) - 1) % 256)
    y[-3] = chr((ord(data[-3]) - 1) % 256)
    y[-2] = chr((ord(data[-2]) + 2) % 256)
    output = ''.join(y)
    adler_output = adler32(output)
    if adler_output == adler_input:
        return output
    # end if
    # print 'I give up'
    return None
# end def generateAdler32Collision(data)

a = 'A' * 1013 + 'test'
adler32(a) == -1892941051
adler32(generateAdler32Collision(a)) == -1892941051
generateAdler32Collision(a) != a
generateAdler32Collision(a) == ('A' * 1013 + 'tfqu')

successes = 0
failures = 0
for i in xrange(1 << 24):
    a = rand_data(1024)
    a32 = adler32(a)
    coll = generateAdler32Collision(a)
    if coll is None or coll == a:
        failures += 1
        continue
    # end if
    successes += 1
# next i
print 'Successes:', successes
print 'Failures: ', failures
print 'Success rate:', (1.0 * successes) / (successes + failures)
