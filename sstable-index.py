#!/usr/bin/env python3

import argparse
import binascii
import struct

cmdline_parser = argparse.ArgumentParser()
cmdline_parser.add_argument('index_file', help='index file to parse')

args = cmdline_parser.parse_args()

f = open(args.index_file, 'rb')
data = f.read()
f.close()

offset = 0
size = len(data)

print('Index entries:')
partitions = 0

while offset < size:
    partitions += 1

    key_length = struct.unpack_from('>h', data, offset)[0]
    key = struct.unpack_from('>{}s'.format(key_length), data, offset + 2)[0]
    offset += key_length + 2

    (position, promoted_length) = struct.unpack_from('>ql', data, offset)
    offset += 12

    print('\tKey:\t\t\t{}\n\tPosition:\t\t{}\n\tPromoted length:\t{}'.format(binascii.hexlify(key), position, promoted_length))

    if promoted_length:
        (deletion_time, timestamp, entries_count) = struct.unpack_from('>lql', data, offset)
        offset += 16

        print('\tDeletion time:\t\t{}\n\tTimestamp:\t\t{}\n\tEntries count:\t\t{}'.format(deletion_time, timestamp, entries_count))

        for _ in range(0, entries_count):
            start_length = struct.unpack_from('>h', data, offset)[0]
            start = struct.unpack_from('>{}s'.format(start_length), data, offset + 2)[0]
            offset += start_length + 2

            end_length = struct.unpack_from('>h', data, offset)[0]
            end = struct.unpack_from('>{}s'.format(end_length), data, offset + 2)[0]
            offset += end_length + 2

            (entry_offset, width) = struct.unpack_from('>qq', data, offset)
            offset += 16

            print('\t\tStart:\t{}\n\t\tEnd:\t{}\n\t\tOffset:\t{}\n\t\tLength:\t{}\n'.format(binascii.hexlify(start), binascii.hexlify(end), entry_offset, width))
    print('')

print('Total partitions:\t\t\t{}'.format(partitions))
