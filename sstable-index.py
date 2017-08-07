#!/usr/bin/env python3

import argparse
import binascii
import struct

cmdline_parser = argparse.ArgumentParser()
cmdline_parser.add_argument('index_file', nargs='+', help='index file to parse')
cmdline_parser.add_argument('--summary', action='store_true', help='generate a summary instead of full output')

args = cmdline_parser.parse_args()

class FullReport:
    def report_file(self, file):
        print('Parsing {}'.format(file))
    def begin_entries(self):
        print('Index entries:')
    def report_partition_start(self, key, position, promoted_length):
        print('\tKey:\t\t\t{}\n\tPosition:\t\t{}\n\tPromoted length:\t{}'.format(binascii.hexlify(key), position, promoted_length))
    def report_promoted_start(self, deletion_time, timestamp, entries_count):
        print('\tDeletion time:\t\t{}\n\tTimestamp:\t\t{}\n\tEntries count:\t\t{}'.format(deletion_time, timestamp, entries_count))
    def report_promoted_entry(self, start, end, entry_offset, width):
        print('\t\tStart:\t{}\n\t\tEnd:\t{}\n\t\tOffset:\t{}\n\t\tLength:\t{}\n'.format(binascii.hexlify(start), binascii.hexlify(end), entry_offset, width))
    def report_partition_end(self):
        print('')
    def report_end(self, partitions):
        print('Total partitions:\t\t\t{}'.format(partitions))

class SummaryReport:
    def report_file(self, file):
        self.file = file
    def begin_entries(self):
        pass
    def report_partition_start(self, key, position, promoted_length):
        self.key = key
        self.position = position
        self.promoted_length = promoted_length
    def report_promoted_start(self, deletion_time, timestamp, entries_count):
        pass
    def report_promoted_entry(self, start, end, entry_offset, width):
        pass
    def report_partition_end(self):
        print('{:12} {:9} {} {}'.format(self.position, self.promoted_length, binascii.hexlify(self.key), self.file))
    def report_end(self, partitions):
        pass
    
        
reporter = FullReport()
if args.summary:
    reporter = SummaryReport()

for index_file in args.index_file:
  reporter.report_file(index_file)

  data = open(index_file, 'rb').read()

  offset = 0
  size = len(data)

  reporter.begin_entries()
  partitions = 0

  while offset < size:
    partitions += 1

    key_length = struct.unpack_from('>h', data, offset)[0]
    key = struct.unpack_from('>{}s'.format(key_length), data, offset + 2)[0]
    offset += key_length + 2

    (position, promoted_length) = struct.unpack_from('>ql', data, offset)
    offset += 12

    reporter.report_partition_start(key, position, promoted_length)

    if promoted_length:
        (deletion_time, timestamp, entries_count) = struct.unpack_from('>lql', data, offset)
        offset += 16

        reporter.report_promoted_start(deletion_time, timestamp, entries_count)

        for _ in range(0, entries_count):
            start_length = struct.unpack_from('>h', data, offset)[0]
            start = struct.unpack_from('>{}s'.format(start_length), data, offset + 2)[0]
            offset += start_length + 2

            end_length = struct.unpack_from('>h', data, offset)[0]
            end = struct.unpack_from('>{}s'.format(end_length), data, offset + 2)[0]
            offset += end_length + 2

            (entry_offset, width) = struct.unpack_from('>qq', data, offset)
            offset += 16

            reporter.report_promoted_entry(start, end, entry_offset, width)
    reporter.report_partition_end()

  reporter.report_end(partitions)
