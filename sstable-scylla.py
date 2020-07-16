#!/usr/bin/env python3

import argparse
import json
import sstable_tools.scylla


cmdline_parser = argparse.ArgumentParser()
cmdline_parser.add_argument('scylla_file', help='scylla component file to parse')
cmdline_parser.add_argument('-f', '--format', choices=['ka', 'la', 'mc'], default='mc', help='sstable format')

args = cmdline_parser.parse_args()

with open(args.scylla_file, 'rb') as f:
    data = f.read()

metadata = sstable_tools.scylla.parse(data, args.format)

print(json.dumps(metadata, indent=4))
