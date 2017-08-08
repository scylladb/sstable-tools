#!/usr/bin/python3

import binascii
import struct

class Stream:
    def __init__(self, data):
        self.data = data
        self.offset = 0
    def int16(self):
        (val,) = struct.unpack_from('>h', self.data, self.offset)
        self.offset += 2
        return val
    def int32(self):
        (val,) = struct.unpack_from('>l', self.data, self.offset)
        self.offset += 4
        return val
    def int64(self):
        (val,) = struct.unpack_from('>q', self.data, self.offset)
        self.offset += 8
        return val
    def bytes16(self):
        len = self.int16()
        val = self.data[self.offset:self.offset + len]
        self.offset += len
        return val
    def string16(self):
        return self.bytes16().decode('utf-8')
    def map16(self, keytype=string16, valuetype=string16):
        return {self.keytype(): self.valuetype() for _ in range(self.int16())}
    def map32(self, keytype=string16, valuetype=string16):
        return {keytype(self): valuetype(self) for _ in range(self.int32())}
    def array32(self, valuetype):
        return [valuetype() for _ in range(self.int32())]
