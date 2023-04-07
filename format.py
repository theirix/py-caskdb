"""
format module provides encode/decode functions for serialisation and deserialisation
operations

format module is generic, does not have any disk or memory specific code.

The disk storage deals with bytes; you cannot just store a string or object without
converting it to bytes. The programming languages provide abstractions where you don't
have to think about all this when storing things in memory (i.e. RAM). Consider the
following example where you are storing stuff in a hash table:

    books = {}
    books["hamlet"] = "shakespeare"
    books["anna karenina"] = "tolstoy"

In the above, the language deals with all the complexities:

    - allocating space on the RAM so that it can store data of `books`
    - whenever you add data to `books`, convert that to bytes and keep it in the memory
    - whenever the size of `books` increases, move that to somewhere in the RAM so that
      we can add new items

Unfortunately, when it comes to disks, we have to do all this by ourselves, write
code which can allocate space, convert objects to/from bytes and many other operations.

format module provides two functions which help us with serialisation of data.

    encode_kv - takes the key value pair and encodes them into bytes
    decode_kv - takes a bunch of bytes and decodes them into key value pairs

**workshop note**

For the workshop, the functions will have the following signature:

    def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]
    def decode_kv(data: bytes) -> tuple[int, str, str]
"""
import zlib
from struct import pack, unpack

# Record layout
# CRC | header | key | value

# Header layout:
# timestamp | key_size | value_size

HEADER_SIZE = 4 * 3


def encode_header(timestamp: int, key_size: int, value_size: int) -> bytes:
    if key_size < 0 or value_size < 0:
        raise ValueError("Wrong size")
    return pack("!LLL", timestamp, key_size, value_size)


def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]:
    bkey = key.encode("utf-8")
    bvalue = value.encode("utf-8")

    # Calculate crc
    crc = zlib.crc32(pack("!L", timestamp) + bkey + bvalue)
    crc_bytes = pack("!L", crc)

    header = encode_header(timestamp, len(bkey), len(bvalue))

    data = crc_bytes + header + bkey + bvalue

    return len(bkey) + len(bvalue), data


def decode_kv(data: bytes) -> tuple[int, str, str]:
    actual_crc = unpack("!L", data[0:4])[0]
    timestamp, key_size, value_size = decode_header(data[4 : 4 + HEADER_SIZE])

    bkey = data[4 + HEADER_SIZE : 4 + HEADER_SIZE + key_size]
    bvalue = data[4 + HEADER_SIZE + key_size : 4 + HEADER_SIZE + key_size + value_size]

    # Calculate crc
    crc = zlib.crc32(pack("!L", timestamp) + bkey + bvalue)

    if crc != actual_crc:
        raise ValueError("Wrong CRC")

    key = bkey.decode("utf-8")
    value = bvalue.decode("utf-8")

    return timestamp, key, value


def decode_header(data: bytes) -> tuple[int, int, int]:
    unpacked = unpack("!LLL", data)
    timestamp = unpacked[0]
    key_size = unpacked[1]
    value_size = unpacked[2]
    return timestamp, key_size, value_size
