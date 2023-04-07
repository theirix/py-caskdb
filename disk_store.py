"""
disk_store module implements DiskStorage class which implements the KV store on the
disk

DiskStorage provides two simple operations to get and set key value pairs. Both key and
value needs to be of string type. All the data is persisted to disk. During startup,
DiskStorage loads all the existing KV pair metadata.  It will throw an error if the
file is invalid or corrupt.

Do note that if the database file is large, then the initialisation will take time
accordingly. The initialisation is also a blocking operation, till it is completed
the DB cannot be used.

Typical usage example:

    disk: DiskStorage = DiskStore(file_name="books.db")
    disk.set(key="othello", value="shakespeare")
    author: str = disk.get("othello")
    # it also supports dictionary style API too:
    disk["hamlet"] = "shakespeare"
"""
import os.path
import typing
import datetime
from dataclasses import dataclass

from format import encode_kv, decode_kv, decode_header, HEADER_SIZE


# DiskStorage is a Log-Structured Hash Table as described in the BitCask paper. We
# keep appending the data to a file, like a log. DiskStorage maintains an in-memory
# hash table called KeyDir, which keeps the row's location on the disk.
#
# The idea is simple yet brilliant:
#   - Write the record to the disk
#   - Update the internal hash table to point to that byte offset
#   - Whenever we get a read request, check the internal hash table for the address,
#       fetch that and return
#
# KeyDir does not store values, only their locations.
#
# The above approach solves a lot of problems:
#   - Writes are insanely fast since you are just appending to the file
#   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
#       storage, there could be 2-3 disk seeks
#
# However, there are drawbacks too:
#   - We need to maintain an in-memory hash table KeyDir. A database with a large
#       number of keys would require more RAM
#   - Since we need to build the KeyDir at initialisation, it will affect the startup
#       time too
#   - Deleted keys need to be purged from the file to reduce the file size
#
# Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf


@dataclass(frozen=True)
class KeyDirEntry:
    file_name: str
    size: int
    pos: int
    tstamp: int


class KeyDir:
    def __init__(self):
        self._dir = dict()

    def get(self, key: str) -> typing.Optional[KeyDirEntry]:
        return self._dir.get(key)

    def set(self, key: str, entry: KeyDirEntry) -> None:
        self._dir[key] = entry

    def delete(self, key: str) -> None:
        if key in self._dir:
            del self._dir[key]


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db"):
        self._file_name = file_name
        if os.path.isfile(file_name):
            self._file = open(file_name, "r+b")
        else:
            self._file = open(file_name, "w+b")
        self._keydir = KeyDir()

        # Determine size
        self._file.seek(0, 2)
        self._size = self._file.tell()

        self._fill_keydir()

    def _fill_keydir(self) -> None:
        print(f"Fill keydir initially size={self._size}")
        pos = 0
        while pos < self._size:
            # Read header
            self._file.seek(pos + 4)
            header = self._file.read(HEADER_SIZE)
            timestamp, key_size, value_size = decode_header(header)
            print(f"From {pos=} read header {timestamp=} {key_size=} {value_size=}")

            # Re-read whole entry
            self._file.seek(pos)
            read_size = 4 + HEADER_SIZE + key_size + value_size
            data = self._file.read(read_size)
            print(f"From {pos=} read data {read_size=}")

            timestamp, key, _ = decode_kv(data)

            entry = KeyDirEntry(
                pos=pos,
                size=key_size + value_size,
                tstamp=timestamp,
                file_name=self._file_name,
            )
            self._keydir.set(key, entry)

            print(f"init keydir key={key} entry={entry}")

            pos += read_size

        assert pos == self._size

    def _timestamp(self) -> int:
        return round(datetime.datetime.utcnow().timestamp())

    def set(self, key: str, value: str) -> None:
        timestamp = self._timestamp()
        size, data = encode_kv(timestamp, key, value)

        offset = self._file.seek(self._size)
        print(f"set seek to {offset}")
        # print(f"write size {size+4} bytes, data {data.hex()}")
        self._file.write(data)
        write_size = 4 + HEADER_SIZE + size
        self._size += write_size

        entry = KeyDirEntry(
            pos=offset, size=size, tstamp=timestamp, file_name=self._file_name
        )
        self._keydir.set(key, entry)

        print(f"set keydir key={key} entry={entry}, size so far {self._size}")

    def get(self, key: str) -> str:
        entry = self._keydir.get(key)
        if entry is None:
            return ""

        print(f"get seek to {entry.pos} with size {entry.size}")
        self._file.seek(entry.pos)
        read_size = 4 + HEADER_SIZE + entry.size
        data = self._file.read(read_size)
        # print(f"read size {entry.size+4} bytes, data {data.hex()}")
        timestamp, read_key, read_value = decode_kv(data)
        if key != read_key:
            raise ValueError(f"Different keys: keydir {key}, disk {read_key}")
        return read_value

    def delete(self, key: str) -> None:
        self.set(key, '')
        self._keydir.delete(key)

    def close(self) -> None:
        self._file.flush()
        self._file.close()

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)
