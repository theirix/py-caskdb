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
import json
import logging
import os.path
import typing
import datetime
from dataclasses import dataclass
from sortedcontainers import SortedDict  # type: ignore

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


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KeyDirEntry:
    file_id: int
    size: int
    pos: int
    tstamp: int


class KeyDir:
    """In-memory keydir structure.

    Stores all the keys.
    Allow quick traversal
    """

    def __init__(self) -> None:
        # RedBlack Tree-like structure
        self._dir = SortedDict()

    def get(self, key: str) -> KeyDirEntry | None:
        return self._dir.get(key)  # type: ignore

    def set(self, key: str, entry: KeyDirEntry) -> None:
        self._dir[key] = entry

    def delete(self, key: str) -> None:
        if key in self._dir:
            del self._dir[key]

    def range(self, start: str, end: str) -> typing.Iterable[str]:
        keys = self._dir.keys()
        start_idx = self._dir.bisect_left(start)
        if start_idx == len(self._dir):
            return
        for idx in range(start_idx, len(self._dir)):
            # noinspection PyUnresolvedReferences
            key = keys[idx]
            if key <= end:
                yield key
            else:
                break

    def keys(self) -> typing.Iterable[str]:
        return self._dir.keys()  # type: ignore


class Registry:
    """Represents mapping from file id to file path.

    Stored in JSON file in database directory.
    """

    def __init__(self, registry_name: str):
        self._registry_name = registry_name

        self._registry: dict[int, str] = dict()

        if os.path.isfile(registry_name):
            logger.info("Open existing registry {}".format(self._registry_name))

            with open(registry_name, "rt") as f:
                deserialized = json.load(f)
                self._registry = {
                    int(file_id_str): content
                    for file_id_str, content in deserialized.items()
                }
        logger.info(f"Registry of {len(self._registry)} elements")

    def save(self) -> None:
        """Saves metadata to file"""
        with open(self._registry_name, "wt") as f:
            serialized = {
                str(file_id): content for file_id, content in self._registry.items()
            }
            json.dump(serialized, f, indent=True)

    def empty(self) -> bool:
        return not self._registry

    def size(self) -> int:
        return len(self._registry)

    def sorted_key_ids(self) -> typing.Iterable[int]:
        """Provide all file ids in sorted order"""
        return sorted(self._registry.keys())

    def data_path(self, file_id: int) -> str:
        """Provide full path to file with id `file_id"""
        data_file = self._registry[file_id]
        data_path = os.path.join(os.path.dirname(self._registry_name), data_file)
        return data_path

    def active_file_id(self) -> int:
        """Return active file id"""
        if not self._registry:
            raise ValueError("Empty registry is not allowed")
        return max(self._registry.keys())

    def add_file(self) -> int:
        if self._registry:
            file_id = self.active_file_id() + 1
        else:
            file_id = 0

        data_file = f"data_{file_id:0>2}.bin"
        self._registry[file_id] = data_file

        if os.path.isfile(self.data_path(file_id)):
            raise RuntimeError(f"File {data_file} already exist for {file_id=}")

        logger.info(f"Added file {file_id}")

        return file_id

    def remove_file(self, file_id: int) -> None:
        """Disassociate and remove file `file_id"""
        logger.info(f"Removing file {file_id}")
        data_path = self.data_path(file_id)
        if os.path.isfile(data_path):
            os.remove(data_path)
        del self._registry[file_id]


class FileDescriptors:
    """Represents file desscriptor table."""

    def __init__(self, registry: Registry):
        self._registry = registry

        self._fds: dict[int, typing.BinaryIO] = {}

        for file_id in registry.sorted_key_ids():
            assert isinstance(file_id, int)
            data_path = registry.data_path(file_id)
            if os.path.isfile(data_path):
                # Open non-active files as read-only
                fd = open(data_path, "r+b")
            else:
                fd = open(data_path, "w+b")
            self._fds[file_id] = fd

    def file_obj(self, file_id: int) -> typing.BinaryIO:
        if file_id not in self._fds:
            raise ValueError(f"File {file_id} not opened")
        return self._fds[file_id]

    def open(self, file_id: int) -> None:
        """Open new file for writing"""
        logger.info(f"Open fd for file {file_id}")
        data_path = self._registry.data_path(file_id)
        fd = open(data_path, "w+b")
        self._fds[file_id] = fd

    def close(self, file_id: int) -> None:
        logger.info(f"Closing fd for file {file_id}")
        if file_id not in self._fds:
            raise ValueError(f"File {file_id} not opened")
        fd = self._fds[file_id]
        fd.flush()
        fd.close()
        del self._fds[file_id]


class DiskStorage:
    """
    Implements the KV store on the disk

    Args:
        file_name (str): name of the file where all the data will be written. Just
            passing the file name will save the data in the current directory. You may
            pass the full file location too.
    """

    def __init__(self, file_name: str = "data.db", max_size: int = -1):
        self._max_size = max_size

        # active file size
        self._size = 0

        # Registry for storing associations from fileid to filename
        self._registry = Registry(file_name)

        self._descriptors = FileDescriptors(self._registry)

        if self._registry.empty():
            logger.info("Add first file")
            self._registry_add_file()

        # Populate key dir
        self._keydir = KeyDir()
        for file_id in self._registry.sorted_key_ids():
            self._fill_keydir(file_id)

    def _registry_add_file(self) -> None:
        file_id = self._registry.add_file()

        # it is a new file
        self._descriptors.open(file_id)

        self._size = 0

        self._registry.save()

    def _fill_keydir(self, file_id: int) -> None:
        fd = self._descriptors.file_obj(file_id)

        # Determine size
        fd.seek(0, 2)
        self._size = fd.tell()

        logger.info(f"Fill keydir initially size={self._size}")
        pos = 0
        while pos < self._size:
            # Read header
            fd.seek(pos + 4)
            header = fd.read(HEADER_SIZE)
            timestamp, key_size, value_size = decode_header(header)
            logger.debug(
                f"From {pos=} read header {timestamp=} {key_size=} {value_size=}"
            )

            # Re-read whole entry
            fd.seek(pos)
            read_size = 4 + HEADER_SIZE + key_size + value_size
            data = fd.read(read_size)
            logger.debug(f"From {pos=} read data {read_size=}")

            timestamp, key, _ = decode_kv(data)

            entry = KeyDirEntry(
                pos=pos, size=key_size + value_size, tstamp=timestamp, file_id=file_id
            )
            self._keydir.set(key, entry)

            logger.debug(f"init keydir key={key} entry={entry}")

            pos += read_size

        assert pos == self._size

    def _timestamp(self) -> int:
        return round(datetime.datetime.utcnow().timestamp())

    def set(self, key: str, value: str) -> None:
        fd = self._descriptors.file_obj(self._registry.active_file_id())

        timestamp = self._timestamp()
        size, data = encode_kv(timestamp, key, value)

        offset = fd.seek(self._size)
        logger.debug(f"seek to {offset}")
        # logger.debug(f"write size {size+4} bytes, data {data.hex()}")
        fd.write(data)
        write_size = 4 + HEADER_SIZE + size
        self._size += write_size

        entry = KeyDirEntry(
            pos=offset,
            size=size,
            tstamp=timestamp,
            file_id=self._registry.active_file_id(),
        )
        self._keydir.set(key, entry)

        logger.debug(f"set keydir key={key} entry={entry}, size so far {self._size}")

        if self._max_size != -1 and self._size > self._max_size:
            self.split()

    def split(self) -> None:
        """Activate new file and move current active to non-active."""
        self._registry_add_file()

    def compact(self) -> None:
        """Compact all exisiting files to new active file."""

        # Save the iterable to list
        sorted_file_ids = list(self._registry.sorted_key_ids())

        # Add new file
        self._registry_add_file()

        # Iterate over all keys
        for key in self._keydir.keys():
            value = self.get(key)
            self.set(key, value)

        # Close and remove all previous files
        for file_id in sorted_file_ids:
            logger.info(f"Closing {file_id}")
            self._descriptors.close(file_id)
            self._registry.remove_file(file_id)

        self._registry.save()

    def get(self, key: str) -> str:
        entry = self._keydir.get(key)
        if entry is None:
            return ""

        fd = self._descriptors.file_obj(entry.file_id)
        logger.debug(f"get seek to {entry.pos} with size {entry.size}")
        fd.seek(entry.pos)
        read_size = 4 + HEADER_SIZE + entry.size
        data = fd.read(read_size)
        # logger.debug(f"read size {entry.size+4} bytes, data {data.hex()}")
        timestamp, read_key, read_value = decode_kv(data)
        if key != read_key:
            raise ValueError(f"Different keys: keydir {key}, disk {read_key}")
        return read_value

    def delete(self, key: str) -> None:
        self.set(key, "")
        self._keydir.delete(key)

    def close(self) -> None:
        for file_id in self._registry.sorted_key_ids():
            self._descriptors.close(file_id)

    def clean(self) -> None:
        for file_id in self._registry.sorted_key_ids():
            data_path = self._registry.data_path(file_id)
            if os.path.isfile(data_path):
                os.remove(data_path)

    def __setitem__(self, key: str, value: str) -> None:
        return self.set(key, value)

    def __getitem__(self, item: str) -> str:
        return self.get(item)

    def scan(self, start: str, end: str) -> typing.Iterable[str]:
        yield from self._keydir.range(start, end)
