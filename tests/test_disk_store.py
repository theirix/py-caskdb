import datetime
import json
import logging
import os.path
import shutil
import sys
import tempfile
import typing
import unittest

from hypothesis import given, settings, strategies as st

from disk_store import DiskStorage


class TempStorageFile:
    """
    TempStorageFile provides a wrapper over the temporary files which are used in
    testing.

    Python has two APIs to create temporary files, tempfile.TemporaryFile and
    tempfile.mkstemp. Files created by tempfile.TemporaryFile gets deleted as soon as
    they are closed. Since we need to do tests for persistence, we might open and
    close a file multiple times. Files created using tempfile.mkstemp don't have this
    limitation, but they have to deleted manually. They don't get deleted when the file
    descriptor is out scope or our program has exited.

    Args:
        path (str): path to the file where our data needs to be stored. If the path
            parameter is empty, then a temporary will be created using tempfile API
    """

    def __init__(self, path: typing.Optional[str] = None):
        if path:
            self.path = path
            self.dirpath = None
            return

        self.dirpath = tempfile.mkdtemp(prefix="pycaskdb")
        self.path = self.dirpath + "/main.db"

    def clean_up(self) -> None:
        # NOTE: you might be tempted to use the destructor method `__del__`, however
        # destructor method gets called whenever the object goes out of scope, and it
        # will delete our database file. Having a separate method would give us better
        # control.
        if self.dirpath and len(self.dirpath.split("/")) > 2:
            shutil.rmtree(self.dirpath)


class TestDiskCDB(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(stream=sys.stdout, level="INFO")
        self.file: TempStorageFile = TempStorageFile()

    def tearDown(self) -> None:
        self.file.clean_up()

    def test_get(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        store.set("name", "jojo")
        self.assertEqual(store.get("name"), "jojo")
        store.close()

    def test_invalid_key(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        self.assertEqual(store.get("some key"), "")
        store.close()

    def test_dict_api(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        store["name"] = "jojo"
        self.assertEqual(store["name"], "jojo")
        store.close()

    def test_persistence(self) -> None:
        store = DiskStorage(file_name=self.file.path)

        tests = {
            "crime and punishment": "dostoevsky",
            "anna karenina": "tolstoy",
            "war and peace": "tolstoy",
            "hamlet": "shakespeare",
            "othello": "shakespeare",
            "brave new world": "huxley",
            "dune": "frank herbert",
        }
        for k, v in tests.items():
            store.set(k, v)
            self.assertEqual(store.get(k), v)
        store.close()

        store = DiskStorage(file_name=self.file.path)
        for k, v in tests.items():
            self.assertEqual(store.get(k), v)
        store.close()

    def test_dict_delete(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        store["name"] = "jojo"
        store["foo"] = "fooval"
        store.delete("name")
        self.assertEqual(store["name"], "")
        self.assertEqual(store["foo"], "fooval")
        store.close()

    def test_dict_delete_write(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        store["name"] = "jojo"
        store["foo"] = "fooval"
        store.delete("name")
        store["name"] = "new"
        self.assertEqual(store["name"], "new")
        self.assertEqual(store["foo"], "fooval")
        store.close()

    def test_dict_delete_reopen(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        store["name"] = "jojo"
        store["foo"] = "fooval"
        store.delete("name")
        store.close()

        store = DiskStorage(file_name=self.file.path)
        self.assertEqual(store["name"], "")
        self.assertEqual(store["foo"], "fooval")
        store.close()

    def test_range(self) -> None:
        store = DiskStorage(file_name=self.file.path)

        tests = {
            "crime and punishment": "dostoevsky",
            "anna karenina": "tolstoy",
            "war and peace": "tolstoy",
            "hamlet": "shakespeare",
            "othello": "shakespeare",
            "brave new world": "huxley",
            "dune": "frank herbert",
        }
        for k, v in tests.items():
            store.set(k, v)
            self.assertEqual(store.get(k), v)

        scanned = list(sorted(list(store.scan("brave", "hackers"))))
        assert scanned == ["brave new world", "crime and punishment", "dune"]

        scanned = list(sorted(list(store.scan("brave", "aelita"))))
        assert scanned == []

    @given(st.lists(st.text(min_size=1, max_size=10), min_size=1, max_size=100))
    def test_multi(self, keys: typing.List[str]) -> None:
        store = DiskStorage(file_name=self.file.path)

        def genv(key: str) -> str:
            return f"value_{key}."

        for k in keys:
            v = genv(k)
            store.set(k, v)
            self.assertEqual(store.get(k), v)
        for k in keys:
            self.assertEqual(store.get(k), genv(k))
        store.close()

        store = DiskStorage(file_name=self.file.path)
        for k in keys:
            self.assertEqual(store.get(k), genv(k))
        store.close()

    def test_two(self) -> None:
        store = DiskStorage(file_name=self.file.path, max_size=60)

        keys = [f"k{idx}" for idx in range(7)]
        values = [f"v{idx}" for idx in range(7)]

        for k, v in zip(keys, values):
            store.set(k, v)
            self.assertEqual(store.get(k), v)
        for k, v in zip(keys, values):
            self.assertEqual(store.get(k), v)
        store.close()

        with open(self.file.path, "rt") as f:
            jregistry = json.load(f)
            assert len(jregistry) == 2
        for file_id in range(2):
            data_path = os.path.join(
                os.path.dirname(self.file.path), f"data_0{file_id}.bin"
            )
            assert os.path.isfile(data_path)

        store.close()

    def test_two_delete(self) -> None:
        store = DiskStorage(file_name=self.file.path, max_size=60)

        keys = [f"k{idx}" for idx in range(7)]
        values = [f"v{idx}" for idx in range(7)]

        for i in range(7):
            k = keys[i]
            v = values[i]
            store.set(k, v)
            self.assertEqual(store.get(k), v)
            if i == 3:
                store.delete(keys[1])
        self.assertEqual(store.get(keys[1]), "")
        store.close()

        store = DiskStorage(file_name=self.file.path, max_size=60)
        self.assertEqual(store.get(keys[1]), "")
        for i in range(7):
            k = keys[i]
            v = values[i]
            if i == 1:
                self.assertEqual(store.get(k), "")
            else:
                self.assertEqual(store.get(k), v)
        store.close()

    def test_compaction(self) -> None:
        store = DiskStorage(file_name=self.file.path, max_size=60)

        keys = [f"k{idx}" for idx in range(7)]
        values = [f"v{idx}" for idx in range(7)]

        for i in range(7):
            k = keys[i]
            v = values[i]
            store.set(k, v)
            self.assertEqual(store.get(k), v)

        for i in range(7):
            k = keys[i]
            v = values[i]
            if i % 2 == 0:
                store.set(k, v)

        store.compact()

        for file_id in range(2):
            data_path = os.path.join(
                os.path.dirname(self.file.path), f"data_0{file_id}.bin"
            )
            self.assertFalse(os.path.isfile(data_path))

        for k, v in zip(keys, values):
            self.assertEqual(store.get(k), v)

        store.close()


class TestDiskCDBPropertyBased(unittest.TestCase):
    @given(st.lists(st.text(min_size=1, max_size=10), min_size=1, max_size=1000))
    @settings(deadline=datetime.timedelta(seconds=5))
    def test_compaction_hyp(self, keys: typing.List[str]) -> None:
        file = TempStorageFile()
        store = DiskStorage(file_name=file.path, max_size=100)

        def genv(key: str) -> str:
            return f"value_{key}."

        actual = {}
        for k in keys:
            v = genv(k)
            store.set(k, v)
            self.assertEqual(store.get(k), v)
            actual[k] = v
        for i in range(len(keys)):
            k = keys[i]
            if i % 3 == 0:
                v = genv(k) + "add"
                store.set(k, v)
                actual[k] = v
        store.compact()

        for k in keys:
            self.assertEqual(store.get(k), actual[k])
        store.close()

        # Reopen
        store = DiskStorage(file_name=file.path)
        for k in keys:
            self.assertEqual(store.get(k), actual[k])
        store.close()

        file.clean_up()


class TestDiskCDBExistingFile(unittest.TestCase):
    def setUp(self) -> None:
        logging.basicConfig(stream=sys.stdout, level="INFO")
        self.file: TempStorageFile = TempStorageFile()

    def tearDown(self) -> None:
        self.file.clean_up()

    def test_get_new_file(self) -> None:
        store = DiskStorage(file_name=self.file.path)
        store.set("name", "jojo")
        self.assertEqual(store.get("name"), "jojo")
        store.close()

        # check for key again
        store = DiskStorage(file_name=self.file.path)
        self.assertEqual(store.get("name"), "jojo")
        store.close()
