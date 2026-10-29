import base64
import os
import random
import unittest

from cmws_ws.kv import MemoryKV
from cmws_ws.objects import (DIR, FILE, Commit, CorruptObject, MissingObject, ObjectStore,
                             PathConflict, Tree, blob_id)
from cmws_ws.paths import InvalidPath, normalize


class ObjectStoreTest(unittest.TestCase):
    def setUp(self):
        self.kv = MemoryKV()
        self.store = ObjectStore(self.kv)

    def test_blob_roundtrip_text_and_binary(self):
        for data in [b"", b"hello\n", "héllo ☃".encode(), b"\xff\xfe\x00binary", b"a\r\nb\0c"]:
            oid = self.store.put_blob(data)
            self.assertEqual(oid, blob_id(data))
            fresh = ObjectStore(self.kv)  # no cache: must come back from the KV
            self.assertEqual(fresh.get_blob(oid), data)

    def test_same_content_same_id_and_idempotent(self):
        a = self.store.put_blob(b"x")
        b = ObjectStore(self.kv).put_blob(b"x")
        self.assertEqual(a, b)
        self.assertEqual(len([k for k in self.kv.keys() if k.startswith("obj/")]), 1)

    def test_tree_id_independent_of_insertion_order(self):
        x, y = self.store.put_blob(b"x"), self.store.put_blob(b"y")
        t1 = self.store.put_tree(Tree({"a": _f(x), "b": _f(y)}))
        t2 = self.store.put_tree(Tree({"b": _f(y), "a": _f(x)}))
        self.assertEqual(t1, t2)

    def test_commit_roundtrip(self):
        c = Commit(self.store.empty_tree(), ("p1", "p2"), "ann", "msg ☃", 12.5)
        cid = self.store.put_commit(c)
        self.assertEqual(ObjectStore(self.kv).get_commit(cid), c)

    def test_missing_and_corrupt(self):
        with self.assertRaises(MissingObject):
            self.store.get_blob("0" * 64)
        oid = self.store.put_blob(b"good")
        self.kv.put("obj/" + oid, "u:tampered")
        with self.assertRaises(CorruptObject):
            ObjectStore(self.kv).get_blob(oid)

    def test_wrong_kind(self):
        oid = self.store.put_blob(b"x")
        with self.assertRaises(CorruptObject):
            self.store.get_tree(oid)

    def test_build_flatten_lookup(self):
        files = {"README": b"r", "src/a.py": b"a", "src/pkg/b.py": b"b", "src/pkg/c.py": b"c"}
        ids = {p: self.store.put_blob(d) for p, d in files.items()}
        root = self.store.build(ids)
        self.assertEqual(self.store.flatten(root), ids)
        self.assertEqual(self.store.read_file(root, "src/pkg/b.py"), b"b")
        self.assertEqual(self.store.lookup(root, "src/pkg").mode, DIR)
        self.assertIsNone(self.store.lookup(root, "src/nope"))
        self.assertIsNone(self.store.lookup(root, "README/x"))
        self.assertEqual(self.store.lookup(root, "").id, root)

    def test_update_is_path_copying(self):
        ids = {p: self.store.put_blob(p.encode()) for p in ["a/x", "a/y", "b/z"]}
        root = self.store.build(ids)
        new = self.store.update(root, {"a/x": self.store.put_blob(b"new")})
        # The untouched sibling directory is shared, not copied.
        self.assertEqual(self.store.lookup(root, "b").id, self.store.lookup(new, "b").id)
        self.assertNotEqual(self.store.lookup(root, "a").id, self.store.lookup(new, "a").id)

    def test_a_batch_leaves_no_intermediate_trees(self):
        ids = {p: self.store.put_blob(p.encode()) for p in ["a/b/c", "a/b/d", "a/e", "f"]}
        before = set(self.kv.keys())
        self.store.build(ids)
        self.assertEqual(len(set(self.kv.keys()) - before), 3)   # root, a and a/b; nothing else

    def test_delete_prunes_empty_directories(self):
        root = self.store.build({"a/b/c": self.store.put_blob(b"1"), "k": self.store.put_blob(b"2")})
        new = self.store.update(root, {"a/b/c": None})
        self.assertEqual(list(self.store.get_tree(new).entries), ["k"])
        self.assertEqual(self.store.update(new, {"nope/x": None}), new)

    def test_file_dir_conflicts(self):
        root = self.store.build({"a": self.store.put_blob(b"1"), "d/x": self.store.put_blob(b"2")})
        with self.assertRaises(PathConflict):
            self.store.update(root, {"a/b": self.store.put_blob(b"3")})
        with self.assertRaises(PathConflict):
            self.store.update(root, {"d": self.store.put_blob(b"3")})
        # Replacing a file by a directory in one batch is fine when the file is deleted.
        new = self.store.update(root, {"a": None, "a/b": self.store.put_blob(b"3")})
        self.assertEqual(self.store.lookup(new, "a").mode, DIR)

    def test_path_validation(self):
        self.assertEqual(normalize("/a//b/"), "a/b")
        for bad in ["a/../b", "./a", "a/\0"]:
            with self.assertRaises(InvalidPath):
                normalize(bad)
        with self.assertRaises(InvalidPath):
            self.store.update(self.store.empty_tree(), {"": self.store.put_blob(b"x")})

    def test_random_updates_match_a_dict_model(self):
        rng = random.Random(7)
        names = ["a", "b", "c"]
        model: dict = {}
        root = self.store.empty_tree()
        for _ in range(400):
            path = "/".join(rng.choice(names) for _ in range(rng.randint(1, 3)))
            if rng.random() < 0.3:
                changes = {path: None}
                expect = {p: v for p, v in model.items()
                          if p != path and not p.startswith(path + "/")}  # rm -r
            else:
                data = bytes([rng.randrange(256) for _ in range(3)])
                changes = {path: self.store.put_blob(data)}
                expect = dict(model)
                expect[path] = changes[path]
            try:
                new = self.store.update(root, changes)
            except PathConflict:
                # the model must agree the path is blocked by a file or a directory
                self.assertTrue(any(p.startswith(path + "/") or path.startswith(p + "/")
                                    for p in model))
                continue
            root, model = new, expect
            self.assertEqual(self.store.flatten(root), model)


def _f(oid):
    from cmws_ws.objects import Entry
    return Entry(FILE, oid)


if __name__ == "__main__":
    unittest.main()


class ChunkingTest(unittest.TestCase):
    def setUp(self):
        self.kv = MemoryKV()
        self.store = ObjectStore(self.kv)

    def test_boundaries_and_roundtrip(self):
        from cmws_ws.objects import CHUNK_BYTES
        rng = random.Random(1)
        for n in [CHUNK_BYTES - 1, CHUNK_BYTES, CHUNK_BYTES + 1, 3 * CHUNK_BYTES, 3 * CHUNK_BYTES + 17]:
            data = bytes(rng.randrange(256) for _ in range(n))
            oid = self.store.put_blob(data)
            self.assertEqual(oid, blob_id(data))          # same id whether chunked or not
            self.assertEqual(ObjectStore(self.kv).get_blob(oid), data)
            self.assertEqual(len(self.store.blob_chunks(oid)) > 0, n > CHUNK_BYTES)

    def test_no_stored_value_is_much_bigger_than_a_chunk(self):
        from cmws_ws.objects import CHUNK_BYTES
        self.store.put_blob(os.urandom(5 * CHUNK_BYTES))
        self.assertLess(max(len(self.kv.get(k)) for k in self.kv.keys()), CHUNK_BYTES * 4 // 3 + 64)

    def test_editing_one_chunk_stores_one_new_chunk(self):
        from cmws_ws.objects import CHUNK_BYTES
        data = bytearray(os.urandom(6 * CHUNK_BYTES))
        self.store.put_blob(bytes(data))
        before = len(self.kv.keys())
        data[2 * CHUNK_BYTES + 5] ^= 0xFF
        self.store.put_blob(bytes(data))
        self.assertEqual(len(self.kv.keys()) - before, 2)   # the changed chunk + the new manifest

    def test_identical_chunks_are_stored_once(self):
        from cmws_ws.objects import CHUNK_BYTES
        self.store.put_blob(b"A" * (4 * CHUNK_BYTES))
        self.assertEqual(len(self.kv.keys()), 2)             # one chunk, one manifest

    def test_corrupt_or_missing_chunk_is_detected(self):
        from cmws_ws.objects import CHUNK_BYTES
        data = os.urandom(3 * CHUNK_BYTES)
        oid = self.store.put_blob(data)
        chunk = self.store.blob_chunks(oid)[1]
        saved = self.kv.get("obj/" + chunk)
        self.kv.put("obj/" + chunk, "k:" + base64.b64encode(b"tampered").decode())
        with self.assertRaises(CorruptObject):
            ObjectStore(self.kv).get_blob(oid)
        self.kv.put("obj/" + chunk, saved)
        self.assertEqual(ObjectStore(self.kv).get_blob(oid), data)
        del self.kv._data["obj/" + chunk]
        with self.assertRaises(MissingObject):
            ObjectStore(self.kv).get_blob(oid)

    def test_size_limit(self):
        from cmws_ws import objects
        old = objects.MAX_BLOB_BYTES
        objects.MAX_BLOB_BYTES = 1000
        try:
            with self.assertRaises(objects.ObjectError):
                self.store.put_blob(b"x" * 1001)
        finally:
            objects.MAX_BLOB_BYTES = old
