import pytest
from pysled import SledDb, SledIter, SledBatch


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "sled.db")
    return SledDb(path)


@pytest.fixture
def tree(db):
    return db.open_tree(b"test")


def test_open(tmp_path):
    path = str(tmp_path / "sled.db")
    db = SledDb(path)

    assert type(db) == SledDb
    assert db.name == b"__sled__default"


def test_insert(db):
    insert(db)


def test_get(db):
    get(db)


def test_iter(db):
    iter_(db)


def test_getitem(db):
    getitem(db)


def test_setitem(db):
    setitem(db)


def test_contains(db):
    contains(db)


def test_del(db):
    del_(db)


def test_len(db):
    len_(db)


def test_batch_insert(db):
    batch_insert(db)


@pytest.mark.skip
def test_batch_insert_dict(db):
    batch_insert_dict(db)


def test_open_tree(db):
    tree = db.open_tree(b"test")
    assert tree.name == b"test"


def test_get_tree(tree):
    get(tree)


def test_iter_tree(tree):
    iter_(tree)


def test_getitem_tree(tree):
    getitem(tree)


def test_setitem_tree(tree):
    setitem(tree)


def test_contains_tree(tree):
    contains(tree)


def test_del_tree(tree):
    del_(tree)


def test_len_tree(tree):
    len_(tree)


def test_batch_insert_tree(tree):
    batch_insert(tree)


@pytest.mark.skip
def test_batch_insert_dict_tree(tree):
    batch_insert_dict(tree)


def test_drop_tree(db):
    tree = db.open_tree(b"test")
    assert db.drop_tree(b"test") is True
    assert tree.name == b"test"


def insert(tree):
    assert tree.insert(b"alice", b"10") is None
    assert tree.insert(b"bob", b"20") is None
    assert tree.insert(b"alice", b"30") == b"10"


def get(tree):
    tree.insert(b"alice", b"10")

    assert tree.get(b"alice") == b"10"
    assert tree.get(b"bob") is None


def iter_(tree):
    tree.insert(b"alice", b"10")
    tree.insert(b"bob", b"20")

    it = iter(tree)
    assert type(it) is SledIter
    assert next(it) == (b"alice", b"10")
    assert next(it) == (b"bob", b"20")


def getitem(tree):
    tree.insert(b"alice", b"10")

    assert tree[b"alice"] == b"10"
    assert tree[b"bob"] is None


def setitem(tree):
    tree[b"alice"] = b"10"

    assert tree[b"alice"] == b"10"
    assert tree[b"bob"] is None


def contains(tree):
    tree[b"alice"] = b"10"

    assert b"alice" in tree
    assert b"bob" not in tree


def del_(tree):
    tree[b"alice"] = b"10"
    assert b"alice" in tree

    del tree[b"alice"]
    assert b"alice" not in tree


def len_(tree):
    assert len(tree) == 0
    tree.insert(b"alice", b"10")
    assert len(tree) == 1


def batch_insert(tree):
    batch = SledBatch()
    batch.insert(b"alice", b"10")
    batch.insert(b"bob", b"20")
    batch.insert(b"carol", b"30")

    tree.apply_batch(batch)

    assert tree[b"alice"] == b"10"
    assert tree[b"bob"] == b"20"
    assert tree[b"carol"] == b"30"


def batch_insert_dict(tree):
    batch = {b"alice": b"10", b"bob": b"20", b"carol": b"30"}

    tree.apply_batch(batch)

    assert tree[b"alice"] == b"10"
    assert tree[b"bob"] == b"20"
    assert tree[b"carol"] == b"30"
