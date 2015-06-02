import os
import leveldb
from ethereum import slogging
from ethereum.compress import compress, decompress
import time

compress = decompress = lambda x: x

class LevelDB(object):


    def __init__(self, dbfile):
        self.uncommitted = dict()
        self.dbfile = dbfile
        self.db = leveldb.LevelDB(dbfile)
        self.commit_counter = 0
        self.read_counter = 0
        self.write_counter = 0

    def reopen(self):
        # del self.db
        # self.db = leveldb.LevelDB(self.dbfile)
        pass

    def get(self, key):
        self.read_counter += 1
        if key in self.uncommitted:
            if self.uncommitted[key] is None:
                raise KeyError("key not in db")
            return self.uncommitted[key]
        o = decompress(self.db.Get(key))
        self.uncommitted[key] = o
        return o

    def put(self, key, value):
        self.write_counter += 1
        self.uncommitted[key] = value


    def commit(self):
        batch = leveldb.WriteBatch()
        for k, v in self.uncommitted.items():
            if v is None:
                batch.Delete(k)
            else:
                batch.Put(k, compress(v))
        self.db.Write(batch, sync=False)
        self.uncommitted.clear()
        self.commit_counter += 1
        if self.commit_counter % 100 == 0:
            self.reopen()

    def delete(self, key):
        self.uncommitted[key] = None

    def _has_key(self, key):
        try:
            self.get(key)
            return True
        except KeyError:
            return False

    def __contains__(self, key):
        return self._has_key(key)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.db == other.db

    def __repr__(self):
        return '<DB at %d uncommitted=%d>' % (id(self.db), len(self.uncommitted))
