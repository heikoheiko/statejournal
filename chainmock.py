from utils import get_pareto
from ethereum.utils import sha3, big_endian_to_int, int_to_big_endian
from ethereum.slogging import configure
from db import LevelDB
from ethereum.trie import Trie
import statejournal
import sys
import rlp
configure(':debug')

config = dict(txs_per_block=get_pareto(806., 156, 317), # 20 / 80
              num_accounts=1000,  # some accounts are accessed more often
              P_account_is_contract=0.1,
              contract_storage_slots=get_pareto(806., 10000, 507),  # 40 / 60
              storage_reads=get_pareto(806., 100, 2327),  # 40 / 60
              storage_read_update_ratio=2,
              storage_update_delete_ratio=4,
              )


class Storage(object):
    num_reads = 0
    num_writes = 0
    num_misses = 0
    num_deletes = 0

    def __init__(self, db):
        self.db = db

    def _key(self, k):
        return sha3(k)

    def get(self, k):
        k = self._key(k)
        self.num_reads += 1
        v = self.db.get(k)
        if not v:
            self.num_misses += 1
        return v

    def update(self, k, v):
        k = self._key(k)
        self.num_writes += 1
        self.db.update(k, v)


    def delete(self, k):
        k = self._key(k)
        self.num_deletes += 1
        self.db.delete(k)

    def commit(self):
        self.db.db.commit()

class JournalStorage(Storage):

    def _key(self, k):
        return k


class Account(object):

    def __init__(self, chain, account):
        self.number = account
        self.address = sha3(str(account))
        self.storage_slots = int(1 + config['contract_storage_slots'](account))
        self.chain = chain

    def __repr__(self):
        return '<Acccount({}) slots={}>'.format(self.number, self.storage_slots)

    def read_account(self):
        rlpdata = self.chain.storage.get(self.address)
        if not rlpdata:
            self.write_account(0, 0)
            return self.read_account()
        nonce, balance, s, c = rlp.decode(rlpdata)
        nonce = big_endian_to_int(nonce)
        balance = big_endian_to_int(balance)
        return nonce, balance

    def write_account(self, nonce=0, balance=2**100):
        s = sha3(self.address)
        c = sha3(s)
        data = [int_to_big_endian(nonce), int_to_big_endian(balance), s, c]
        self.chain.storage.update(self.address, rlp.encode(data))

    def update_nonce(self):
        nonce, balance = self.read_account()
        self.write_account(nonce+1, balance)

    def update_balance(self, difference):
        nonce, balance = self.read_account()
        self.write_account(nonce, balance + difference)

    def delete_account(self):
        self.chain.storage.delete(self.address)

    def store(self, k, v):
        k = k % self.storage_slots
        self.chain.storage.update(self.address+str(k), int_to_big_endian(v))
#        assert k in self.keys()

    def read(self, k):
        k = k % self.storage_slots
        r = self.chain.storage.get(self.address+str(k))
        if not r:  # create storage
            self.store(k, 1)
            return self.read(k)
        return big_endian_to_int(r)

    def delete(self, k):
        k = k % self.storage_slots
        self.chain.storage.delete(self.address+str(k))

    def keys(self):
        self.chain.storage.commit()
        leveldb = self.chain.db.db.db
        keys =[]
        for key in leveldb.RangeIter(key_from=self.address, include_value=False):
            keys.append(key)
            if key.startswith(self.address):
                keys.append(key)
            else:
                break
        return keys

class Transaction(object):

    def __init__(self, chain, receiver, tx_num):
        assert isinstance(receiver, int)
        sender = config['num_accounts'] - receiver
        self.hash = sha3(str(tx_num))

        # increase nonce
        Account(chain, sender).update_nonce()

        reads = int(config['storage_reads'](tx_num))
        updates = int(reads / config['storage_read_update_ratio'])
        deletes = int(updates / config['storage_update_delete_ratio'])

        # print reads, updates, deletes

        assert updates + deletes <= reads

        account = Account(chain, receiver)

        if not reads:  # simple value transfer
            account.update_balance(tx_num)
            return

        # read, update, write
        for i in range(reads):
            i += tx_num
            v = account.read(i)
            if i < updates:
                account.store(i, v+i+tx_num)
            elif i < updates + deletes:
                account.delete(i)


class Block(object):
    def __init__(self, chain, number):
        self.number = number
        self.num_txs = int(config['txs_per_block'](number))
        #  print 'num_txs', self.num_txs
        for i in range(self.num_txs):
            account = chain.num_txs % config['num_accounts']
            t = Transaction(chain, account, chain.num_txs)
            chain.num_txs += 1


class Chain(object):

    num_blocks = 0
    num_txs = 0
    head = None

    def __init__(self, db, storage_class=Storage):
        self.db = db
        self.storage = storage_class(db)

    def add_block(self):
        b = Block(self, number=self.num_blocks)
        self.num_blocks += 1
        self.head = b
        self.storage.commit()

def test_trie(num_blocks, path):
    db = LevelDB(path)
    t = Trie(db)
    chain = Chain(t)
    for i in range(1000):
        chain.add_block()
    return chain

def test_statejournal(num_blocks, path):
    db = LevelDB(path)
    t = statejournal.StateJournal(db)
    chain = Chain(t, storage_class=JournalStorage)
    for i in range(num_blocks):
        chain.add_block()
    return chain



def test_statejournal_read(chain):
    # validate chain
    sj = chain.storage.db  # state journal
    sj.commit()
    lr = statejournal.JournalReader(sj.db)
    state = lr.validate_state(sj.update_counter)
    assert state == sj.state

    #  get spv
    for i in range(config['num_accounts']):
        account = Account(chain, i)
        keys = account.keys()
        if keys:
            break
    assert len(keys)
    print 'keys found', account
    k = keys[0]
    val, update_counter = sj.get_raw(k)
    proof = lr.get_spv(update_counter)
    hash_chain = proof['hash_chain']
    assert len(hash_chain)
    s = hash_chain[0]
    for h in hash_chain[1:]:
        s = sha3(s + h)
    assert s == sj.state


if __name__ == '__main__':
    path = sys.argv[1]
    num_blocks = 1
    chain = test_statejournal(num_blocks, path)
    # chain = test_trie(num_blocks, path)
    print chain.storage.num_reads, 'reads'
    print chain.storage.num_writes, 'writes'
    print chain.storage.num_deletes, 'deletes'
    print chain.storage.num_misses, 'misses'

    # test_statejournal_read(chain)
