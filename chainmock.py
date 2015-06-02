from utils import get_pareto
from ethereum.utils import sha3, big_endian_to_int, int_to_big_endian
from ethereum.slogging import configure
from db import LevelDB
from ethereum.trie import Trie
import statejournal
import sys
import rlp
import resource
configure(':info')

config = dict(txs_per_block=get_pareto(806., 156, 317), # 20 / 80
              num_accounts=1000,  # some accounts are accessed more often
              contract_storage_slots=get_pareto(806., 10000, 507),  # 40 / 60
              storage_reads=get_pareto(806., 100, 2327),  # 40 / 60
              storage_read_update_ratio=2,
              storage_read_delete_ratio=2,
              num_blocks=10000
              )


class Storage(object):
    num_reads = 0
    num_writes = 0
    num_misses = 0
    num_deletes = 0
    seen_keys = set()
    max_mem_usage = 0

    def __init__(self, db):
        self.db = db

    def update_mem_usage(self):
        m = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024**2
        self.max_mem_usage = max(m, self.max_mem_usage)

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
        self.seen_keys.add(k)

    def delete(self, k):
        k = self._key(k)
        self.num_deletes += 1
        self.db.delete(k)
        self.commit()

    def commit(self):
        self.update_mem_usage()
        self.db.db.commit()

class JournalStorage(Storage):

    def _key(self, k):
        return k

    def commit(self):
        self.update_mem_usage()
        self.db.commit()


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

        # receiving account
        account = Account(chain, receiver)

        reads = int(config['storage_reads'](tx_num))
        reads = min(reads, account.storage_slots)
        updates = int(reads / config['storage_read_update_ratio'])
        deletes = int(updates / config['storage_read_delete_ratio'])

        # print reads, updates, deletes

        assert updates + deletes <= reads


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


def get_trie_chain(path):
    db = LevelDB(path)
    t = Trie(db)
    return Chain(t)

def get_statejournal_chain(path):
    db = LevelDB(path)
    t = statejournal.StateJournal(db)
    return Chain(t, storage_class=JournalStorage)

def test_add_blocks(chain, num_blocks):
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

    #  get spv ################
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
    assert proof['value'] == val
    hash_chain = proof['hash_chain']
    assert len(hash_chain)
    s = hash_chain[0]
    for h in hash_chain[1:]:
        s = sha3(s + h)
    assert s == sj.state


def test_writes(chain, accounts, storage_slots, commit_interval=1000):
    print 'created hashes'
    counter = 0
    while True:
        for k in accounts:
            counter += 1
            v = int_to_big_endian(counter)
            chain.storage.update(k+v, v)
            if counter == storage_slots:
                return chain
            if counter % commit_interval == 0:
                chain.storage.commit()

def test_reads(chain, accounts, storage_slots):
    counter = 0
    while True:
        for k in accounts:
            counter += 1
            v = int_to_big_endian(counter)
            r = chain.storage.get(k+v)
            assert v == r, repr((v, r, counter))
            if counter == storage_slots:
                return chain

def test_update(chain, accounts, storage_slots):
    counter = 0
    while True:
        for k in accounts:
            counter += 1
            v = int_to_big_endian(counter)
            v2 = int_to_big_endian(counter + 1)
            chain.storage.update(k+v, v2)
            if counter == storage_slots:
                return chain

def test_delete(chain, accounts, storage_slots):
    counter = 0
    while True:
        for k in accounts:
            counter += 1
            v = int_to_big_endian(counter)
            chain.storage.delete(k+v)
            if counter == storage_slots:
                return chain

def do_test():
    # h = "create|read|update|delete trie|journal num_slots num_accounts path"
    # print sys.argv[0], h
    print sys.argv

    task, tech, storage_slots, num_accounts, path = sys.argv[1:]
    storage_slots = int(storage_slots)
    num_accounts = int(num_accounts)
    accounts = [sha3(str(i)) for i in range(num_accounts)]

    if tech == 'trie':
        use_trie = True
    else:
        use_trie = False

    if use_trie:
        chain = get_trie_chain(path)
        # set state root
        try:
            sr = chain.storage.db.db.get('STATE_ROOT')
            chain.storage.db.set_root_hash(sr)
        except KeyError:
            pass
    else:
        chain = get_statejournal_chain(path)

    if task == 'create':
        test_writes(chain, accounts, storage_slots)
    elif task == 'read':
        test_reads(chain, accounts, storage_slots)
    elif task == 'update':
        test_update(chain, accounts, storage_slots)
    elif task == 'delete':
        test_delete(chain, accounts, storage_slots)
    else:
        raise Exception('unknown')

    if use_trie:
        # store state root
        sr = chain.storage.db.root_hash
        chain.storage.db.db.put('STATE_ROOT', sr)

    return chain

def report(chain):
    chain.storage.commit()
    print 'memory usage', chain.storage.max_mem_usage
    s = chain.storage
    ldb = s.db.db
    print len(chain.storage.seen_keys), 'storage locations'
    print ldb.read_counter, 'db reads'
    print ldb.write_counter, 'db writes'
    if chain.num_blocks:
        print s.num_reads, 'app reads'
        print s.num_writes, 'app writes'
        print s.num_deletes, 'app deletes'
        print s.num_misses, 'app misses'
        print chain.num_txs, 'transactions'
        print chain.num_blocks, 'blocks'

def test_fake_chain():
    num_blocks = config['num_blocks']
    # chain = test_add_blocks(chain, config['num_blocks'])


if __name__ == '__main__':
    chain = do_test()
    report(chain)

"""
Test 1Mio Writes / Reads w/ 1k accounts


Trie:
memory usage 924
1000000 app reads
9610120 db reads
1000000 app writes
11947818 db writes
1000000 storage locations
python chainmock.py testt  687,75s user 188,99s system 158% cpu 9:13,01 total
2,6G    testt

Trie Read:
memory usage 1758
python chainmock.py testt  150s user 110s system


Journal:
memory usage 483
1000000 app reads
2000000 db reads
1000000 app writes
1000000 db writes
1000000 storage locations
python chainmock.py testj  47,45s user 2,19s system 107% cpu 46,006 total
115M    testj (86gb state journal) = 29M

Journal Read:
memory usage 665
1000000 app reads
1000000 db reads
python chainmock.py testj  18,90s user 0,78s system 100% cpu 19,606 total

Size:
11M / 2,6GB = x236
30M / 2,6GB = x85
51M / 2,6GB = x50
137M / 2,6GB = x19 (worst case with log)

Write:
875 / 50 = x17.5
Read:
260 / 20 = x13

----------------------

Test 1Mio Writes / Reads w/ ONE account
Journal:
97M    testj (86gb state journal) = 11M

Test 1Mio Writes / Reads w/ 1M account
Journal:
137M    testj (86gb state journal) = 51M


Test w/ 2x updates





Trie:
------------------
2789094 app reads
17959499 db reads
883654 app writes
10508762 db writes
1163 app deletes
605587 app misses
194406 transactions
10000 blocks
605314 storage locations
python chainmock.py testt  953,67s user 259,22s system 137% cpu 14:44,01 total
(pypy) ~/dev/ethereum/hashjournal (git)-[master] % du -h testt
2,2G    testt

(same size with compression turned on)

State Journal:
------------------
2789094 app reads
3673911 db reads
883654 app writes
883654 db writes
1163 app deletes
605587 app misses
194406 transactions
10000 blocks
605314 storage locationsl
python chainmock.py testj  85,06s user 2,86s system 109% cpu 1:20,32 total
(pypy) ~/dev/ethereum/hashjournal (git)-[master] % du -h testj
103M    testj
(pypy) ~/dev/ethereum/hashjournal (git)-[master] % ls -al testj/state_journal*
-rw-r--r--  1 heiko  staff  91532758 31 Mai 10:49 testj/state_journal
-rw-r--r--  1 heiko  staff   3539268 31 Mai 10:49 testj/state_journal.idx
(pypy) ~/dev/ethereum/hashjournal (git)-[master] %

Results:
    StateJournal is
    10x faster (for mixed read/writes)
    20x smaller if keeping the log
    250x smaller if pruning the log for old finalized states
"""
