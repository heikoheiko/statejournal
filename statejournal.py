from ethereum.utils import sha3
from ethereum.utils import big_endian_to_int, int_to_big_endian, zpad
from hashlib import sha256
import rlp
import os

"""
Datastructure:

K,V Store:
    mapping(key > [update_counter, value])

Journal:
    size | H | [key, value, ptr_oldlog] | size

    key / value can also be transactions or blocks

        [txhash, blocknum, txidx]
        [blkhash, blocknumber]

Journal Index:
    blocknumber|txidx|journal_ptr (6 bytes)
    journal



Rollbacks:
    revert the state by reading the logs backward and applying the differences
    (can be done in memory w/o commiting)


SPV:
    want proof for k,v pair, that it is in the current state root in log(n) time.



How to get a value at a certain state (update_counter):

    state == update_counter
    get current value, recursively get (via ptr_oldlog) older states



Advantages:
    x 10 less size
    x 30 faster for writes

total:12500000  batch:23986 took:0.21   writes/sec:6628 size:1201M



Design Goals:
    - Datastructures with
        - a minimal footprint for the current state
        - fast read / write access
        - pruning old data (blocks, txs, accounts) should be possible
        - suitable for DHT based long term storage of old data
        - fast reconstruction of the current state w/o going through the vm
    - Base assumptions
        - in a practical BC system state is final after N blocks
            - thus most of the old state is irrelevant
        - hashes are non compressable and slow, use them sparsely
        - checking current state and computing new states happens frequently and should be fast
        - querying recent states (chain branch switch) happens frequently and should be fast
        - querying old states
            - happens infrequently (spv) and linear O(age) is acceptable
            - sufficient if supported by a subset of nodes on the network
    - Techniques:
        - split current state and historical states
            - old blocks, memory, txs, logs is historical state
            - current state is only the current values, data at addresses
        - have a digest for state updates only
        - use an update counter to reference older states
        - state is a tuple (update_counter, updates_digest)
        - recover old states by (backward reading) the journal (or have N cached snapshots)

Limitations:
    - no spv for deleted data, i.e. one can get a proof, that a value is not set anymore



------------------

In Memory Snapshots for reverts up to N blocks


log compression within a tx should work very well, because
- addresses will repeat
- update counter share the same prefix



Hash Journal

each entry has:
[type, [key, data, update counter], H]

------ block ------
[tx]
[storage update]
[storage update]
...
[tx medstate, gas, logs]
[tx]
[storage update]
...
[tx medstate: gas, logs]
[block: state]
[size]
------ block ------


Hash Journal Indexes:
    block|tx hash to journal_ptr (end, to read the size)


To Test:
    level db key compression (e.g. repeating prefixes)





"""
b32 = 2**32
b16 = 2**16

class StateJournal(object):
    """
    Concepts:
        - There is only one state at a time
        - there is a state counter which is incremented with every state change
        - there is a journal with state changes
        - there is an index
    """


    def __init__(self, db, state=sha3('')):
        self.journal = open(os.path.join(db.dbfile, 'state_journal'), 'a')
        self.journal_index = open(os.path.join(db.dbfile, 'state_journal.idx'), 'a')
        self.db = db
        self.state = state
        self.update_counter = 0

    def get_raw(self, key):
        try:
            v = self.db.get(key)
            val, counter = rlp.decode(v)
            counter = big_endian_to_int(counter)
            return val, counter
        except KeyError:
            return b'', 0

    def get(self, key):
        return self.get_raw(key)[0]

    def update(self, key, value):
        self.update_counter += 1
        old_value, old_counter = self.get_raw(key)

        # store in leveldb
        value = rlp.encode([value, self.update_counter])
        self.db.put(key, value)

        # generate log
        log = rlp.encode([key, value, old_counter])

        # update state
        old_state = self.state
        self.state = sha3(self.state + sha3(log))

        # H | [key, value, old_counter] | log_size
        self.journal.write(self.state)
        self.journal.write(log)
        log_len = 32 + len(log) + 2
        assert log_len < b16, log_len
        self.journal.write(zpad(int_to_big_endian(log_len), 2))  # 2 bytes

        # write index
        pos = self.journal.tell()
        assert pos < b32
        idx = zpad(int_to_big_endian(pos), 4)  # 4 bytes
        self.journal_index.write(idx)

    def commit(self):
        self.journal_index.flush()
        self.journal.flush()

    def delete(self, key):
        self.update(key, '')
        self.db.delete(key)



class JournalReader(object):

    def __init__(self, db):
        self.journal = open(os.path.join(db.dbfile, 'state_journal'), 'r')
        self.journal_index = open(os.path.join(db.dbfile, 'state_journal.idx'), 'r')

    def read_update(self, update_counter):
        "first update has update_counter=1"
        self.journal_index.seek((update_counter - 1) * 4)
        log_end_pos = big_endian_to_int(self.journal_index.read(4))
        self.journal.seek(log_end_pos - 2)
        log_len = big_endian_to_int(self.journal.read(2))
        self.journal.seek(-log_len, 1)
        state = self.journal.read(32)  # state after change
        log = self.journal.read(-32 + log_len - 2)
        key, value, prev_update_counter = rlp.decode(log)
        prev_update_counter = big_endian_to_int(prev_update_counter)
        return dict(key=key, value=value, prev_update_counter=prev_update_counter,
                    state=state, log_hash=sha3(log))

    def validate_state(self, last_update_counter):
        state = sha3('')
        for i in range(1, last_update_counter+1):
            l = self.read_update(i)
            assert sha3(state + l['log_hash']) == l['state']
            state = l['state']
        return state

    def get_spv(self, update_counter_start):
        # read the update
        r = self.read_update(update_counter_start)
        if update_counter_start == 1:
            prev_state = sha3('')
        else:
            prev_state = self.read_update(update_counter_start-1)['state']
        r['hash_chain'] = [prev_state, r['log_hash']]
        update_counter = update_counter_start + 1
        while True:
            try:
                u = self.read_update(update_counter)
            except IOError:
                break
            r['hash_chain'].append(u['log_hash'])
            update_counter += 1
        return r

