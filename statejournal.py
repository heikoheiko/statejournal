from ethereum.utils import sha3
from ethereum.utils import big_endian_to_int, int_to_big_endian, zpad
import rlp
import os

"""
Efficient journal based cryptographically authenticated data structure
 as an alternative to the Merkel Patricia Tree

The basic idea is to update a hash function with the stream of state updates,
which are written to a journal, instead of using a merkel tree.
While the latter is good at supporting access to different states it has severe
runtime, memory and storage penalties.


Improvements:
    - x50 lower storage requirements (x20 if keeping the full journal, x1000 if deleting storage)
    - x12/x17 faster for reads/writes
    - x135/x70 lower system IO for reads/writes
    - x6 lower memory footprint (levedb needs to cache key prefixes)
    - supports pruning of old data
    - straight forward implementation (direct k,v mapping + journal)

The storage improvements are due to
    - less data (no merkel tree levels)
    - compressable keys, b/c keys to account storage location can share the same prefix

Limitation:
    - For O(log(n)) SPVs one of the queried peers must be honest,
         when requesting a current state value
    - If only the current state root can be trusted, SPV takes O(age)


Design Goals:
    - minimal footprint for the current state
    - fast read / write access
    - pruning old data (deleted state, blocks, txs, accounts) should be possible
    - suitable for DHT based long term storage of old data
    - fast reconstruction of the current state w/o going through the vm

Base assumptions:
    - in a practical BC system state is final after N blocks
        - thus most of the old state is irrelevant
    - hashes are non compressable and slow, use them sparsely
    - checking current state and computing new states happens frequently and should be fast
    - querying recent states (chain branch switch) happens frequently and should be fast
    - querying old states
        - happens infrequently (spv) and linear O(age) is acceptable for most use cases

Implementation:
    - split current state and historical states
        - current state is only the current data at addresses
        - historical state is stored in a local journal or a DHT
        - old blocks, txs, receipts is historical state
    - use a hash digest to track state updates H(H'|H(update))
    - keys, values are directly mapped to leveldb
    - an update counter (as part of the value) is used to reference older states
    - recover old states by
        - using in memory snapshots (for up to N blocks)
        - backward reading the journal
    - all state changes are written to a journal
    - to support log(n) SPVs
      the journal merges in state_digests at tx and block boundaries (see below)

Changes to the current Ethereum protocol:
    - block header:
        - change: state_root is now the state_digest
        - change: tx_list_root becomes H(H(tx0), ... H(txN))
        - change: receipts_root becomes H(H(R0), ... H(RN))
        - add: update_counter
        - add: state_digest of a second prev block (so we get a tree structure for log(n) SPVs)
               referenced block number is the highest divisor of the current block number
               for divisors in [2^0, 2^1, ... 2^n]
    - tx receipts:
        - change: medstate is now the state_digest
        - add: update_counter

Changes to the Ethereum implementation:
    - Changes to block header and tx receipt
    - StateJournal replaces Trie
    - blocks that are not yet considered to be in the final chain
      need to cache their state updates in memory (or another StateJournal)

SPV:
    A light client wants a SPV for a (value, update_counter) tuple.
        Client asks the network for a SPV, receives the chain of state_digests from the
            last value change to the current state_digest.
        The recursively hashed state_digests must match the current state_digest.

    A light client wants to know the current state of an account property:
        Client asks the network with a key corresponding to the account property
            for (value, update_counter). At least one answer must be honest.
        Client asks the network for a SPV.

    A light client wants to download/check a transaction/block_header.
        Same as above but with the transaction/block_header hash as the key.
        This can be done recursively to download all block_headers / txs.

    A light client wants to know an old state of an account property at `target update_counter`:
        Get the current state which contains the `previous update_counter`.
        The returned `previous update_counter` references the update_counter for the state change
            at that key previous to the last update.
        While `previous update_counter` > `target update_counter`:
            Ask the network for the journal entry at `previous update_counter`

    Light clients want to collectively validate a block or watch updates
        Download and spv the necessary data.

DHT based State Journal:


"""
b32 = 2**32
b16 = 2**16


class StateJournal(object):
    state_journal_fn = 'state_journal'
    state_journal_index_fn = 'state_journal.idx'
    empty_state_digest = sha3('')

    """
    Updates to the state are tracked by state_digest updates
    Previous states are referenced by a state_counter

    The StateJournal only tracks one chain, i.e. does not keep track of forks.
    A StateJournal (continuing from the previous one) can be created for every block and deleted
        once the block is considered to be in the final chain.

    SPVs are supported by
        - providing a (value, update_counter) tuple and the current state_digest
        - traversing the journal up to the current state_digest

    Rollbacks are supported by
        - reading the log backwards and restoring the old values
        - non final states should better be kept in a chain of in memory State Journals

    Datastructure:

        Key Value Store (the state db):
            mapping(key : rlp[value, update_counter])
            note: `key` can be of arbitrary size

        Journal Log:
            state_digest[32] | rlp[key, value, old_counter] | log_size[2]

        Journal Index:
            journal_pos_ptr[4]
            i.e post log pos position is at (update_counter-1) * 4
    """


    def __init__(self, db, state_digest=empty_state_digest):
        self.journal = open(os.path.join(db.dbfile, self.state_journal_fn), 'a')
        self.journal_index = open(os.path.join(db.dbfile, self.state_journal_index_fn), 'a')
        self.db = db
        self.state_digest = state_digest
        self.update_counter = 0

    def get_raw(self, key):
        "returns (value, update_counter)"
        try:
            v = self.db.get(key)
            val, counter = rlp.decode(v)
            counter = big_endian_to_int(counter)
            return val, counter
        except KeyError:
            return b'', 0

    def get(self, key):
        "returns value"
        return self.get_raw(key)[0]

    def update(self, key, value):
        """
        - increases the update counter
        - retrieves the the old_update_counter for the key
        - stores the value in leveldb
        - generates a log: rlp[key, value, old_update_counter]
        - computes the new state_digest as: H(last_state_digest, H(log))
        - adds to the journal: state_digest | log | journal_entry_length
        - updates index with the postion of the end of above journal_entry
        """
        self.update_counter += 1
        old_value, old_counter = self.get_raw(key)

        # store in leveldb
        if value:
            value = rlp.encode([value, self.update_counter])
            self.db.put(key, value)
        else:
            self.db.delete(key)

        # generate log
        log = rlp.encode([key, value, old_counter])

        # update state
        self.state_digest = sha3(self.state_digest + sha3(log))

        # state_digest | [key, value, old_counter] | journal_entry_length
        self.journal.write(self.state_digest)
        self.journal.write(log)
        journal_entry_length = 32 + len(log) + 2
        assert journal_entry_length < b16, journal_entry_length
        self.journal.write(zpad(int_to_big_endian(journal_entry_length), 2))  # 2 bytes

        # write index
        pos = self.journal.tell()
        assert pos < b32
        idx = zpad(int_to_big_endian(pos), 4)  # 4 bytes
        self.journal_index.write(idx)

    def commit(self):
        self.journal_index.flush()
        self.journal.flush()
        self.db.commit()

    def delete(self, key):
        "actually deletes the key from the database"
        self.update(key, '')


    def rollback(self, update_counter, verify=False):
        """
        rollback to the state after update_counter based on the local journal

        In practice this file based rollback should not be used,
        but instead updates for young blocks which are probably not final yet
        should be held in memory
        """
        # read log backwards
        jr = JournalReader(self.db)
        for uc in reversed(range(update_counter + 1, self.update_counter+1)):
            u = jr.read_update(uc)
            key = u['key']
            # update with old value
            prev_uc = u['prev_update_counter']
            if prev_uc > 0:
                v = jr.read_update(prev_uc)['value']
                self.db.put(key, v)
            else:
                self.db.delete(key)
            # read state before the update we reverted
            if uc > 1:
                state_digest = jr.read_update(uc-1)['state_digest']
                assert sha3(state_digest, u['log_hash']) == u['state_digest']
                self.state_digest = state_digest
            else:
                self.state_digest = self.empty_state_digest

        #  truncate the logfile and index
        self.journal_index.seek((update_counter - 1) * 4)
        log_end_pos = big_endian_to_int(self.journal_index.read(4))
        self.journal_index.truncate()
        self.journal.seek(log_end_pos)
        self.journal.truncate()


class JournalReader(object):
    """

    """

    def __init__(self, db):
        self.journal = open(os.path.join(db.dbfile, StateJournal.state_journal_fn), 'r')
        self.journal_index = open(os.path.join(db.dbfile, StateJournal.state_journal_index_fn),
                                  'r')

    def read_update(self, update_counter):
        "first update has update_counter=1"
        self.journal_index.seek((update_counter - 1) * 4)
        log_end_pos = big_endian_to_int(self.journal_index.read(4))
        self.journal.seek(log_end_pos - 2)
        log_len = big_endian_to_int(self.journal.read(2))
        self.journal.seek(-log_len, 1)
        state_digest = self.journal.read(32)  # state_digest after change
        log = self.journal.read(-32 + log_len - 2)
        key, value, prev_update_counter = rlp.decode(log)
        prev_update_counter = big_endian_to_int(prev_update_counter)
        return dict(key=key, value=value, prev_update_counter=prev_update_counter,
                    state_digest=state_digest, log_hash=sha3(log))

    def validate_state(self, last_update_counter):
        state_digest = StateJournal.empty_state_digest
        for i in range(1, last_update_counter+1):
            l = self.read_update(i)
            assert sha3(state_digest + l['log_hash']) == l['state_digest']
            state_digest = l['state_digest']
        return state_digest

    def get_spv(self, update_counter_start):
        """
        returns all hashes from a given value up to the current state.
        recursively hasing them up should lead to the current state root.

        note: the user first needs to know or query and trust
            -  the current state root
            -  the current value,update_counter
            -  i.e. at least one queried peer needs to give a honest answer

        PoC implementation is O(n), but can be changed to O(log(n)) by
            - adding state_digests to txs and (tree like) for blocks
        """

        # read the update
        r = self.read_update(update_counter_start)
        if update_counter_start == 1:
            prev_state_digest = StateJournal.empty_state_digest
        else:
            prev_state_digest = self.read_update(update_counter_start - 1)['state_digest']
        r['hash_chain'] = [prev_state_digest, r['log_hash']]
        update_counter = update_counter_start + 1
        while True:
            try:
                u = self.read_update(update_counter)
            except IOError:
                break
            r['hash_chain'].append(u['log_hash'])
            update_counter += 1
        return r

