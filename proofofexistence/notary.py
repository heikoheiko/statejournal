# Copyright (c) 2015 Heiko Hees
from hashlib import sha256


def hash_func(x):
    # change hash_func here
    # from Crypto.Hash import keccak
    # return keccak.new(digest_bits=256, data=x).digest()
    return sha256(x).digest()


def H(a, b):
    if a > b:
        return hash_func(a + b)
    return hash_func(b + a)

assert H(hash_func('a'), hash_func('b')) == H(hash_func('b'), hash_func('a'))


def hx(h):
    return h.encode('hex')[:8]

_even_base = 2
_odd_base = 64


def distant_ancestor(number):
    assert number > 0
    if number % 2 == 0:
        base = _even_base
        dnumber = number
    else:
        base = _odd_base
        dnumber = number + 1
    p = 1
    while dnumber % (base ** p) == 0:
        p += 1
    p -= 1
    if dnumber == base ** p:
        p -= 1
    bn = dnumber - base ** p
    if bn == number:
        return number - 1
    return bn


def get_path(number, target):
    """
    get the hops from number to target based on a skip list generated with
    direct and distant ancestors
    """
    if target == number:
        return [(None, number)]
    prev = number - 1
    distant = distant_ancestor(number)
    if distant >= target:
        return [(True, number)] + get_path(distant, target)
    else:
        return [(False, number)] + get_path(prev, target)


class Notary(object):

    def __init__(self):
        self.logs = []
        self._add_log(hash_func(''), hash_func(''))

    @property
    def counter(self):
        return len(self.logs)

    def _add_log(self, _hash, data_hash):
        self.logs.append((_hash, data_hash))

    def _get_log(self, number):
        return self.logs[number]

    def hash_at(self, number):
        return self._get_log(number)[0]

    def data_at(self, number):
        return self._get_log(number)[1]

    @property
    def digest(self):
        return self.hash_at(self.counter - 1)

    def _prev_hash(self, number):
        return self.hash_at(number - 1)

    def _distant_hash(self, number):
        return self.hash_at(distant_ancestor(number))

    def add_hash(self, data_hash):
        """
        adds a hash32 to the notary.

        """
        assert len(data_hash)
        number = self.counter
        h = H(self._distant_hash(number),  # implements a skip list for O(log(n)) proofs
              H(data_hash, self._prev_hash(number)))
        self._add_log(h, data_hash)

    def get_proof(self, number, digest=False):
        """
        if digest:
            get a proof for a digest at number being included
        else:
            get a proof for data at number being included
        does include the data/digest at number
        does not include the current digest (which is what we plan to compute)
        """
        assert number < self.counter
        path = get_path(self.counter - 1, number)
        assert path[0][1] == self.counter - 1
        assert path[-1][1] == number
        path.pop(-1)
        path.reverse()
        hashes = []
        if digest:
            hashes.append(self.hash_at(number))
        else:
            hashes.append(self.data_at(number))
            hashes.append(self._prev_hash(number))
            hashes.append(self._distant_hash(number))
        for is_distant, number in path:
            if is_distant:
                # merge to distant hash
                hashes.append(H(self._prev_hash(number), self.data_at(number)))
            else:  # merge to prevhash
                hashes.append(self.data_at(number))
                hashes.append(self._distant_hash(number))
        return hashes


class PersistentNotary(Notary):

    _record_size = 2 * 32
    _dirty = False

    def __init__(self, fn):
        self._wfh = open(fn, 'a')
        self._rfh = open(fn, 'r')
        self._rfh.seek(0, 2)  # EOF
        self._counter = self._rfh.tell() / self._record_size
        if self.counter == 0:
            self._add_log(hash_func(''), hash_func(''))

    @property
    def counter(self):
        return self._counter

    def _add_log(self, _hash, data_hash):
        self._wfh.write(_hash + data_hash)
        self._counter += 1
        self._dirty = True

    def _get_log(self, number):
        if self._dirty:
            self.flush()
        self._rfh.seek(number * self._record_size)
        r = self._rfh.read(self._record_size)
        assert len(r) == self._record_size
        return r[:32], r[32:]

    def flush(self):
        self._wfh.flush()
        self._dirty = False

    def close(self):
        self._wfh.close()
        self._rfh.close()


def evaluate_proof(hashes):
    """
    first element in `hashes` should be the hash for which the proof was requested
    should return: the digest of the notary (at the time of requesting the proof)
    """
    assert hashes
    if len(hashes) == 1:
        return hashes[0]
    h = H(hashes.pop(0), hashes.pop(0))
    while hashes:
        h = H(h, hashes.pop(0))
    return h
