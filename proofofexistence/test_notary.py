import os
from notary import hash_func, Notary, PersistentNotary, evaluate_proof, get_path


def test_notary(num_entries=200, fn=None):
    data = [hash_func(str(i)) for i in range(num_entries)]

    if fn:
        n = PersistentNotary(fn)
    else:
        n = Notary()
    assert n.counter == 1
    for d in data:
        n.add_hash(d)
    assert n.counter == num_entries + 1
    # test proof for data
    for target in [num_entries - i for i in range(num_entries)[:-1]]:
        hashes = n.get_proof(target)
        assert hashes[0] == n.data_at(target)
        assert evaluate_proof(hashes) == n.digest
    for target in [num_entries - i for i in range(num_entries)[:-1]]:
        hashes = n.get_proof(target, digest=True)
        assert hashes[0] == n.hash_at(target)
        assert evaluate_proof(hashes) == n.digest
    if fn:
        n.close()


def do_test_persistent_notary(fn):
    num_entries = 200
    test_notary(num_entries=num_entries, fn=fn)  # creates a notary
    # reopen and get a proof
    n = PersistentNotary(fn)
    assert n.counter == 201
    assert evaluate_proof(n.get_proof(20)) == n.digest


def test_persistent_notary(tmpdir):
    do_test_persistent_notary(os.path.join(tmpdir.dirname, '_notary.tmp'))


def test_paths():
    yr = 365 * 24 * 3600
    lengths = []
    targets = [i / 10. for i in range(1, 10)]
    for i in [yr * j for j in range(1, 10)]:
        for t in reversed(targets):
            t = int(i * t)
            assert t < i
            p = get_path(i, t)
            # print i, t, len(p), len(p) * 32 / 1024 ,'kB'
            lengths.append(len(p))
    print 'max length', max(lengths)
    print 'avg length', sum(lengths) / len(lengths)


def do_test_speed(fn, num_entries=10000):
    """
    can add ~100k entries / second
    """
    n = PersistentNotary(fn)
    for i in range(num_entries):
        n.add_hash(hash_func(str(i)))
    n.flush()


def do_test_proof_speed(fn, num_proofs=10000):
    """
    requires a populated database
    can generate ~40k proofs / second
    """
    n = PersistentNotary(fn)
    num_entries = n.counter
    assert num_entries > num_proofs
    for i in range(1, num_proofs, num_entries / num_proofs):
        n.get_proof(i, digest=False)


if __name__ == '__main__':
    import sys
    # do_test_persistent_notary(sys.argv[1])
    # do_test_speed(sys.argv[1], num_entries=1000000)
    do_test_proof_speed(sys.argv[1], num_proofs=100000)
