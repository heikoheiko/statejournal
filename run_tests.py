#!/usr/bin/env python
import os
import subprocess

# storage = 'journal'
# for num_values in (10000, 100000, 1000000):
#     for num_accounts in (num_values, num_values/10, 1):
#         path = 'data_%s_%d_%d' % (storage, num_values, num_accounts)
#         for action in ('create', 'read', 'update', 'delete'):
#             cmd = ['time', 'python', 'chainmock.py', action, storage,
#                     str(num_values), str(num_accounts), path]
#             assert 0 == subprocess.call(cmd)
#             subprocess.call(['du', '-h', path])
#             subprocess.call(['du', '-h', os.path.join(path, 'state_journal')])
#             subprocess.call(['du', '-h', os.path.join(path, 'state_journal.idx')])
#             print '-' *20
#         print '- -' *20
#     print '===' *20



# storage = 'trie'
# for num_values in (10000, 100000, 1000000):
#     for num_accounts in (num_values,):
#         path = 'data_%s_%d_%d' % (storage, num_values, num_accounts)
#         for action in ('create', 'read', 'update', 'delete'):
#             cmd = ['time', 'python', 'chainmock.py', action, storage,
#                     str(num_values), str(num_accounts), path]
#             assert 0 == subprocess.call(cmd)
#             subprocess.call(['du', '-h', path])
#             print '-' *20
#         print '- -' *20
#     print '===' *20



# storage = 'journal'
# num_values = 100000
# num_accounts = num_values
# path = 'data_%s_%d_%d' % (storage, num_values, num_accounts)
# for action in ['create'] + ['update'] * 100:
#     cmd = ['time', 'python', 'chainmock.py', action, storage,
#             str(num_values), str(num_accounts), path]
#     assert 0 == subprocess.call(cmd)
#     subprocess.call(['du', '-h', path])
#     subprocess.call(['du', '-h', os.path.join(path, 'state_journal')])
#     subprocess.call(['du', '-h', os.path.join(path, 'state_journal.idx')])
#     print '-' * 20


storage = 'trie'
for num_values in (100000,):
    for num_accounts in (num_values,):
        path = 'data_%s_%d_%d' % (storage, num_values, num_accounts)
        for action in ('create', 'delete'):
            cmd = ['time', 'python', 'chainmock.py', action, storage,
                    str(num_values), str(num_accounts), path]
            assert 0 == subprocess.call(cmd)
            subprocess.call(['du', '-h', path])
            print '-' *20
        print '- -' *20
    print '===' *20
