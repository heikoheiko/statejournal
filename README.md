# StateJournal

PoC of an efficient journal based cryptographically authenticated data structure.

The basic idea is to update a hash function with the stream of state updates,
which are written to a journal. The latest state is directly mapped to a key value store.

While the Merkel Patricia Tree is good at supporting access to states in different chains,
it has runtime, memory and storage issues.

Potential Improvements:
    - x50 lower storage requirements (x20 if keeping the full journal, x1000 if deleting storage)
    - x12/x17 faster for reads/writes
    - x135/x70 lower system IO for reads/writes
    - x6 lower memory footprint (levedb needs to cache key prefixes)
    - supports deletion of old data
    - straight forward implementation (direct k,v mapping + journal)


### Storage and Performance Comparisons:

https://docs.google.com/spreadsheets/d/1fHios5d3tTMBy2pUjQ4ll0fBiZMmtfxyVHXJFs9Jx6M/edit
