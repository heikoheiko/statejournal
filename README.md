# statejournal

Efficient journal based cryptographically authenticated data structure
 as an alternative to the Merkel Patricia Tree

The basic idea is to update a hash function with the stream of state updates,
which are written to a journal, instead of using a merkel tree.
The latest state is directly mapped to a key value store.

While the Merkel Patricia Tree is good at supporting access to states in different chains,
it has runtime, memory and storage issues.


### Storage and Performance Comparisons:

https://docs.google.com/spreadsheets/d/1fHios5d3tTMBy2pUjQ4ll0fBiZMmtfxyVHXJFs9Jx6M/edit
