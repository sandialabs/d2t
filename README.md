d2t
===

Doubly Distributed Transactions

Last updated: June 10, 2014
For current information, please refer to
http://www.lofstead.org/txn
or contact
Jay Lofstead (gflofst@sandia.gov)

This distribution contains the Doubly Distributed Transaction library, test
code and associated support libraries for the test code.

The library directory contains the core library code. It only relies on MPI
currently.

The test directory has an example test harness that is used in the published
papers to demonstrate how to use the library.

The services directory contains example services used by the test harness.
These services may be useful as a starting point for other purposes as well.

When referencing this work, please use one of the entries in the included
at the website. For quick reference:
Lofstead 2012 Cluster (original protocol)
Dayal 2013 HPDIC @ IPDPS (use in containers)
Lofstead 2013 PDSW (optimized protocol)
Lofstead 2014 Cluster (submitted) (optimized protocol discussion + services)
