This is the basic client-side only transaction protocol. An example test
harness is separate that depends on the datastore and metadata services.

Usage requirement:
1. In order for the commit phase to work properly, there must be a touch event
with servers after phase 1 of the two-phase commit. Otherwise, the server
could have died, but it would be unknown.
