<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2015, Joyent, Inc.
-->

# Manta directory count testing

Manta uses PostgreSQL triggers to maintain counts of objects in each directory
so that the directory count can be queried quickly on GET requests for the
directory itself.  The trigger is installed when any libmanta client connects to
the Manta metadata tier.  As part of initialization, the client checks whether
the trigger is installed and up-to-date and updates it if necessary.  This
process was historically subject to race conditions (resulting in incorrect
directory counts) and could also be extremely disruptive.

In this directory are provided several tools and test programs to help verify:

* that the trigger install process is correct
* that the trigger install process is race-free (via stress testing)
* that the trigger itself is race-free (via stress-testing)


## Testing the trigger install process in isolation

The trigger install process is abstracted so that it can be tested outside the
"manta" tables.  This makes iteration quicker and safer.

The "install" directory contains tools to test this process using a pair of
scratch Moray buckets (to avoid interacting with the deployed Manta
installation):

* `tg-show`: shows the testing buckets, triggers, stored functions, and the
  reported and actual object counts.  You can use this to manually verify the
  functionality of the install and reset tools.
* `tg-reset`: removes the testing buckets, triggers, and stored functions.  You
  can use this to restart testing from a clean slate.
* `tg-install [-c CONCURRENCY] VERSION`: installs version N of the trigger.
  For details on trigger versions, see below.  With `-c`, runs the trigger
  install process CONCURRENCY times in parallel for testing purposes.
  The result of each install attempt should be one of a few options: the process
  determines that no changes need to be made (because the installed trigger is
  already new enough), the process installs a newer trigger, or the process
  fails with a "tuple concurrently updated" error.  While scary, this last error
  is harmless, and in practice clients should retry until they get no error.
  For details on this, see the comment in the directory count trigger
  implementation.
* `tg-load`: applies load to the test Moray bucket by creating and removing
  objects.
* `tg-stress`: runs an end-to-end stress test.  This is the closest thing to an
  automated test.  See below.

The way this testing works is that we have:

* a "data" bucket (analogous to the "manta" bucket), which just contains
  arbitrary objects
* a "counts" bucket (analogous to the "manta\_directory\_counts" bucket).
  Objects in this bucket contain a version number, a count of inserts, and a
  count of deletes.  The idea is that each row in this table denotes the number
  of inserts and deletes completed at that version _of the trigger_.  There's a
  special row for version -1 that denotes all inserts and deletes, in order to
  test the case of trigger updates conflicting over a row.
* a counting trigger with multiple versions.  The trigger implementation for
  version N updates the corresponding row in the "counts" bucket every time an
  "insert" or "update" is completed.  It also updates the "counts" bucket row
  with version -1.

The idea is that we can stress-test this mechanism by running lots of insert and
delete operations, along with a lot of trigger update operations.  After an
extended test period, we check that:

* the total count of objects matches the delta between inserts and deletes
  across all the per-version counts in the "counts" table.  This enables us to
  say with some confidence that exactly one trigger was in effect across the
  entire test period.
* the total count of objects matches the delta between inserts and deletes for
  the global count in the "counts" table.  This reinforces that the mechanism
  works even when different triggers may update the same row.
* every trigger version has a non-zero number of inserts and deletes.  This
  enables us to say with confidence that the trigger update process actually
  does update the trigger.

The best, most automated test for this is the "tg-stress" program.  This program
resets the testing state, installs version 1 of the trigger, and then starts
applying load to the data bucket.  Lots of objects are created and removed
concurrently.  Periodically, the test also runs the "install\_trigger" process
with some concurrency.  The test alternates between attempting to install an
older trigger version or a newer trigger version.  The test continues until it
reaches a maximum trigger version, at which point it exits and reports how many
successful inserts and deletes it completed.  It compares all the counts and
makes sure things are consistent.


## Validating non-disruptive updates

It's critical that the trigger update process _not_ take any exclusive
PostgreSQL table locks when no change needs to be made.  Unfortunately,
PostgreSQL does not have a built-in way to observe when these locks are taken,
but it _does_ have a way to observe when these locks are contended.  The
[pglockwaits](https://github.com/joyent/pgsqlstat) program shows per-second
locks taken, including exclusive locks.

To verify that updates are non-disruptive, run `pglockwaits` (e.g., on a
primary Manatee peer) and make sure that the "AX" column is only non-zero when
the trigger actually gets updated.


## Testing the trigger install process for Manta

Of course, it's also important to verify that the trigger install process does
the right thing in the context of Manta.  A few tools are provided for this in
the "manta" directory:

* `manta-triggers-show`: shows the list of triggers and installed trigger
  functions associated with the "manta" table.
* `manta-triggers-reset`: attempts to remove versions of the Manta trigger
  _after_ the original (unversioned) one.  To actually reset the state of things
  to the way they were before we started versioning these triggers, you should
  restart a component running an older version of node-libmanta (e.g., an older
  jobsupervisor or muskie instance), and _then_ run this command.
* `manta-triggers-setup`: applies the Manta trigger update process, by just
  instantiating a node-libmanta client and then closing it.
* `manta-triggers-clobber VERSION`: forcefully sets the Manta triggers to
  pre-existing version N.  This overrides the normal mechanism, which updates
  triggers only if the new version is actually newer.  This is useful during
  testing to rollback a version of the trigger that you added during
  development.


## Testing trigger correctness

The trigger itself is known to be racy -- see MANTA-2720.

To verify this, there are a few tools provided in the "manta" directory:

* `manta-dircount-stress`: stresses the directory count code.  Given a scratch
  directory (whose contents will be destroyed!), removes all objects in it, then
  fills it with a predefiend number of objects with some concurrency.  This
  cycle repeats.  Each time the directory becomes full or empty, the reported
  count is compared to the actual number and the expected number.  If these do
  not match, the directory count has become incorrect.  Note that in order to
  test more edge cases, some inserts are made during the "wipe" phase, and some
  deletes are made during the "populate" phase, but they're bounded so that each
  phase converges.
* `manta-dircount-load`: similar to the "stress" version, but only creates and
  removes objects without actually verifying the directory counts.
