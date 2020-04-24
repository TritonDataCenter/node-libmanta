<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2020 Joyent, Inc.
-->

# node-libmanta

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

This repo serves to hold any/all common code that is shared between Manta
components.  Currently a [mahi](https://mo.joyent.com/docs/mahi) client,
the indexing ring client, and some common utils exist.

# Testing

You'll need an existing Manta deployment to run the test suite.  You should
set the `ELECTRIC_MORAY` environment variable to the hostname or IP address of
an electric-moray instance in your deployment and the `MAHI_HOST` environment
variable to the hostname or IP address of an authcache instance in your
deployment.  As an example, in a deployment called `emy-10` with DNS configured
in your development zone, you might set these to:

```
$ export ELECTRIC_MORAY=electric-moray.emy-10.joyent.us
$ export MAHI_HOST=authcache.emy-10.joyent.us
$ make prepush
```

# Release process

Manta components that use libmanta should use versions tagged in git via:

    "libmanta": "git+https://github.com/joyent/node-libmanta.git#v1.2.3"

To release a new libmanta:

1. Make one or more commits that include **updating the package.json "version"
   field**.

2. Get that commit approved and integrated via a **pull request**.

3. Update your local copy and run **`make cutarelease`** to git tag and push.

(Note: Some earlier version of libmanta were published to npm. That practice
is deprecated for this module because "libmanta" isn't a generally useful
module that has a place in the npm registry.
