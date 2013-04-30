# Joyent Engineering Guide

Repository: <git@git.joyent.com:node-libmanta.git>
Browsing: <https://mo.joyent.com/node-libmanta>
Who: Mark Cavage
Docs: <https://mo.joyent.com/docs/node-libmanta>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This repo serves to hold any/all common code that is shared between Manta
components.  Currently a [mahi](https://mo.joyent.com/docs/mahi) client,
the indexing ring client, and some common utils exist.

# Testing

You'll need to define the morays you have for the index ring, as well as the
address of where to find Mahi (redis); for example, assuming the bellingham
lab (and you have DNS set appropriately):

```
INDEX_URLS=tcp://1.moray.bh1.joyent.us:2020,tcp://2.moray.bh1.joyent.us:2020 \
  MAHI_HOST=authcache.bh1.joyent.us make prepush
```
