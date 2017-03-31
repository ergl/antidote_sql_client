## Architecture

This document is meant to give a high-level overview of the design of the
SQL interface to Antidote.

This client works using Antidote's typescript client, which uses the
protocol buffers interface to communicate with an Antidote server.

At the lowest level, this interface implements an ordered key-value store on
top of Antidote. The values are stored in LWW-registers, exposed natively by
Antidote.

Most of the complexity in the implementation is in the ordering relation
between keys. The design for this ordering follows the main ideas used by both
Google's [F1 SQL database](https://research.google.com/pubs/pub41344.html) and [CockroachDB](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/),
consisting in a hierarchical encoding, where keys are classified depending on the kind
of value they point to.

This ordering is implemented client-side, as Antidote (last I looked) doesn't
expose an ordered key sequence structure. If this was implemented, the
implementation would be more straightforward and more efficient.

In practice, this means that each table owns an ordered key set, and therefore
knows about _all_ the keys in that table. This can be a problem for tables with
lots of rows, which could be mitigated by partitioning this set. Yet another
problem is the presence of write-write conflicts among concurrent transactions
that insert (or update) keys into this set. Read-only transactions are not affected.

In addition to this set, each table also knows about its own metadata record,
containing schema, primary key fields, foreign keys and any [unique] indices
associated with it. If using autoincremented primary keys, it also holds a
counter indicating the last value used. Note that in the current version, only
autoincremented primary keys are supported.

### Key ordering

As of the current version of this interface, the following key hierarchies are identified:

- Table keys
- Primary index keys
- Field keys
- Index keys
- Unique index keys

Between keys in the same hierarchy, a simple lexicographic ordering is applied.
Ordering between keys in different hierarchies depends on the specific hierarchies
being compared, following the following partial order:

Table keys > Primary index keys > Field Keys
Table keys > Index keys
Table Keys > Unique index keys

At the implementation level, the following order also holds true:

Primary index keys > Index keys > Unique index keys

although this detail is not crucial, and is only used for convenience.

### Key structure

The above hierarchy is represented by using common prefixes. Most (if not all)
of the key structure is based on the Cockroach post, linked above. They surely
do a better job at explaining it than me.
