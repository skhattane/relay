# Open points

Seen in use, not yet done.

- **A seat held by a session that no longer reads.** A session stopped by job control (Ctrl-Z) keeps its server
  running: the server holds the seat, writes each message into an inbox nobody reads, and acks it. The sender
  is told « sent » and the message is lost; a session started afterwards stays out of the held seat, and only
  `status` says so. Wanted: the server notices that its session is stopped (its parent's state, or an inbox
  that stops draining) and releases the seat instead of acking; a session that finds its seat held says so IN
  the session at start, not only in `status`; `status` names a holder that is suspended.
- **A new machine without a key to the repository.** Today a machine needs a clone before it can install the
  plugin. Wanted: a way to fetch the plugin with an invitation alone.
- **No end-to-end encryption.** The Worker's storage holds messages in clear until delivered.
- **One seat per machine per room.** Two sessions of one machine cannot both receive in the same room.
- **The inbox line format is not documented by the vendor.** The fallback is tested; a check at start that the
  inbox really delivers (without injecting a message) does not exist yet.
