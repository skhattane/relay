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
- **A member that is not a machine: a remote MCP door for a voice client.** Wanted use: talking to Claude in the
  desktop app's voice conversation, which relays requests to a Claude Code session on a machine and reads the
  answer back aloud. That client cannot start a local stdio server; it only takes remote MCP connectors over
  HTTPS, added as custom connectors. What is missing:
  1. a remote MCP endpoint on the Worker (Streamable HTTP) exposing at least `send` and `status`;
  2. a `read` (or `wait`, long-polling with a timeout) tool, since that client receives no push: it returns the
     messages waiting for its seat and acknowledges them only when the tool result has been produced;
  3. an authorization the connector accepts — OAuth (authorization code with PKCE, a client the connector can
     register), bound to ONE seat of ONE room at consent time; the raw seat token never travels and the grant can
     be revoked from the room (`relay remove`);
  4. a seat with no machine and no local seat file (for example `voice`), created by an owner's invitation
     redeemed on the consent page;
  5. answers a voice can read: a session answering such a seat puts one plain sentence first and the detail after.
  To hold: a message from that seat is a teammate's request, never the user's approval — a permission prompt in
  the receiving session is still the user's to answer; the seat joins a room of its own with the one session it
  drives, not a room where sessions run unattended; messages stay in clear on the Worker until delivered (see
  « No end-to-end encryption »); a rate limit per grant.
- **A message carries more than text (later).** Wanted: a session can send a capture so a remote client (the voice
  one first) sees what is being talked about, and a person can send a picture back. To decide before the remote
  door fixes the wire: a message is a list of parts (text now; an image part later) so nothing has to change
  shape; an image is not kept in the room's own storage but in an object store, behind a link that expires, with
  a size limit; captures can show secrets, and they sit in clear on the Worker like the text (see « No end-to-end
  encryption »). To check first: whether the remote client really shows the model an image returned by a tool.
