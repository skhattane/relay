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
  (3 and 4 built on the branch remote-door-oauth: `relay connector` + an enrolment code on the consent page,
  `relay grants`, `relay revoke`; the URL door and `relay remote-seat` are removed.)
  To hold: a message from that seat is a teammate's request, never the user's approval — a permission prompt in
  the receiving session is still the user's to answer; the seat joins a room of its own with the one session it
  drives, not a room where sessions run unattended; messages stay in clear on the Worker until delivered (see
  « No end-to-end encryption »); a rate limit per grant.
- **A message carries more than text (later).** Wanted: a session can send a capture so a remote client (the voice
  one first) sees what is being talked about, and a person can send a picture back. To decide before the remote
  door fixes the wire: a message is a list of parts (text now; an image part later) so nothing has to change
  shape; an image is not kept in the room's own storage but in an object store, behind a link that expires, with
  a size limit; captures can show secrets, and they sit in clear on the Worker like the text (see « No end-to-end
  encryption »). How an image reaches a member is a setting of the SEAT: a client that cannot open a link (the
  voice one, most likely) receives the image itself in the tool's answer, within the size limit; a session on a
  machine receives the link and fetches it. To check first: whether the remote client really shows the model an
  image returned by a tool.
- **The remote door, seen in its first hour.** A `read` waiting 40 s was cut by the client at about 30 s (« the
  connector's server isn't responding »); the message was not lost (the next `read` delivered it). Wanted: the
  wait capped at 25 s on the server (default 20); the Worker's logs kept (observability on) so such a moment can
  be read instead of deduced; a « what is new since my last read » digest, since that client gets no push; each
  message carrying one line to say aloud and the detail apart, so two models do not summarise in a chain.
  (Built on the branch remote-door-2: the 25-second cap, `observability` on with one log line per door request,
  `news`, and `say` beside the text. Still open: reading the kept logs from deploy.sh, which needs a token with
  Workers Observability permission, not the deploy token.)
- **A person answers a precise question through any door.** Wanted: a member reached by voice (the remote door)
  can answer « yes » or « no » to a numbered question from a session, and the answer is data with a known author,
  not a sentence a model interprets; a script or a hook can wait for it without a model. (Built on the branch
  decisions: `relay ask` / `asks` / `decision` / `decide` / `withdraw`, a per-seat decider mark set only by an owner,
  the door tools `asks` and `decide`, `ask --wait` printing one JSON line with exit 0/3/4.) Still open: nothing
  answers the assistant's permission prompts by itself — a machine owner who wants that writes a hook calling
  `relay ask --wait` (example in the README, installed nowhere); a session has no `ask` tool of its own (it can run
  the command); a decision request cannot be asked from the remote door; the door's `decide` relies on the client
  model to repeat the spoken option back, which the relay cannot check.
- **Machines follow a value without a model.** Wanted: one machine's script writes « the value everyone should
  follow » and each machine's script reads it with its own seat. (Built on the branch room-state: a per-room
  key → value table, `relay state` / `get` / `set` / `del` / `state-open`, versions that never go backwards and
  `--if-version`, every change announced by relay, owners write unless a key is opened to members, the door reads
  only, a credential guard.) Still open: nothing waits for a change (a script polls `relay get`, or a session reads
  the « state: » notice); the 256-names cap on tombstones is not exercised by the tests; values are in clear on the
  Worker like messages.
- **What a session reads has a fixed shape.** The header is the only line written flush left; every line of
  content, whoever wrote it, is behind `│ `, split on every Unicode line terminator, its control and format
  characters shown as `·` (plugin inbox, channel fallback, remote door). Still open: the shape is a convention the
  model is told about, not a channel it cannot read across — a model that ignores its instructions can still be
  persuaded by content; on the door, a message with a say line has three room lines (header, `SAY:`, `DETAIL:`).
