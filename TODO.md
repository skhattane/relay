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
- **Sessions, what stays open.** A session name is a label, not a boundary: the sessions of one machine share the seat
  file, so any of them can take another's name over; a per-session secret would be needed for more, and the decision
  so far is that they are one trust unit. An older plugin shows `from desk` for a session's message (its reply goes to
  the main session) and its `send` tool refuses the `desk/build` form. A decision records the seat only, not the
  session whose shell it was typed in. A message to the machine handed to a main session that leaves before confirming
  it can be shown by the next main too (twice, never lost). The sender of an unread message is told, but not a sender
  whose seat left the room meanwhile. A session that is suspended (Ctrl-Z) still holds its name and acks (see the first
  point); now that a machine runs several sessions, the main one being suspended is the case that matters.
  A half-open main connection (a network that drops without a close) holds the main role until the room notices it is
  gone; meanwhile messages to the machine are pushed to it and wait unacked. A corrupt outbox file is read as empty and
  replaced by the next write, so what it held is lost.
- **End-to-end encryption, as an option per room.** The Worker's storage holds messages in clear until delivered.
  Decided: an option of a room, not a default. An encrypted room cannot hold a member that decrypts on the Worker,
  such as the hosted door; a door hosted elsewhere, with its own keys, could be a member.
- **Pictures, what stays open.** A desktop app's VOICE mode does not show its model an image returned by a tool (tested
  2026-11-17 with a blind picture: the model receives a text marker, at the call's turn and the next, while a coding
  assistant's session does show it to its model); a limit of that client, not of the relay — such a seat can be set to `link` or rely on the description. A connected client keeps the
  tool list it loaded: the door's changed read is seen only after the connector is reconnected. Not available in an
  encrypted room (for a later version: an encrypted room cannot let the Worker sniff or serve a picture). The door's
  `send` carries no picture (a remote client has no file to give). The platform's request log records the path of a
  link that is opened (its token), for the link's short life; the Worker's own lines never do. A picture that a
  machine fails to fetch is still acknowledged, so it is lost for that seat (the session is told). The 7-day prune of
  a picture is the same deletion as the message's. Not yet checked with a real client: that the door's image content
  block is shown to the model by the phone and desktop apps outside voice mode. Untested: the 7-day prune of a picture
  on its own, the socket's refusal of a picture, a symlink planted in the machine's local pictures folder.
- **A link whose token is in no request log.** Serve a link as `/i/<room>#<token>` with a tiny page that posts the
  fragment to the relay: a fragment is never sent in a request, so the token would appear in no request log, and a chat
  application's preview (which fetches what is pasted, using up a one-view link and caching the picture) could not
  consume the link. Not built in this version.
- **Several clients on one connector seat, what stays open.** Each client is told apart by its MCP session (the
  `Mcp-Session-Id` the door issues at initialize); this is not yet checked with the real clients. Where a new session
  starts is the open choice. Today it starts where some client of the seat has already read: a client that opens a
  new session for every conversation (or every turn) would then SKIP what another device read first. The alternative,
  starting at the lowest live cursor of the same grant, would REPLAY instead (messages that client already saw in an
  earlier session). Which is right depends on how the real clients reuse sessions — to be seen with a phone and a
  desktop under one account before changing it. A client that
  sends no session header still shares one cursor per grant. The door does not take an HTTP DELETE to end a session
  (a cursor goes by the cap of 8 per grant or after 7 days unused). A client away longer than the keep time (24 h, 200 read
  messages per seat) is not told that it missed messages.
- **A delivery receipt naming which client read.** Wanted: the sender can learn that a message was read, and by
  which client (machine session, or the connector's client name; on a seat with several clients, which of them).
- **Reading the kept logs from the CLI.** The Worker keeps its logs (one line per door request); reading them
  needs a token with Workers Observability permission, not the deploy token.
- **The inbox line format is not documented by the vendor.** The fallback is tested; a check at start that the
  inbox really delivers (without injecting a message) does not exist yet.
- **Decisions, what stays open.** Nothing answers the assistant's permission prompts by itself — a machine owner who
  wants that writes a hook calling `relay ask --wait` (example in the README, installed nowhere, tested); a session
  has no `ask` tool of its own; a decision request cannot be asked from the remote door; the door's `decide` relies
  on the client model to repeat the spoken option back, which the relay cannot check.
- **Shared state, what stays open.** Nothing waits for a change (a script polls `relay get`, or a session reads the
  « state: » notice); the 256-names cap on tombstones is not exercised by the tests; values are in clear on the
  Worker like messages.
- **The shape is a convention.** The header is the only line written flush left and every line of content is behind
  `│ ` (plugin inbox, channel fallback, door). It is a convention the model is told about, not a channel it cannot
  read across: a model that ignores its instructions can still be persuaded by content. Acts that carry a machine's
  authority refuse to run in a session's shell, by its environment: a speed bump, not a lock.

# Done

- **The remote door** (branches remote-door-prototype, remote-door-2): a member that is not a machine, over MCP
  Streamable HTTP, with `send`, `status`, `read`.
- **The wait cap**: a door `read` waits at most 25 s (default 20), a larger ask clamped, under the client's ~30 s cut.
- **Kept logs**: Workers Logs on, one JSON line per door request and per OAuth endpoint, never a secret or a text.
- **`news`**: what waits on the door, without taking it.
- **The say line**: one sentence to say aloud, stored apart from the detail.
- **OAuth access** (branch remote-door-oauth): a connector seat entered with a one-time enrolment code, no secret in
  any URL, grants listed and revoked by an owner.
- **Decisions** (branch decisions): a numbered question with fixed options, answered only by a marked decider.
- **Shared state** (branch room-state): a per-room key → value table, versioned, announced, owners write.
- **A cursor per client on a connector seat** (branch door-cursors): two clients of one seat (a phone and a desktop
  under one account) each see every message, once; a new client replays nothing already read and misses nothing
  unread; read messages kept 24 h (200 per seat) for the other clients; 8 cursors per grant, 32 per seat; sessions bound to
  their grant, seat and room; revoke, remove and the replay protections forget them; the old batch text emptied.
- **Sessions as suffixes** (branch session-suffix): one seat per machine, its sessions named under it (`desk/build`,
  neutral `s1`, `s2` by default); `desk` reaches the main session (the one that ran `relay join` last, else the oldest),
  `desk/build` that session, waiting 10 minutes while it is away, then the sender is told; `all` reaches each machine once; at most 8 sessions per seat and
  50 waiting messages per sender and machine; every right stays the seat's.
- **Pictures** (branch images): one picture per message (PNG, JPEG, WebP, GIF by their bytes; 1 MiB), a description
  beside it; a machine fetches it with its own token into a private bounded folder and shows the local path; the door
  returns a real image block, once per client session; a short-lived link (15 min, one view as an option) only on
  request; a setting per seat (inline, link, off); a picture lives and dies with its message; caps per message, per
  sender per minute and per room.
