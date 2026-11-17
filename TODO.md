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
- **Sessions as suffixes under one seat per machine.** Today one seat per machine per room, one receiving session
  per seat. Decided: a machine keeps ONE seat; its sessions are suffixes under it (`desk/build`, `desk/review`). A
  message with no suffix goes to the machine's designated main session; a message to an absent session waits a
  bounded time, then the sender is told it was not read.
- **End-to-end encryption, as an option per room.** The Worker's storage holds messages in clear until delivered.
  Decided: an option of a room, not a default. An encrypted room cannot hold a member that decrypts on the Worker,
  such as the hosted door; a door hosted elsewhere, with its own keys, could be a member.
- **Images.** Decided (owner, 2026-11-17): a real image returned by the door's `read` is the main path; a description
  written by the sender and a link the person can open come as a complement, never as a replacement. Delivery is a
  setting of the seat: the image itself in the tool's answer for a client that cannot open a link, a link for a
  machine. Tested 2026-11-17 with a temporary tool returning an image content block (blind picture: a number and a
  word the client was never told): a Claude Code session shows it to the model; the desktop app's VOICE mode does
  not — the model receives a text marker, at the call's turn and at the next turn too. That is a limit of that
  client, not of the relay; an image its user attaches to the conversation is seen. Also seen: a connected client
  keeps the tool list it loaded — a tool added later is seen only after the connector is reconnected. Not built yet.
  A link: on the Worker, a random token in the path (the link is the key), short-lived (15 min by default, one
  view as an option), the picture kept in the room in clear until it expires; not available in an encrypted room.
- **Several clients on one connector seat, what stays open.** Each client is told apart by its MCP session (the
  `Mcp-Session-Id` the door issues at initialize); this is not yet checked with the real clients: a client that opens
  a new session for every conversation (or every turn) starts each one at the seat's frontier, so a message another
  client read first would be skipped by it — to be seen with a phone and a desktop under one account. A client that
  sends no session header still shares one cursor per grant. The door does not take an HTTP DELETE to end a session
  (a cursor goes by the cap of 8 or after 7 days unused). A client away longer than the keep time (24 h, 200 read
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
  unread; read messages kept 24 h (200 per seat) for the other clients; sessions bound to their grant, seat and room;
  revoke and remove forget them.
