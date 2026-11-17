<p align="center">
  <img src="assets/relay-banner.svg" alt="relay — rooms, seats, and messages that pass from one session to the next" width="100%">
</p>

# relay

Messages between the lead sessions of a coding assistant running on several machines.

A session receives a message in its own inbox, as a message whose first line says where it is from:

```
[relay · room ops · from laptop · #100042 · 2026-11-15T20:40:49Z] (reply with the relay send tool, to "ops:laptop")
│ text
│ more text
```

and answers with the relay `send` tool. Where no inbox socket is found, the message arrives as a preview
channel event instead (`<channel source="plugin:relay:relay" room="ops" from="laptop" msg_id="…" sent_at="…">`, with
the same text; see « If the inbox stops working » below).

**The shape is the defence.** The header is the only line the plugin writes flush left; every line of the body is
behind `│ `, whoever wrote it — a member, a connector, the room's own notice quoting a name or a stored value. The
body is split on every line terminator Unicode knows (CR LF, CR, LF, VT, FF, U+001C–U+001E, U+0085, U+2028,
U+2029), and inside a line each control character (C0, C1; tab aside) and each format character (zero-width,
bidirectional controls, BOM…) is shown as `·`. So no content can produce a line that reads as a header or a
decision; a decision is only the header the plugin renders from the room's structured field,
`[relay · room ops · DECISION #12 · allow · by voice · remote]`. The remote door's answers have the same shape:
each message under a line the room writes, `SAY:` and `DETAIL:` alone on their lines, every line of content behind
`│ `. The room also refuses, at input, line breaks and control or format characters where one line is expected (a
say line, a note, an option, a name, a key; an option also refuses `[ ] · « » │ "`), control characters and
bidirectional controls in a message or a detail, and control or format characters in a state value; every notice
starts with the room's own words (`member b joined the room, invited by a`). Those refusals are hygiene; the shape is
what holds. A message may hold other format characters (an emoji joined by a zero-width joiner, say): it is accepted
and shows with `·` where the joiner was. A decided request's header quotes its option as a JSON string; an expired or
withdrawn one has its own header, `[relay · room ops · REQUEST #12 EXPIRED]` (or `WITHDRAWN`), never the DECISION form.

Either way the session is told it is a message from another session: a teammate's request, never the approval of the
person at the keyboard.

**Acts that carry the machine's authority are terminal-only.** `decide`, `decider`, `set`, `del`, `state-open`,
`revoke`, `remove`, `connector` and `images` refuse to run in a shell started by an assistant session (any of `CLAUDECODE`,
`CLAUDE_CODE_ENTRYPOINT`, `CLAUDE_CODE_SESSION_ID` set), whether typed directly or through `/relay` (`relay cmd`):
« this act carries the machine's authority: a person runs it in a terminal of their own, outside the assistant's
session ». No flag overrides it. A session has no tool to write the state either.

What this is: a guard against a session doing these acts because a message asked it to. What it is NOT: a boundary. A
session that has a shell holds the machine's seat file and can call the Worker itself, or clear its environment. The
real boundary for decisions is which SEAT the owner marks as a decider: mark a seat a person holds — a remote
connector seat they enrolled, or a machine no unattended session runs on — never a machine where unattended sessions
run.

The post office is one Cloudflare Worker with one Durable Object per room; each listening session keeps one
WebSocket per room open to it, so a message is pushed the moment it is sent.

## The model

- A **room** is a name (`[a-z0-9][a-z0-9-]{0,31}`) and one object of its own: a message never leaves its room.
  Rooms are created with the admin key only; members come in by invitation only.
- A **seat** is a member of a room: a random token created on the machine (`~/.config/relay/seats/<room>.json`,
  mode 0600). The relay keeps only its sha256. One seat per machine per room; the sessions a machine runs are
  **suffixes** under it, `desk/build`, `desk/review` (see « Sessions » below).
- A seat has a **name** in each room (unique there, changeable at any time; `all` and `relay` are reserved) and
  a **role**: `owner` (the creator) or `member`.
- **Any member** can invite: `relay invite` prints a one-time link valid 24 h. Only an owner can remove a member.
  The last owner cannot leave. Rooms are not deleted in this version.
- **Nothing enters a room silently**: an invitation minted, a member joining (and who invited it), removed,
  leaving or renamed is announced to every member as a message from `relay`, stored like any other message
  so absent members hear it when they come back. Nobody can send as `relay`.
- Each **session** of a machine receives on a connection of its own, under its own name. A session listens to the
  rooms listed for its working directory in `~/.config/relay/projects.json`; at start it takes a free name and
  never one another session holds. `relay join <room>` is explicit: the session listens and becomes the machine's
  **main** session for that room.
- Delivery: the session's server writes each message into the session's inbox socket (one connection per
  message) and acks it to the relay only once written; without an inbox it falls back to the channel event.
- A machine whose seat the relay no longer knows (removed) deletes its seat file and forgets the room.
- Messages go to one member by name (its main session), to one session of it (`name/session`), or to `all`,
  inside a room. They wait up to 7 days for a member that is away (10 minutes for a session that is away, then the
  sender is told) and are delivered once. A message that cannot leave because the relay is unreachable waits in a
  local outbox, shared by the machine's sessions.
- Guardrails: 48 KiB per message; 60 messages per sender per minute, a message to `all` counting one per
  recipient; the same text to the same target within 10 seconds is stored once, and the sender is told.
- A message may carry one **picture** (see « Pictures »).

## Install

The repository is a plugin marketplace holding one plugin, `relay`: the server and the `/relay` command come
together. Python 3 (standard library only) on every machine; for encrypted rooms the `cryptography` package is used when it
is installed, and a built-in implementation otherwise.

```
git clone <this repository> ~/src/relay
claude plugin marketplace add ~/src/relay      # tells the CLI where this catalogue of one plugin is
claude plugin install relay@relay              # installs the plugin: the server and the /relay command
```

Then start the CLI normally. In every session the plugin's server starts: it links `~/.local/bin/relay` to
itself (so a plain terminal and `/relay` reach the same program), offers its tools on any machine that holds a
seat, and connects only for the rooms the session listens to.

The machine that will hold the admin key deploys the Worker once (Node.js, and a Cloudflare API token with
Workers Scripts:Edit in the file named by `$CF_WORKERS_TOKEN_FILE`):

```
cd ~/src/relay
plugin/bin/relay admin-key   # creates ~/.config/relay/admin, prints only its sha256
./deploy.sh deploy           # prints the relay's address, https://relay.<your subdomain>.workers.dev
```

### A first room

```
/relay create ops --url https://relay.<your subdomain>.workers.dev   # on the machine with the admin key
/relay invite ops                                                    # prints https://relay.…/j/ops/<secret>
/relay accept https://relay.…/j/ops/<secret>                        # on the other machine
/relay join ops                                                      # on each machine, in the project's session
```

`join` makes the running session listen at once, as the machine's main session for that room, and records the room
for its directory, so later sessions started there listen too, each under a name of its own.

### If the inbox stops working (the fallback)

The session inbox is the normal road: no launch option, no confirmation screen. Its format is not documented
by the vendor, so an update of the CLI may break it. The sign: `/relay status` shows the room connected, the
sender is told « sent », and nothing appears in the session. The plugin also declares itself as a preview
channel, the road it falls back to when it finds no inbox; that road only shows messages in a session started
with the channel option, which asks for a confirmation at each start:

```
claude --dangerously-load-development-channels plugin:relay@relay
```

To force the fallback while the inbox still exists (a test, or an inbox that accepts lines and drops them),
start the session with `RELAY_INBOX=/nonexistent` in its environment as well.

If a « Held message from another session » dialog ever appears for a relay message, something other than the
plugin's own server wrote into the session: the server is a child of the session and is never held. Deny it.

### Update

`git pull` in the clone. A marketplace added from a local path is read in place; the change shows at the next
session start. For this version, in this order:

1. update the plugin on every machine first — it is harmless against the old Worker, and the protection against a
   forged header lives in the plugin (a machine on the old plugin shows message bodies without the `│ ` prefix; it
   also acknowledges a message with a picture without fetching it, so the picture is lost for that seat);
2. deploy the Worker (its notices now start with « member … », and the door's `read` and `news` changed layout);
3. mark deciders only after both;
4. ask remote clients to reconnect, so they load the door's new tools and instructions (a connector session keeps the
   old ones until then).

For sessions (« Sessions » below) the order does not matter: a session on the older plugin keeps working against the
new Worker as its machine's only, main session. When the Worker is deployed every connection is closed once (« the
relay was updated ») and reconnects by itself; an older session comes back as its machine's main session. Restart a
session to give it the new plugin and its own name.

### Uninstall

```
/relay leave-room ops                    # for each room, to free the seat (optional)
claude plugin uninstall relay@relay
claude plugin marketplace remove relay
rm ~/.local/bin/relay                    # the link the server made
```

Seats and settings stay in `~/.config/relay`, outboxes in `~/.local/state/relay`; delete them to forget
everything.

## Commands

The same words work as `/relay …` in a session, where the relay program answers with no model turn (`/relay`
alone is `status`, `/relay help` lists the commands), and in a shell as `relay …`.

| command | what it does |
|---|---|
| `status` | the rooms this session listens to, who is connected, and the sessions of each machine |
| `list` | every room this machine has a seat in |
| `join <room>` / `leave <room>` | the running session starts listening to a room as the machine's main session / stops, no restart |
| `send <[room:]name[/session]\|[room:]all> [--say s] [--image f [--describe d]] <text…>` | one message (the room may be left out when there is only one); `--say`: one sentence to be said aloud; `--image`: one picture, `--describe`: one line saying what it shows (see « Pictures ») |
| `rename [<room>] <name>` | this machine's name in a room |
| `create <room> [--name n] [--url u] [--encrypted]` | a new room; needs the admin key; `--encrypted`: end to end (« Encrypted rooms ») |
| `invite <room>` | a one-time link, valid 24 h |
| `accept <link>\|- [--name n]` | takes a seat in that room (`-`: the link read from standard input) |
| `remove <room> <name>` | (owner) takes a member's seat away at once |
| `leave-room <room>` | gives up this machine's seat |
| `connector <room> <name>` | (owner) a seat for a remote connector that connects by OAuth; prints its URL, writes a one-time enrolment code to a file |
| `grants <room>` / `revoke <room> <name>` | (owner) the connector grants, without secrets / delete one seat's grants (the seat stays) |
| `ask <room> --say s --option a --option b […] [--wait]` | a decision request (see « Decisions »); `--wait` prints one JSON line, exit 0/3/4 |
| `asks <room>` / `decision <room> <n>` | the open decision requests / the state of one, as one JSON line |
| `decide <room> <n> <option> [--note t]` / `withdraw <room> <n>` | (a decider) answer a request / (its author) withdraw it |
| `decider <room> <name> on\|off` | (owner) mark or clear a member as a decider — only a seat a person holds, never a machine where unattended sessions run |
| `state <room>` / `get <room> <key> [--json]` | the room's shared state / one key's value alone, exit 3 if absent (see « Shared state ») |
| `set <room> <key> <value> [--if-version n]` / `del <room> <key> [--if-version n]` | write / delete a key; exit 5 on a version conflict |
| `state-open <room> <key> members\|owners` | (owner) who may write that key |
| `image-link <room> <message> [--minutes n] [--once]` | a link a person opens in a browser to a picture you sent or received with that message (15 min by default, at most 60; `--once`: one view) |
| `images <room> <name> inline\|link\|off` | (owner) how a seat receives pictures: the image itself (default), a short-lived link instead, or the description only |
| `rekey <room>` | (owner, encrypted room) a new key generation, printed to pass person to person |
| `key <room> [--dropped]` / `take-key <room>#k<n>.<key>` or `take-key <room> -` | (encrypted room) this machine's current key of the room, to pass on / keep a key a member passed you (`-`: read from standard input) |
| `help` | one line per command |
| `admin-key` · `serve` | shell only |

`list` reads:

```
ops
  • laptop (you) — connected · laptop.local · 203.0.113.7
    sessions: api (main, this session), web
  • desk — not connected
```

In a session, the server's tools are `send {to, text, say?, room?, image?, describe?}` (`to` may be written `room:name` or
`room:name/session`; `image` is the path of one local picture),
`status {}`, and for the shared state `state {room?}` and `get {key, room?}` (read only: the state is written by a
person, with `relay set`); the CLI shows them under the plugin's prefix.

A message may carry **a line to say aloud**: `say` is one plain sentence, `text` the detail (it may then be
empty). The room stores the two apart; a machine receives `text` as the sentence, a blank line, then the detail
(and the sentence again in a `say` field), so a client that knows nothing of it shows the whole message. When a
session answers a remote seat (« reads by itself (remote door) »), it puts one sentence in `say`.

## Sessions

Here a **session** is one coding-assistant session running on a machine, under that machine's seat — not the
**client session** of a connector seat on the remote door (« Several clients on one seat » below), which has no name
and cannot be addressed.

A machine keeps **one seat** per room, whatever number of sessions it runs. Each session is a **suffix** under that
seat: `desk/build`, `desk/review`. Two addressing rules:

1. **`<room>:<seat>` reaches the machine's main session.** `relay send ops:desk "…"` goes to whichever session of
   `desk` is main now. `ops:all` reaches every other machine once, at its main session, never each of its sessions.
2. **`<room>:<seat>/<session>` reaches that session.** `relay send ops:desk/review "…"` goes to `review` only. If no
   session `review` of `desk` is connected, the message waits 10 minutes; a session that connects under that name
   in time reads it. Past that it is deleted and the sender is told, by a notice from `relay` that names the message
   and its ref. The 10 minutes run only while the session is absent: a message handed to a connected session is
   confirmed or handed again at its reconnect like any other, and its 10 minutes start only if the session goes away
   without confirming it:

   ```
   [relay · room ops · from relay · #100311 · 2026-11-16T21:02:10Z]
   │ message #100307 to desk/review was not read: no session review read it within 10 min, so it was deleted (ref 3f9c…, sent 2026-11-16T20:52:10Z)
   │ its beginning: please rebase on main
   ```

   The notice goes back to the session that sent the message when it is still connected, else to the sending
   machine's main session (a notice of the room is not subject to the 10-minute deletion — the 7-day limit of every
   message still applies: if that session leaves before reading it, it goes to the main one). A connected session that never confirms a message keeps it until the 7-day limit, which tells nobody; the cap of waiting messages per sender bounds it. The notice's frame carries `unread: {id, ref, to, at}` as data.

A message from a session shows its author as `<seat>/<session>`, and the reply hint names that address:

```
[relay · room ops · from desk/build · #100042 · 2026-11-16T20:40:49Z] (reply with the relay send tool, to "ops:desk/build")
│ tests are green
```

`relay send` run inside a session's own shell (`/relay send`) names that session as the author too; from a
person's terminal, a message has no session and a reply goes to the machine's main session. A message queued while the relay
is unreachable keeps its author: any session of the machine may send what waits in the shared outbox, but under the
session that wrote it — one whose session never got its name waits for it while that session's process lives, then
goes with no session at all, never under another session's name.

**A session's name.** By default a neutral one: the lowest free of `s1`, `s2`, … under the machine's seat.
`RELAY_SESSION=<name>` in the session's environment gives a chosen name. `RELAY_SESSION_FROM_DIR=1` asks for the last
part of the working directory, made a name (`~/src/web-app` → `web-app`). **Every member of the room sees session
names**, connector clients included: in `status`, `list`, the door's `status`, as the author of a message (`from
desk/build`) and in the notices about messages not read. That is why the default says nothing of the machine: name a
session after a directory or a task only in a room where every member may know it. A name is 1 to 32 characters among `a-z 0-9 -`,
starting with a letter or a digit; `all` and `main` are reserved; it may not begin with `relay` or `decision`. That
pattern alone refuses control, zero-width and bidirectional characters, upper case, spaces and a second `/` — `/` is
only ever the one separator between a seat and its session. Names are unique within a seat. A neutral name or a name
after the directory is numbered by the relay when another session holds it (`s2`; `web-app`, then `web-app-2`), so two
sessions in one directory each get one. A name given with `RELAY_SESSION` is never renamed: when another session holds
it, the new session stays out of that room and says so in `status`; `relay join <room>` takes the name over (the other
session stops and says so, and does not come back). A name the relay refuses is said the same way. A session whose
link drops reconnects under the same name, taking back only its own connection (each link carries a random id); if a
sibling took the name meanwhile, a neutral or directory name is numbered anew and a given one stays out — only
`relay join` ever takes a name another session holds.

**The main session.** The one that ran `relay join <room>` last; when none did, the oldest connected one. When the
main session leaves, the next one (the oldest remaining) is main at once, and every message waiting for the machine
goes to it. With no session connected, a message to the machine waits its 7 days and goes to the next session that
connects, which is then main. So a message to the machine is never dropped by a change of main: at worst it is shown
by two sessions (one that had it on screen without confirming, and the next main). The designation is sent once; a
session that drops and reconnects brings back the time it was designated (and its age), so it is main again only if
nobody ran `relay join` after it, and after an update of the relay — when every session reconnects — the one that ran
`relay join` last is main again, whatever the order they come back in.

**Bounds.** At most 8 sessions per seat connected at once (the ninth stays out and says so); at most 50 messages
from one sender waiting for the sessions of one machine (one more is refused: the sender waits until they are read or
their 10 minutes end). The 60 messages a minute are the seat's, all its sessions together.

`status` and `list` show the sessions under each connected machine, which one is main, and which one is the session
asking; the door's `status` shows them too. Sessions come and go without a notice to the room: they are not members.

**What a session name is, and is not.** A name is a label of delivery, never an authority or a credential. Every
right stays decided by the seat's token, for every session under it: owner rights, the decider mark, writing the
shared state, the limits. A decision is the seat's (`by desk`), whichever session's shell a person typed it in. One
seat cannot claim, read or impersonate another seat's sessions: a session name lives under the seat that connected
with its own token, and the author of a message is always the sender's seat (a seat that names a session `build`
shows as `from laptop/build`, never as anyone else's). The sessions of one machine share its seat file, so they are
one trust unit: the relay keeps their deliveries apart (a message to `desk/build` is never pushed to `desk/review`),
and a session cannot take a name another one holds by starting, but anything that can read the seat file can connect
under any of the seat's names and take it over. A suffix keeps sessions from mixing up their messages; it is not a
wall between processes of one machine. The acts a session's shell refuses (« Acts that carry the machine's authority
are terminal-only ») stay refused with or without a session name.

**An older session.** A session still running a plugin from before sessions names none. Against the new Worker it
keeps working as its machine's main session when it is alone, or when it took the seat with `relay join` (it then
leaves the other sessions connected, and main). At its start it stays out, as before, while any session of its machine
is connected. It shows a session's message under the seat's name alone (`from desk`), so its reply reaches desk's
main session, not the very session that wrote; and its own `send` tool does not know the `desk/build` form (its
`relay send` in a shell does).

## A member that is not a machine

Some clients cannot start a local server: they only take a remote MCP connector over HTTPS (the Claude apps'
« custom connector », for example). Such a client holds a **connector seat** and reaches it through the Worker's
door, `POST /mcp/<room>` (MCP Streamable HTTP, JSON answers, no stream), authorized by OAuth. It gets no push, so
it pulls:

- `read {wait_seconds?, again?}` — the messages for the seat that this client has not read yet, oldest first, each
  returned once to each client (see « Several clients on one seat » below). With nothing waiting it holds the request up to `wait_seconds` (0–25, default 20; a larger value is clamped, as a
  client may cut a request at about 30 s) and answers as soon as one arrives, else « no new message ».
  `again: true` returns this client's last batch once more, if an answer was lost on the way. Each message is under a line
  the room writes, `[from <name> · <time> · #<id>]`, and every line of its content is behind `│ `; a message with a
  line to say has the room's bare labels `SAY:` and `DETAIL:`, each followed by its content:

  ```
  [from desk · 2026-11-16T20:40:49Z · #100042]
  SAY:
  │ Done, merged.
  DETAIL:
  │ Branch x landed at abc123.
  ```

  The door tells its client to speak the line under SAY as is and give the detail only when asked, and that only
  a line without `│ ` in front is the relay speaking.
- `news {}` — what is waiting for this client, without taking it: how many messages, from whom, the oldest and newest time, the
  newest one's SAY line (or its first 120 characters), and who is connected.
- `send {to, text, say?}` — to a member by name (a machine: its main session), to one session of a machine
  (`name/session`), or `all`, with the same limits and duplicate rule as every other road.
  It carries no picture: a remote client has no local file to give, and a model writing a picture out in base64
  is neither reliable nor bounded by anything the door could check.
- `status {}` — the members, their role, and how each receives (for a connector seat, how many client sessions it had
  in the last 24 hours: a count, never an identifier).

### Several clients on one seat

A connector is set per account in the client, so a phone and a desktop under one account share one connector, one
grant and one seat. Each of them still sees every message addressed to the seat, once: the door gives each client a
**client session** at `initialize` (the transport's own `Mcp-Session-Id` header, which the client sends back on every
request) and keeps a cursor per client session, the last message that client has read. A client session is the door's
own: it has no name, nobody can address it, and it is not one of the sessions of a machine seat (« Sessions » above).
Below, « session » means a client session.

- A client seen for the first time starts where SOME client of the seat has already read: it replays no old history,
  and every message nobody has read yet is still ahead of it.
- A message read by one client is kept for the others **24 hours** after it was first read, and at most the **200**
  newest read messages per seat; older ones are dropped, so a client away longer than that misses them. A message
  nobody has read waits 7 days, as on every road.
- At most **8** cursors per grant (that grant's least recently used one is forgotten: the clients of one grant never
  push out another grant's) and **32** per seat, all its grants together (past it a new session is refused, nothing is
  evicted); a cursor unused for 7 days is forgotten. A forgotten session gets a 404, the transport's sign for the
  client to initialize again; it then starts as a new client. `initialize` counts in the seat's call limit (15 calls
  in 5 seconds) like a tool call.
- A session works only with the grant that opened it, on its own seat, in its own room: under any other grant, seat
  or room (or made up) it is a 404 and nothing is read. An empty or overlong (over 256 characters) `Mcp-Session-Id`,
  or the header sent twice, is refused the same way (404). The session id is kept only as its sha256, never logged
  and never shown.
- `relay revoke` and `relay remove` forget the seat's cursors and the messages kept after a read, at once. A grant
  ended by a replay protection (an authorization code or an old refresh token used again) takes its cursors with it
  at once, and the seat's kept messages when no other live grant remains on the seat (otherwise they stay for that
  grant's clients, within the keep time and the cap).
- After a revoke, the connector seat shows as « not connected » in `status` and `list` until its next use of the
  door: the « reads by itself » mark goes with the revoke.
- Up to one waiting `read` per client: a newer one from the same client takes over and the older returns empty.
- `again` shows the messages with the names members have now (a sender renamed since shows under its new name).
- `status` shows, for every connector seat of the room (not only the caller's), how many client sessions it had in
  the last 24 hours: a count, never an identifier.
- A client that sends no session header uses one cursor per grant (two such clients would still split the messages
  between them, as before).
- Decisions are unchanged: the first valid decision wins, whichever client sends it.
- Machine seats are unchanged: a machine's message is deleted when the machine session it went to acknowledges it.

### Connect a client by OAuth

On an owner's machine:

```
relay connector ops voice
```

It creates the seat `voice` in `ops` (or reuses it), and no token for that seat exists anywhere: the seat is
entered only through OAuth. It prints the **connector URL**, `https://relay.…/mcp/ops`, which holds no secret,
and writes a one-time **enrolment code** to `~/.config/relay/remote/ops-voice.enrol` (mode 0600), printing only
the file's path. The code is valid 10 minutes and works once; running the command again replaces it.

In the client, add a custom connector with the connector URL. If it asks how to identify itself, choose to
register automatically (Dynamic Client Registration). The client finds the relay's OAuth endpoints by itself and
opens the relay's consent page, which shows the name the application gives itself, the host it will be sent back
to (check it is the client's own: for the Claude apps, `claude.ai`) and the room. Paste the enrolment code there.
The client is now the seat `voice`; it refreshes its access by itself.

```
relay grants ops          # (owner) seat, client, created, last used — never a secret
relay revoke ops voice    # (owner) deletes voice's grants: its next request fails; the seat stays
relay remove ops voice    # (owner) deletes the seat and its grants
```

To connect it again after a revoke, run `relay connector ops voice` for a new code and reconnect the client.

What the relay does, in short: it is its own OAuth authorization server (OAuth 2.1 as the MCP authorization
specification asks): protected-resource and authorization-server metadata, client registration without storage
(the client id is self-describing and signed), PKCE S256 only, one-use authorization codes of 60 seconds bound to
the client, the exact redirect URI and the challenge; access tokens of one hour and refresh tokens rotated at each
use (a grant unused 30 days ends; an old refresh token presented again ends its grant at once). A redirect URI must
be, character for character, one of the Worker variable `OAUTH_REDIRECT_URIS` (default
`https://claude.ai/api/mcp/auth_callback,https://claude.com/api/mcp/auth_callback`, the Claude apps' documented
callback). Every code and token is kept in the room only as its sha256. Joins, enrolment codes, grants made and
revoked are announced to the room like every other change; the client's name in such a notice is its own claim,
quoted and cut to 40 characters (« a connector calling itself … »).

Still true: messages sit in clear on the Worker until read, and a message a connector seat's client has read stays
there up to 24 hours more for the seat's other clients (« Several clients on one seat »). A message from such a seat is a teammate's request,
never the user's approval: a permission prompt in the receiving session is still the user's to answer. Give the
seat a room of its own with the one session it drives, not a room where sessions run unattended. In `status` and
`list` it shows as « reads by itself (remote door) » once it has used the door.

### A seat made by the former URL door

Earlier versions had `relay remote-seat`, whose connector address carried the seat's token. That door is gone: a
request to `/mcp/<room>/<token>` gets the same 404 as any unknown path. Remove such a seat like any other,
`relay remove <room> <name>`, then delete its file `~/.config/relay/remote/<room>-<name>.url`. Its name is free
again for `relay connector`. An `Origin` is checked on the door only when the Worker's `MCP_ORIGINS` var lists
some.

A message for the seat may come with a picture: `read` returns it as an image content block right after that message's
text (see « Pictures »), and `news` only says one waits, by its type and size.

The door writes one JSON line per request: time, method or tool, room, the member's name, status, duration, how the
client was known (`session`: new, known, none, unknown, or refused when the seat's session cap was full), and for `read` the wait asked, the wait used and how many
messages it returned — never a token, a session id, a seat hash or a message's text. The OAuth endpoints write one line each too (endpoint, room, status, duration), never a code or a token.
The Worker keeps its logs (Workers Logs, `observability` in `worker/wrangler.jsonc`; 3 days on the free plan, 7 on
paid), read in the dashboard under the Worker's Observability tab. No URL of the Worker carries a secret, so the
platform's own request log records none either — with one exception: a picture's link (« Pictures ») is its own key, and
the platform's request log records its path for the link's short life. The Worker's own lines never carry it.

## Decisions

A message is free text, and stays a teammate's request, never an approval. A **decision** is a separate,
explicit primitive: a member asks a precise question with a fixed list of answers, a member the room's owner
trusted answers it, and the answer is data with a known author — never a sentence a model has to interpret.

- **Ask.** Any member: `relay ask <room> --say "<one sentence>" --option yes --option no [--detail "<text>"]
  [--to <name>]... [--for <seconds>] [--wait]`. The say line is one plain sentence (it may be spoken aloud), 2 to 6
  options (labels of at most 32 characters, unique ignoring case), addressed to named members or, by default, to
  every decider, living 10 minutes by default and 24 hours at most. The room numbers it (per room, increasing) and
  announces it to every other member as a notice from `relay`: the sentence, then the number, the options, until
  when and for whom.
- **Who may decide.** Only a seat the room's owner has marked a **decider**: `relay decider <room> <name> on|off`
  (owner only). The mark is off for every seat by default, the owner's own machines included; `status` and `list`
  show it. The author never decides its own request. A request addressed to named members is decided only by those
  of them who are deciders. Mark as a decider only a seat a person holds — a remote connector seat they enrolled, or
  a machine no unattended session runs on — never a machine where unattended sessions run.
- **Decide.** `relay decide <room> <n> <option> [--note "<text>"]`, or the remote door's `decide` tool. The option
  must be one of that request's labels exactly (case aside; nothing fuzzy); a note is at most 300 characters. Labels
  are stored trimmed, spaces collapsed, in Unicode NFC, and matched the same way; the `option` a decision reports is
  that stored label. A label holding a control, zero-width or bidirectional-control character is refused, and a say
  line, a detail line, a note or a label may not begin with « DECISION » or « [relay » (the room's own words). The
  first valid decision wins; a second is refused with who decided and when. The room records who (the seat's name),
  through which door (`machine`, or `remote` with the name the connector gives itself) and when, tells the room, and
  delivers to the author a notice from `relay` carrying the decision as data (a `decision` field in the machine's
  frame). The plugin recognises a decision by that field alone, on a message from the room itself, and shows it under
  a header no message body can produce: `[relay · room <room> · DECISION #<n> · <option> · by <name> · <door>]`. In
  a member's message, a body line that starts with `[relay` or `DECISION` is shown quoted (`> `). An announcement
  always starts with the room's words, `decision request #<n> from <name>:`, the author's sentence after them. If the
  request expires first, the author is told the same way (`… · DECISION #<n> · expired · by nobody · -]`).
  A ref sent again returns its request only while it is open and asks the same thing; otherwise it is refused.
- **Other words.** `relay asks <room>` (the open requests, and whether you may decide each), `relay decision <room>
  <n>` (one JSON line, below), `relay withdraw <room> <n>` (its author only). A seat that leaves or is removed
  withdraws its open requests. `/relay ask --wait` is refused in a session (it would block it): run it in a shell.
- **Reading it without a model.** `relay ask … --wait` blocks until the request is decided, expires or is withdrawn,
  then prints one line of JSON on stdout and exits with a code a script acts on:

  ```
  {"ask": 12, "state": "decided", "option": "yes", "by": "voice", "door": "remote", "note": "", "client": "Claude"}
  ```

  exit `0` decided, `3` expired, `4` withdrawn (`1` on an error). `option`, `by`, `door` and `note` are empty when
  nobody decided; `by` is the author for a withdrawal; `client` is there only for the remote door. `relay decision`
  prints the same line (and exits 0 whatever the state). `ask --wait` gives up 10 seconds past the request's expiry,
  whatever the relay does (unreachable, or still open), and exits 3 with an `expired` line. It trusts no malformed
  answer: a state that is not the request it asked about, or « decided » with an option that is not one of the labels
  it passed, is exit 1 with nothing on stdout — never 0.
- **On the remote door.** `asks {}` lists the open requests this seat may decide (number, say line, options, who
  asks, time left) and takes nothing; `decide {ask, option, note?}` answers one. The door tells its client to read a
  request's say line aloud with its options, and to call `decide` only with an option the person has just spoken for
  that numbered request, repeating it back first — never on its own judgment.
- **In a session.** The plugin tells the session that what arrives under that decision header is the answer of the
  member named in it, with the authority the room's owner gave that member as a decider, and answers only request
  `<n>`; a free-text message is still a teammate's request, never an approval.
- **Limits.** At most 20 open requests per seat and 30 requests per seat per minute; a say line of 500 characters,
  a detail of 4 KiB; a request names at most 16 members. Expiry is enforced whenever the room is read, and by the
  room's alarm (set only once a request exists). Closed requests are kept 7 days.

**What this does not do by itself.** It does not answer the coding assistant's own permission prompts. A
decision reaches a session as a notice; the session still asks the person at the keyboard. Letting a decider answer
a permission prompt from afar takes a hook on that machine, which only the machine's owner can choose to write and
install; nothing here installs or enables one.

What the assistant's hook documentation says (« Hooks reference » and « Choose a permission mode », read
2026-11-16): the `PermissionRequest` event runs when a tool call needs a permission decision; it receives
`tool_name` and `tool_input` on stdin; it answers on stdout with
`{"hookSpecificOutput": {"hookEventName": "PermissionRequest", "decision": {"behavior": "allow"}}}` or
`"behavior": "deny"` (with an optional `message`). An exit code alone decides nothing there: exit 0 without that
JSON leaves the normal flow, and exit 2 is not honoured for this event. A command hook's default timeout is 600
seconds, set per hook with `"timeout"` in seconds. Under `bypassPermissions` there are no permission prompts, so
this hook is never asked and nothing waits for a decider.

An example the machine's owner could adapt (Python 3, standard library; `relay` on the hook's `PATH`). It denies
whenever it is not sure: an error of any kind, a `relay` it cannot run, no answer within the lifetime plus 20
seconds, an answer it cannot read, or a tool input too long to show the deciders whole (never a silent cut):

```python
#!/usr/bin/env python3
# A PermissionRequest hook: asks the deciders of one room; anything but a decided « allow » denies.
import json, os, subprocess, sys

ROOM = "ops"
LIFETIME = int(os.environ.get("RELAY_HOOK_SECONDS", "120"))  # the hook's timeout: at least LIFETIME + 30
DETAIL_MAX = 4096  # bytes a request's detail may hold


def answer(behavior, message=""):
    decision = {"behavior": behavior, **({"message": message} if message else {})}
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PermissionRequest", "decision": decision}}))
    sys.exit(0)


try:
    req = json.load(sys.stdin)
    tool = json.dumps(str(req.get("tool_name", "?")))[1:-1]  # ASCII: every control or invisible character escaped
    action = json.dumps(req.get("tool_input", {}))
    detail = "tool: %s\ninput: %s" % (tool, action)
    if len(detail.encode()) > DETAIL_MAX:
        answer("deny", "the tool input is too long to show whole to the deciders of %s" % ROOM)
    say = "Allow %s on desk: %s?" % (tool, action)  # a voice door speaks this line first: it names the action
    if len(action) > 80:  # never a silent cut: the decider is told how much more there is
        say = "Allow %s on desk: %s … (%d more characters — hear the detail before deciding)" % (
            tool, action[:80], len(action) - 80)
    ran = subprocess.run(["relay", "ask", ROOM, "--say", say, "--detail", detail, "--option", "allow", "--option", "deny",
                          "--for", str(LIFETIME), "--wait"], capture_output=True, text=True, timeout=LIFETIME + 20)
    if ran.returncode == 0 and json.loads(ran.stdout).get("option") == "allow":
        answer("allow")
    answer("deny", "not allowed through the relay room %s" % ROOM)
except SystemExit:
    raise
except BaseException:  # no relay program, a timeout, an unreadable answer, anything else: no
    answer("deny", "the relay room %s could not be asked, so the call is denied" % ROOM)
```

Through a voice door the decider hears the say line first, and may hear nothing else: the say line of such a hook
must itself name the action — the tool and the first 80 characters of its input, as above. When the input is longer,
the say line ends « … (N more characters — hear the detail before deciding) », and the door tells its client to read
the detail aloud before asking for a decision. The whole detail is
visible to every member of the room, not only to the deciders: do not use such a hook where a tool's input may
carry something the room should not read.

registered in that machine's settings with a timeout of the lifetime plus 30 seconds:

```json
{"hooks": {"PermissionRequest": [{"matcher": "Bash",
  "hooks": [{"type": "command", "command": "/path/to/that/script", "timeout": 150}]}]}}
```

What it grants: every decider of that room can allow, one call at a time, a call the assistant would otherwise
have asked about (here, shell commands), seeing the tool and its input in the request's detail. What it does not:
it adds no permission rule, it is not asked for calls the machine's rules already allow, and it is not asked at all
in a mode where permissions are bypassed. While it waits (up to the lifetime) that call waits; no answer is a denial.
Install it only where you accept that the deciders of that room approve actions on that machine.

## Shared state

Each room has a small **key → value table** of its own, for coordination between machines with **no model in the
loop**: one machine's script writes « the value everyone should follow », and every machine's own script reads it
with that machine's seat. The room knows nothing of what a key means.

**Never put a secret, a token or a key in the state.** Values are stored in clear on the Worker and announced to
every member of the room. A cheap guard refuses a value that looks like a credential, and there is no override:
a PEM header (`-----BEGIN …-----`); `sk-`, `sk_`, `ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_`, `github_pat_` or `xox?-`
at the start of a word followed by 20 or more token characters; or a run of 40 or more base64url characters
(`A-Z a-z 0-9 _ -`, no space) that mixes upper case, lower case and digits — the shape of a random token. A git
commit id, a lowercase slug or ordinary JSON passes. The guard is not a scanner; the rule is the sentence above.

```
relay set ops deploy.target blue                 # prints {"key": …, "value": "blue", "version": 1, "by": "desk", "at": …}
v=$(relay get ops deploy.target) || echo "not set"   # the value alone; exit 3 and nothing on stdout if absent
relay get ops deploy.target --json               # {"key": "deploy.target", "value": "blue", "version": 1, "by": "desk", "at": "…"}
relay set ops deploy.target green --if-version 1 # exit 5 on a conflict, the CURRENT line on stdout
relay state ops                                  # deploy.target · v2 · by desk · 12 s ago · owners may write · green
relay del ops deploy.target [--if-version n]
relay state-open ops deploy.target members       # (owner) any member may now write this key; `owners` closes it
```

- **Belongs to one room** and never leaves it: no cross-room read, no export.
- **Every change is announced** to every member as a notice from `relay` of one line that starts with the room's own
  words: `state: deploy.target set to version 2 by desk: "green"` (also `… deleted at version 3 by desk`, `… opened
  to members by desk: …`). The value comes after those words as a JSON string (quotes, backslashes and `\n`
  escaped); a value over 120 characters is given by its size only. Like every body line it reaches a session behind
  `│ `, so a value that begins with `DECISION` or `[relay` is stored as is and still never reads as a header or a
  decision. A machine's frame carries the change as data (`state`: op, key, version, by, at, writers, size). **A read
  consumes nothing and announces nothing.** A session's `get` and `state` tools return values only as tool output,
  never into the session's inbox: `get` gives one JSON line, `state` one line per key with the value JSON-escaped and
  cut to 60 characters.
- **Announcing costs like a message to all**: one unit of the writer's budget (60 a minute, shared with messages)
  per other member. When the budget cannot pay, the change is refused with 429 before it is applied — a change that
  cannot be announced does not happen.
- **Who writes.** By default only an owner's seat. An owner opens one key to every member with `relay state-open
  <room> <key> members` (per key; `owners` closes it again; a key can be opened before its first write). A
  connector seat (the remote door) never writes, whatever the key's setting: it has only the read tools. `relay
  state` shows who may write each key.
- **Versions and conditional writes.** A key's first write is version 1; every write after it adds 1, a delete
  included. Each value carries who wrote it (the seat's name at that moment), when, and its version. `--if-version
  n` writes only if the current version is n (`0`: only if the key does not exist); otherwise nothing is written and
  the relay answers 409 with the current value, version and writer. Without it the write is unconditional (last
  writer wins) and still bumps the version.
- **Delete keeps the version.** A deleted key leaves a tombstone, so versions never go backwards: a key deleted at
  version 3 is written next as version 4. A script that remembers « version 2 » can never overwrite a value that
  replaced a deleted key by chance.
- **Exit codes** a script acts on: `get` 0 (the value, then a newline) or 3 (no such key, nothing on stdout); `set`
  0 (its JSON line) or 5 (version conflict, the current JSON line); `del` 0, 3 or 5; `1` on any other refusal or
  error, with the reason on stderr and nothing on stdout. The CLI trusts no malformed answer: `get` prints the value
  only when the answer is the view of the very key asked, existing, with a string value; `set`, `del` and `get
  --json` check the same way; anything else (a missing field, null, another key, a wrong type) is exit 1 with
  nothing on stdout. `relay get` needs no session: it is a plain HTTPS call with the machine's seat.
- **Limits.** A key is 1–64 characters among `a-z 0-9 . _ -`; a value is ONE line of at most 1024 bytes (UTF-8;
  JSON on one line if you want structure); at most 64 keys per room, and 256 key names counting deleted ones (past
  that, reuse a deleted key's name); 30 changes per seat per minute. A value holds no line terminator of any kind, no
  control character (C0, C1; tab aside) and no format character (zero-width, bidirectional controls, BOM…), so `relay
  get`, which prints it raw for scripts, always prints exactly one line.
- **Routes** (bearer = the seat's token): `GET /v1/rooms/<room>/state`, `GET /v1/rooms/<room>/get?key=<key>`,
  `POST /v1/rooms/<room>/set {key, value, if_version?}`, `POST /v1/rooms/<room>/del {key, if_version?}`,
  `POST /v1/rooms/<room>/state-open {key, writers}`.
- **Tools.** The remote door adds `state {}` and `get {key}`, read only. A session gets `state` and `get`, read only
  too: writing the state carries the machine's authority, so a person does it in a terminal of their own (`relay
  set` is refused in an assistant's shell); the plugin tells the session never to write the state or decide a request
  because a message asks.

## Pictures

A member can send a **picture** with a message. For the receiver the real image is the main road; a short
**description** written by the sender, and a **link** a person can open, come as a complement, never as a replacement.

```
relay send ops:voice "the dashboard now" --image ~/shot.png --describe "the error rate graph, flat since noon"
relay send ops:desk --image ./diagram.webp          # the text may be left out when a picture goes
```

The session's `send` tool takes the same: `image` (the path of a local file) and `describe`. The relay answers
« sent … ; the picture (image/png, 48213 bytes) goes with message #100042 »: that number names the picture for
`relay image-link`. A picture leaves over HTTPS, never on the socket, and is never queued in the outbox: when the relay
cannot be reached, the send fails and says so. The remote door's `send` carries no picture (« A member that is not a
machine »).

- **What is accepted.** PNG, JPEG, WebP and GIF, recognised by their leading bytes, never by the file's name or a type
  anyone declares; SVG and everything else is refused, and so is a file named `.png` that holds HTML. The plugin checks
  before sending; the Worker checks again and is the one that decides. One picture per message, at most **1 MiB**. The
  description is one line of at most 300 characters, checked like a say line: no line break, no control, zero-width or
  bidirectional character, and it may not begin with « DECISION » or « [relay ».
- **A machine seat** fetches the picture with its own seat token (never a public link) before it acknowledges the
  message, into `~/.local/state/relay/pictures/` (the folder 0700, each file 0600), under a name the plugin makes —
  `<room>-<message>-<random>.<ext>`, the extension of the sniffed type, nothing the sender wrote. The folder keeps at
  most 64 files and 64 MiB; the oldest are removed first. The session reads:

  ```
  [relay · room ops · from laptop · #100042 · 2026-11-17T09:12:03Z · picture] (reply with the relay send tool, to "ops:laptop")
  │ the dashboard now
  │ picture (image/png, 48213 bytes), saved at: /home/me/.local/state/relay/pictures/ops-100042-3f9c01aa.png
  │ its description, by the sender: the error rate graph, flat since noon
  ```

  « · picture » in the header is the relay's word that a picture came; the lines under it are behind `│ ` like all
  content, so the session opens the file itself. A picture that cannot be fetched is said so on that line, and the
  message is still delivered.
- **Sessions of a machine.** A picture follows its message: one sent to `<seat>/<session>` is fetched by that session,
  one sent to the machine's name by its main session. Any session of the recipient's machine may fetch it (the seat is
  the unit of trust: every session of a machine holds the same seat token). When a message for a session expires unread after its bounded
  wait, its picture is deleted with it, and the « not read » notice to the sender says a picture was attached. The
  machine's sessions share the private folder; writing a picture and evicting the oldest are one step under a file
  lock, so two sessions never evict at once.
- **A connector seat** (the remote door): `read` returns the picture as a real image content block of the tool's answer,
  right after its message's text — the room's line `PICTURE: image/png, 48213 bytes (the image follows)`, then
  `DESCRIPTION:` with the sender's words behind `│ `. Each client session gets it once, like any message. One `read`
  returns at most 2 pictures; the messages after them wait for the next read, and the answer says so. `news` only says
  how many pictures wait, with their type and size.
- **The setting of a seat**, by an owner: `relay images <room> <seat> inline|link|off`. `inline` (the default) is the
  above. `link`: the seat gets the description and a **delivery link** instead of the image — for a client that cannot
  show an image. The relay makes it when it delivers the message — on the door, at each client session's read, one per
  client session and message (each client reads every message); on a machine, once per message — it always lives the
  default 15 minutes, and it counts in no link cap. `off`: the description only; nothing is stored for that seat, and the sender is told
  « the picture was NOT delivered to … ». A message is delivered as the seat was set when it was sent. `status` and
  `list` show a setting other than the default (`pictures: link`); every change is announced to the room.
- **A link**, for a person to open in a browser: `relay image-link <room> <message> [--minutes n] [--once]`, by the
  picture's sender (with the number its send answered) or by a seat it was sent to (with that message's number), never
  anyone else. Only on request, never on its own (a seat set to `link` is its owner's request). It prints
  `https://relay.…/i/<room>/<token>`: a random 32-byte token, the link being the key, kept by the relay only as its
  sha256 and bound to that one picture. It lives 15 minutes by default (1 to 60), `--once` for one view. It is served
  with the exact sniffed type, `X-Content-Type-Options: nosniff`, `Content-Disposition: inline` with a neutral file name
  (`picture.png`), `Cache-Control: no-store`, `Content-Security-Policy: default-src 'none'; sandbox`, no cookie; that
  path serves nothing else and takes no data: an opening only deletes what has expired and counts a one-view link's view
  (two openings at once of a one-view link: one is served). An expired, used-up or unknown link gets exactly the answer
  of any unknown path, so it says nothing about whether the picture existed. Each seat has at most 8 live links to one
  picture and makes at most 20 links a minute; one seat's links never cost another seat its own.

  **Give a link only to a person, by a channel you trust.** A link pasted into a chat application is FETCHED by that
  application's preview: that uses up a one-view link, and leaves a copy of the picture in the application's cache. And
  the hosting platform's request log records a link's address, its token included, readable by whoever administers the
  hosting account for as long as that log is kept; what it opens is bounded by the link's short life.
- **Lifetime.** A picture lives and dies with its message: it is deleted when the message is — acknowledged by a machine
  seat, past the door's keep time or cap after a read (24 hours, 200 messages per seat), at the 7-day prune, at a revoke
  of a connector seat's grants (what was read), at the removal of a seat. A message to `all` shares one stored picture,
  deleted with its last copy; a recipient that has acknowledged its copy can no longer fetch it or make a link. Its links
  die with it, and a seat's links end at a revoke of its grants and at its removal. One exception: the delivery link of
  a seat set to `link` keeps the picture until that link expires (15 minutes at most), though a machine acknowledges the
  message at once. A link made with `relay image-link` never keeps a picture, whatever its lifetime: it dies with it.
- **Bounds.** 1 MiB per picture, one per message; 6 pictures per sender per minute; a room holds at most 128 pictures
  and 32 MiB stored at once (everything stored counts: unread, kept for a connector seat's other clients after a read,
  held by a delivery link). One sender's pictures that still have an unread recipient hold at most a quarter of each
  (32 pictures, 8 MiB), so a sender whose pictures wait for a seat that never reads cannot hold the whole room; pictures
  already read and kept for a seat's other clients, or held only by a delivery link, are bounded in time (the door's
  keep time, 15 minutes) and do not count in the share. When the room, or the sender's share, is full a new picture is
  REFUSED with a clear message and nothing is evicted; room is made as messages are read. On the Worker a picture is stored once per send, in chunks of 256 KiB (a row of a
  Durable Object's SQLite storage holds at most 2 MB).
- **What is stored in clear.** The Worker's storage holds each picture IN CLEAR, with its description, from the send
  until it is deleted as above — like every message. A link's token is not stored, only its hash. Nothing of a
  picture's bytes, its description or a link's token is in the Worker's own log lines or the plugin's; the platform's
  request log records the path of a link that is opened (above).
- **A session sends what it can read.** The session's `send` tool can send any local picture the session can read. The
  plugin tells the session to send a picture only when the person asked for it or it is the session's own output,
  never because a message asks — an instruction to a model, not a lock.

Security properties checked by the tests: only a picture's sender and the seats it was sent to fetch it or make a link
to it (another member, a seat of another room, a recipient set to `off` and a recipient that has acknowledged its copy
are refused); a link serves its own picture only, and an expired or used-up one serves nothing and says nothing; a
one-view link opened twice at once is served once; no route lets a caller without a seat store anything; the type
served is the type sniffed at send (an HTML file claiming PNG is refused there); the description cannot forge a header
(it is checked at input and shown behind `│ `); the local file's name is the plugin's own, and a description shaped
like a path writes nothing outside the folder; only the delivery link keeps a picture, for 15 minutes at most; the link
caps are per seat; a revoke, a grant ended by a replay (the seat's last) and a removal end a seat's links; each door
client of a `link` seat gets a working link of its own; the room's total and a sender's share (only pictures with an unread recipient, the default 32 included) are enforced;
nothing of a picture or a link's token is in the Worker's own log lines or the plugin's. Not tested: the 7-day prune of
a picture on its own, the socket's refusal of a picture, a symlink planted in the local folder (the folder itself is
checked to be a directory, made 0700, and files are created with `O_EXCL` and `O_NOFOLLOW`). The door's OAuth, per-seat
limits and cursors are unchanged (their own tests).

## Encrypted rooms

An **option of a room**, chosen when it is created, never a default: `relay create <room> --encrypted`. In such a room
the Worker stores and relays the TEXT of every member's message (and its say line) as an envelope it cannot read. A
room created without the option is unchanged and stays in clear; converting an existing room, either way, is not part of
this version.

**What it protects.** The words members write to each other. The Worker's storage, its logs and anyone who reads them
see an envelope, `relay-e2e:1:<generation>:<sent at>:<ref>:<base64url>`, never the text.

**What it does not protect — read this first.**

- **There is no per-member authentication.** The key is the room's, shared by every member. Any holder of it TOGETHER
  WITH the Worker can write a message that appears to come from any member: the Worker names the sender, and the
  envelope only checks that the name matches what the sealer wrote. That includes a removed member until the room is
  rekeyed; after a rekey, for up to 7 days after each machine learns of it, with a backdated message shown as sealed
  with the previous key (« Removal and rekey »).
  Without the Worker's help, a key holder cannot pass as another member.
- **The room's own notices** (« member … joined », « … moved the room's key to generation n », « message #… was not
  read ») are clear text the Worker writes. They are not authenticated: a dishonest Worker can write any of them.
- **Reordering and dropping are not detected.** The Worker can hold a message back, deliver messages in another order,
  or not deliver one; the receiver cannot tell. It can only refuse what was moved, altered or replayed.

**What the Worker still sees**, exactly: who sends to whom (the seat names, the session names, `all`), when, the size of
each envelope, the message refs, the key generation number and a check of each generation's key (a SHA-256 under its
own label, from which the 256-bit key cannot be found), the room's own notices (joined, left, renamed, invitation
minted, key generation changed, a message nobody read — that notice no longer quotes its beginning), the members' names,
roles and addresses (host name and IP of each connection), and who is connected. Everything but the text.

**What is not available in an encrypted room**, each refused with its own sentence, never sent in clear: the hosted
remote door and connector seats (a member that would read on the Worker cannot exist where the Worker cannot read),
decisions (`ask`, `decide` and the rest), the shared state, and pictures. A picture is refused at every entry with
« pictures are not available in an encrypted room in this version »: `relay send --image` and the session's `send`
tool before the picture's bytes leave the machine (a machine without the room's key asks the relay first, with a request
that carries no picture), the relay's send, `image-link` and the `images` setting (in the CLI and on the relay), the
fetch of a picture, and a `/i/<room>/…` link, which answers as an unknown one; no picture row can exist for such a room,
and a machine never fetches one there. The Worker keeps an allow-list
for such a room (create, invite, accept, hello, send, rename, remove, leave, connect, rekey): any other route, one
added later included, is refused there until it is made for it.

**Where it ends.** End to end means machine to machine: the plugin decrypts before rendering, so the session on the
receiving machine reads the text as in a clear room (same header, `│ ` lines, sessions as suffixes, the bounded wait,
the receipts), and so does whatever that session sends its own context to.

### The key and the invitation

- **One secret key per room**: 32 random bytes from the system's source, made on the creating machine and kept in
  `~/.config/relay/keys/<room>.json` (mode 0600, folder 0700). It is never sent to the Worker — in no request, header,
  socket frame, log line or error (the tests record every byte the plugin sends during create, invite, accept, join,
  send, rekey and take-key and search it for the key in every spelling).
- **It travels with the invitation**, in the part after `#`: `relay invite ops` prints
  `https://relay.…/j/ops/<secret>#k1.<key>`. The program sends only the part before `#` (the join request carries the
  seat's hash and its name); a browser never sends a fragment either. The whole string is a one-time secret passed
  person to person, as before. A person who has only the server-side part gets a seat and no key: that machine can
  neither read nor send in the room, and is told so, until a member who holds the key passes it (`relay key ops` on
  theirs, `relay take-key ops#k1.<key>` on the new one).
- **On standard input** rather than the command line: `relay accept -` reads the link, and `relay take-key ops -` the
  key (`ops#k1.<key>`, or `k1.<key>` alone), as one line. Given as an argument, the key is visible in the process list
  to every user of the machine while the command runs, and is kept in the shell's history; on a machine others use,
  paste it on standard input.
- **Terminal acts**: `create --encrypted`, `invite` of an encrypted room, `accept` of a link that carries a key, `key`,
  `take-key` and `rekey` are refused in a shell started by an assistant session — they show or take the key, and a
  session's transcript is not a place for it. Like the other terminal-only acts, this is a guard on the environment
  variables an assistant session sets, not a boundary: a process that clears them, or reads the key file, is not
  stopped by it.
- **`status`** says a room is encrypted, its key generation, and whether THIS machine holds that generation, with a
  fingerprint — 32 bits (8 hex digits) of a SHA-256 over the room, the generation and the key — for two people to
  compare their machines by eye. It does not help guess the key (256 bits). Never the key.
- **Which way the plugin goes** is decided by its own key file, in one rule in the code (`sealing_ring`): a machine
  with a key file of the room seals every message there, whatever the Worker says, and refuses to send when that file
  cannot be read. Without a key file, a Worker that says the room is encrypted makes it refuse too (`relay send` asks
  the greeting first). Once a key file exists, a message queued in clear before it is sealed when it leaves, never sent
  in clear. `image-link`, `images` and every other road use the same test: a key file exists. The Worker's word can
  add a refusal; it can never make the machine send in clear what it would seal. An invitation that carries a key
  means the room is encrypted: the key is kept even when the Worker answers « not encrypted », and `accept` then stops
  with an error.

### What the Worker refuses

From creation, the room records only the flag `encrypted` and the key generation (1). It refuses any message there
that is not the envelope (an older or misconfigured plugin sending clear text gets « this room is end-to-end encrypted
… this one came in clear: update the plugin on this machine and hold the room's key »), a say line beside the envelope,
an envelope of another generation, an envelope whose ref is not the message's, and an envelope in a room that is not
encrypted (a clear room's text may not begin with `relay-e2e:`).

### Removal and rekey

- **A removed member still holds the key it had**, and whatever it copied of what it read. Removing it stops the Worker
  from delivering to it; it does not unteach it the key. Leaving or being removed deletes the key file on that machine, and the keys set aside for that room,
  as tidiness, not protection.
- **`relay rekey <room>`** (an owner, in a terminal): a new key generation, made on that machine; the Worker is told only
  its number (it accepts exactly the next one, from an owner) and announces it. The command prints
  `relay take-key <room>#k<n>.<key>` to pass to each remaining member, person to person, as an invitation is. From then
  on the Worker refuses an envelope of the old generation, with a sentence telling the sender it needs the new key and
  how it is passed; a member still on the old one is shown « could not be read: it uses key generation n, which this
  machine does not hold ». Messages already delivered are not encrypted again; older generations stay in the key file,
  to read what was sealed under them. No automatic key agreement in this version.
- **After a rekey, an old generation is still read for 7 days — and its holder can backdate.** Each machine records,
  on its own clock, when it learned that a generation was replaced (it ran the rekey or took the newer key). From then on
  an envelope of the older generation is read only if its sending time is before that moment (10 minutes of clock
  difference allowed) and it arrives within 7 days of it: the messages that were in flight. But the sending time is
  written by whoever seals. Someone who holds the old key — a member removed before the rekey — TOGETHER WITH the
  Worker can date a new message before the rekey and have it read, up to 7 days after each machine learns of the rekey;
  the window starts only when that machine learns, so a machine that stays away longer accepts it longer. Every message
  read under a previous generation is shown with a line of its own before its text, behind `│ `: « sealed with the
  previous key (generation n), before this machine learned of the rekey » — its assurance is lower.
- **What the relay says of the generation is only a tentative mark.** A greeting announcing a newer generation that
  this machine holds no key of does not replace its current key: messages of the current key are still read, with a
  line before their text, « the relay says the key was changed; this machine does not hold the new one », until a key of
  the newer generation is actually held. A lying relay can cause that line; nothing more.
- **These warning lines are the receiving machine's own.** « sealed with the previous key … » and « the relay says the
  key was changed … » are written by the machine that reads the message and always come FIRST, before the sender's
  text. A sender can imitate their words inside its text, but cannot remove them.
- **Two owners rekeying at once never keep different keys for one generation.** The rekey request carries a check of
  the new key — a SHA-256 of it under its own label, « relay room key check », never the key — which the Worker keeps
  with the generation and returns in its answer, its refusal and its greeting. A machine adopts a key it made only when
  the room's check is that key's; otherwise another owner's rekey landed, and the machine says « another owner changed
  the room's key: take it from them ». The key made here is never deleted: it is SET ASIDE in the keys folder (0600,
  named with its generation and time), and `relay key <room> --dropped` (a terminal act, like `key`) prints a
  `take-key` line for each key set aside or still pending — in case it was the room's key after all. A relay that gives
  no check (a room created before checks existed) never causes a key to be dropped: it stays pending, and the owner is
  told how to adopt it.
- **An owner can always move on.** When this machine does not hold the room's current generation (another owner's, or
  one whose key nobody kept), `relay rekey` goes to the next generation anyway and says so: the members on the lost
  generation need the new key like everyone else.
- **A lost answer costs nothing.** A key made by `rekey` stays pending until the Worker confirms its generation (its
  answer, the next greeting, or a refusal naming it); if the answer is lost after the Worker moved, the next command —
  `status`, a send, a session's link — confirms it, and nothing is lost. A refused rekey keeps nothing. `rekey` reads the
  room's generation first and never makes a key over one it has not reconciled. `create --encrypted` whose answer is
  lost keeps the seat and the key; if the room was not made, the next command's « not a member » forgets both.

### The cipher, and its fallback

- **ChaCha20-Poly1305 as in RFC 8439**, with a random 96-bit nonce per message. Not XChaCha20: with one key per room
  and a few messages a minute, random 96-bit nonces stay far below the point where a repeat is a concern (2^32
  messages per key), and RFC 8439 is the construction both implementations below provide.
- **The implementation**: the `cryptography` package when it is importable on the machine; otherwise a small
  pure-Python implementation of the same standard inside the plugin (the plugin keeps no third-party dependency). The
  tests prove the fallback with the RFC's own vectors (the ChaCha20 block function, encryption, the Poly1305 MAC and
  its key generation, the AEAD construction, Appendix A.3's eleven Poly1305 vectors with their edge cases, and
  Appendix A.5's decryption), against node's `chacha20-poly1305` on random inputs, and against the
  package both ways when it is present; one test machine always runs the fallback while the others use the package.
  `status` says which one runs. `RELAY_CRYPTO=pure` forces the fallback.
- **The fallback is not constant-time.** Its timing depends on the data and the key. It runs only on the key holder's
  own machine, on messages that machine sends or receives, so an observer would have to measure that machine's own
  computation closely; still, where that matters, install the package.
- **What a message is bound to.** The associated data is the room, the key generation, the sending time, the sender as
  the Worker names it (`<seat>` or `<seat>/<session>`), the recipient as addressed (`<name>`, `<name>/<session>` or
  `all`) and the sender's ref. A ciphertext moved to another room, recipient, sender, session or ref fails
  authentication, and so does a truncated or altered one; the receiving plugin also checks that a message's recipient
  is itself. The Worker names the sender from the seat's token, so to pass as another member one needs both the key
  and the Worker.
- **A replay** — the same envelope delivered again under a new number — is recognised by the receiver: the plugin
  already de-duplicates by message number and time, which a new number escapes; for an encrypted room it also records
  the sender, ref and sending time of each envelope it shows, checked and written at once under the machine's lock for
  the room (two sessions of one machine never both show one envelope). A record is kept until its envelope's own sending
  time is 8 days old, and an envelope dated more than 8 days ago (or more than 10 minutes ahead) is refused anyway; past
  20 000 records the oldest go, and every envelope dated at or before the newest one that went is refused (« cannot be
  told from a replay »). A replay is acked and not shown. The record names the session that claimed it (its link, its
  process and when that process started): while that session is alive, another session of the machine takes the same
  number for a replay; if it died before the hand-over, the next one finishes the delivery once. A crash between the
  hand-over and the record's « handed over » mark shows the message ONE more time at the resend — the only case, as in
  a clear room (at worst shown twice on a crash).
- **A machine whose clock is wrong** by more than the allowed difference (10 minutes) sees messages « out of the
  accepted window »: every message, when its clock is far enough off. On the sender's side, a machine whose clock is
  fast seals messages that every other machine refuses, and nobody tells it: `status` and the first send of a session
  compare this machine's clock with the relay's `Date` header and warn when they differ by more than the allowed skew
  (the relay's clock is trusted for nothing else). Keep the machines' clocks set by the network.
- **A message that cannot be read is never shown as text**: the session is told, behind `│ `, « a message from
  <seat> could not be read: <why>. Nothing of its content is shown. » The receiver tells the sender nothing.
- **The outbox**: a message queued while the relay is unreachable waits on the key holder's disk (0600) sealed under
  the room's key, and is sealed again for the wire once the relay has said this seat's and session's names.

### Limits of this version

- No forward secrecy: whoever obtains a generation's key reads every message sealed under it that they hold a copy of.
- One shared key per room, no per-member keys: any key holder can read every message of the room it receives, and with
  the Worker's help write as any member (« What it does not protect »).
- A rename of the sender or the recipient while a message waits makes that message unreadable (« could not be read »):
  the names are bound into it.
- The 10-second duplicate rule cannot see through the envelope (every envelope differs): an identical text sent twice
  is stored twice.
- A message's text is about 36 KiB at most (its envelope must fit the 48 KiB limit).
- Converting a room, decisions, the shared state, pictures and the hosted door are not available, as said above.

## Limits

- The inbox socket and its line format are not documented by the assistant's vendor and may change. Only a
  child process of the session is heard there, which is why the session's own server writes into it.
- The preview channel remains as the fallback: it shows messages only in sessions launched with its option,
  which asks for a confirmation at each start (see « If the inbox stops working »).
- One seat per machine per room; its sessions are names under it, not separate members, and they share the seat's
  rights and budget (see « Sessions »).
- In a room that is not encrypted, the Worker's storage holds the messages, and their pictures, in clear until they
  are delivered (for a connector seat, up to 24 hours after a first read, for its other clients); end-to-end encryption
  is an option of a room chosen at its creation, with its own limits (« Encrypted rooms »), and holds no picture.
- Pictures only (PNG, JPEG, WebP, GIF; one per message, 1 MiB), no other files. A client's own display decides whether a
  picture returned by a tool reaches its model (« Pictures » and TODO.md).

## Tests

```
python3 tests/test.py    # starts wrangler dev --local on 127.0.0.1:28787 and stops it at the end
```

Runs are kept under `tests/.run/`; `RELAY_TEST_RUN_DIR=<a short path>` puts them elsewhere. `RELAY_TEST_ONLY=encrypted` runs the
checks of encrypted rooms alone; run the suite once more with the `cryptography` package importable to check the two
ciphers against each other.

## License

MIT, see LICENSE.
