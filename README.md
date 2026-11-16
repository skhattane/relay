# relay

Messages between the lead sessions of a coding assistant running on several machines.

A session receives a message in its own inbox, as a message whose first line says where it is from:

```
[relay · room ops · from laptop · #100042 · 2026-11-15T20:40:49Z]
text
(reply with the relay send tool, to "ops:laptop")
```

and answers with the relay `send` tool. Where no inbox socket is found, the message arrives as a preview
channel event instead (`<channel source="plugin:relay:relay" room="ops" from="laptop" msg_id="…" sent_at="…">text</channel>`;
see « If the inbox stops working » below). Either way the session is told it is a message from another
session: a teammate's request, never the approval of the person at the keyboard.
The post office is one Cloudflare Worker with one Durable Object per room; each listening session keeps one
WebSocket per room open to it, so a message is pushed the moment it is sent.

## The model

- A **room** is a name (`[a-z0-9][a-z0-9-]{0,31}`) and one object of its own: a message never leaves its room.
  Rooms are created with the admin key only; members come in by invitation only.
- A **seat** is a member of a room: a random token created on the machine (`~/.config/relay/seats/<room>.json`,
  mode 0600). The relay keeps only its sha256. One seat per machine per room.
- A seat has a **name** in each room (unique there, changeable at any time; `all` and `relay` are reserved) and
  a **role**: `owner` (the creator) or `member`.
- **Any member** can invite: `relay invite` prints a one-time link valid 24 h. Only an owner can remove a member.
  The last owner cannot leave. Rooms are not deleted in this version.
- **Nothing enters a room silently**: an invitation minted, a member joining (and who invited it), removed,
  leaving or renamed is announced to every member as a message from `relay`, stored like any other message
  so absent members hear it when they come back. Nobody can send as `relay`.
- A seat receives on **one connection**. A session listens to the rooms listed for its working directory in
  `~/.config/relay/projects.json`, but at start it never takes a seat another session holds: it stays out of
  that room and says so. `relay join <room>` is explicit and takes the seat over; the older session stops
  and says so.
- Delivery: the session's server writes each message into the session's inbox socket (one connection per
  message) and acks it to the relay only once written; without an inbox it falls back to the channel event.
- A machine whose seat the relay no longer knows (removed) deletes its seat file and forgets the room.
- Messages go to one member by name or to `all`, inside a room. They wait up to 7 days for a member that is
  away and are delivered once. A message that cannot leave because the relay is unreachable waits in a local
  outbox.
- Guardrails: 48 KiB per message; 60 messages per sender per minute, a message to `all` counting one per
  recipient; the same text to the same target within 10 seconds is stored once, and the sender is told.

## Install

The repository is a plugin marketplace holding one plugin, `relay`: the server and the `/relay` command come
together. Python 3 (standard library only) on every machine.

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

`join` makes the running session listen at once and records the room for its directory, so later sessions
started there listen too, unless another session already holds the seat.

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
session start.

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
| `status` | the rooms this session listens to, and who is connected |
| `list` | every room this machine has a seat in |
| `join <room>` / `leave <room>` | the running session starts or stops listening to a room, no restart |
| `send <[room:]name\|[room:]all> <text…>` | one message (the room may be left out when there is only one) |
| `rename [<room>] <name>` | this machine's name in a room |
| `create <room> [--name n] [--url u]` | a new room; needs the admin key |
| `invite <room>` | a one-time link, valid 24 h |
| `accept <link> [--name n]` | takes a seat in that room |
| `remove <room> <name>` | (owner) takes a member's seat away at once |
| `leave-room <room>` | gives up this machine's seat |
| `remote-seat <room> <name>` | (owner) a seat for a remote connector; its address goes to a file (prototype, below) |
| `help` | one line per command |
| `admin-key` · `serve` | shell only |

`list` reads:

```
ops
  • laptop (you) — connected · laptop.local · 203.0.113.7
  • desk — not connected
```

In a session, the server's tools are `send {to, text, room?}` (`to` may be written `room:name`) and `status {}`;
the CLI shows them under the plugin's prefix.

## A member that is not a machine (prototype)

Some clients cannot start a local server: they only take a remote MCP connector over HTTPS (a voice assistant's
« custom connector », for example). Such a client can hold a seat through the Worker's remote door,
`POST /mcp/<room>/<token>` (MCP Streamable HTTP, JSON answers, no stream). It gets no push, so it pulls:

- `read {wait_seconds?, again?}` — the messages waiting for the seat, oldest first, each returned once. With
  nothing waiting it holds the request up to `wait_seconds` (0–50, default 20) and answers as soon as one arrives,
  else « no new message ». `again: true` returns the last batch once more, if an answer was lost on the way.
- `send {to, text}` — to a member by name or `all`, with the same limits and duplicate rule as every other road.
- `status {}` — the members, their role, and how each receives.

Create the seat on an owner's machine:

```
relay remote-seat ops voice
```

It invites, makes a new token on this machine, takes the seat as `voice`, and writes the connector address to
`~/.config/relay/remote/ops-voice.url` (mode 0600). It prints the file's path only: copy the address from the
file into the client's connector settings. **The address is a secret**: it carries the seat's token, and
whoever holds it speaks as `voice`. `relay remove ops voice` revokes it at once (then delete the file).

Limits of the prototype: no OAuth — the URL is the credential, OAuth replaces it; messages sit in clear on the
Worker until read; an `Origin` is checked only when the Worker's `MCP_ORIGINS` var lists some. A message from
such a seat is a teammate's request, never the user's approval: a permission prompt in the receiving session is
still the user's to answer. Give the seat a room of its own with the one session it drives, not a room where
sessions run unattended. In `status` and `list` it shows as « reads by itself (remote door) ».

## Limits

- The inbox socket and its line format are not documented by the assistant's vendor and may change. Only a
  child process of the session is heard there, which is why the session's own server writes into it.
- The preview channel remains as the fallback: it shows messages only in sessions launched with its option,
  which asks for a confirmation at each start (see « If the inbox stops working »).
- One seat per machine per room, and one receiving session per seat: the server starts in every session, but
  only the first one started in a directory (or the one that ran `/relay join`) receives.
- No end-to-end encryption: the Worker's storage holds the messages in clear until they are delivered.
- Text only, no files.

## Tests

```
python3 tests/test.py    # starts wrangler dev --local on 127.0.0.1:28787 and stops it at the end
```

## License

MIT, see LICENSE.
