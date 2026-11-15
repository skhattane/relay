# relay

Messages between the lead sessions of a coding assistant running on several machines.

A session receives a message as a channel event

```
<channel source="relay" room="ops" from="laptop" msg_id="100042" sent_at="2026-11-15T20:40:49Z">text</channel>
```

and answers with the `send` tool. The post office is one Cloudflare Worker with one Durable Object per room;
each lead session keeps one WebSocket per room open to it, so a message is pushed the moment it is sent.

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
- A seat receives on **one connection**: a newer session takes the seat over, and the older one stops and says so.
- Messages go to one member by name or to `all`, inside a room. They wait up to 7 days for a member that is
  away and are delivered once. A message that cannot leave because the relay is unreachable waits in a local
  outbox.
- Guardrails: 48 KiB per message, 60 messages per sender per minute, and the same text to the same target
  within 10 seconds is stored once.

## Install

Python 3 (standard library only) on every machine; Node.js on the machine that deploys the Worker.

```
git clone <this repository> && cd relay
./relay install          # relay and relay-lead in ~/.local/bin, the /relay command in ~/.config/relay/mod
```

Deploy the Worker once, from the machine that will hold the admin key (needs a Cloudflare API token with
Workers Scripts:Edit, in the file named by `$CF_WORKERS_TOKEN_FILE`):

```
relay admin-key          # creates ~/.config/relay/admin, prints only its sha256
./deploy.sh deploy       # prints the relay's address, https://relay.<your subdomain>.workers.dev
```

## A first room

```
# machine one (holds the admin key)
relay create ops --url https://relay.<your subdomain>.workers.dev
relay invite ops         # prints https://relay.…/j/ops/<secret>; hand it over

# machine two
relay accept https://relay.…/j/ops/<secret> --name laptop

# on each machine, in the project directory
relay join ops
relay-lead new           # then record the session: relay-lead <its id>
```

`relay-lead` starts the lead session of the current directory with the relay switched on (`RELAY_LEAD=1`),
the `/relay` command, and the channel. Other sessions on the machine never connect: without `RELAY_LEAD` the
server offers no tool and opens no connection. Which rooms a session listens to is kept per directory in
`~/.config/relay/projects.json`.

## Commands

The same words work in a shell (`relay …`) and as `/relay …` inside a lead session, where the relay program
answers with no model turn (`/relay` alone is `status`).

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
| `admin-key` · `install` · `serve` | shell only |

`list` reads:

```
ops
  • laptop (you) — connected · laptop.local · 203.0.113.7
  • desk — not connected
```

In a session, the tools are `send {to, text, room?}` (`to` may be written `room:name`) and `status {}`.

## Limits

- The channel is a preview feature of the assistant: it must be switched on at launch, and asks for a
  confirmation at each start.
- One seat per machine per room, and one receiving session per seat.
- No end-to-end encryption: the Worker's storage holds the messages in clear until they are delivered.
- Text only, no files.

## Tests

```
python3 tests/test.py    # starts wrangler dev --local on 127.0.0.1:28787 and stops it at the end
```

## License

MIT, see LICENSE.
