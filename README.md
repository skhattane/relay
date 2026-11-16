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
| `send <[room:]name\|[room:]all> [--say s] <text…>` | one message (the room may be left out when there is only one); `--say`: one sentence to be said aloud |
| `rename [<room>] <name>` | this machine's name in a room |
| `create <room> [--name n] [--url u]` | a new room; needs the admin key |
| `invite <room>` | a one-time link, valid 24 h |
| `accept <link> [--name n]` | takes a seat in that room |
| `remove <room> <name>` | (owner) takes a member's seat away at once |
| `leave-room <room>` | gives up this machine's seat |
| `connector <room> <name>` | (owner) a seat for a remote connector that connects by OAuth; prints its URL, writes a one-time enrolment code to a file |
| `grants <room>` / `revoke <room> <name>` | (owner) the connector grants, without secrets / delete one seat's grants (the seat stays) |
| `ask <room> --say s --option a --option b […] [--wait]` | a decision request (see « Decisions »); `--wait` prints one JSON line, exit 0/3/4 |
| `asks <room>` / `decision <room> <n>` | the open decision requests / the state of one, as one JSON line |
| `decide <room> <n> <option> [--note t]` / `withdraw <room> <n>` | (a decider) answer a request / (its author) withdraw it |
| `decider <room> <name> on\|off` | (owner) mark or clear a member as a decider |
| `help` | one line per command |
| `admin-key` · `serve` | shell only |

`list` reads:

```
ops
  • laptop (you) — connected · laptop.local · 203.0.113.7
  • desk — not connected
```

In a session, the server's tools are `send {to, text, say?, room?}` (`to` may be written `room:name`) and
`status {}`; the CLI shows them under the plugin's prefix.

A message may carry **a line to say aloud**: `say` is one plain sentence, `text` the detail (it may then be
empty). The room stores the two apart; a machine receives `text` as the sentence, a blank line, then the detail
(and the sentence again in a `say` field), so a client that knows nothing of it shows the whole message. When a
session answers a remote seat (« reads by itself (remote door) »), it puts one sentence in `say`.

## A member that is not a machine

Some clients cannot start a local server: they only take a remote MCP connector over HTTPS (the Claude apps'
« custom connector », for example). Such a client holds a **connector seat** and reaches it through the Worker's
door, `POST /mcp/<room>` (MCP Streamable HTTP, JSON answers, no stream), authorized by OAuth. It gets no push, so
it pulls:

- `read {wait_seconds?, again?}` — the messages waiting for the seat, oldest first, each returned once. With
  nothing waiting it holds the request up to `wait_seconds` (0–25, default 20; a larger value is clamped, as a
  client may cut a request at about 30 s) and answers as soon as one arrives, else « no new message ».
  `again: true` returns the last batch once more, if an answer was lost on the way. A message with a line to say
  reads `SAY: <the sentence>` then `DETAIL: <the rest>`; the door tells its client to speak the SAY line as is and
  give the detail only when asked.
- `news {}` — what is waiting, without taking it: how many messages, from whom, the oldest and newest time, the
  newest one's SAY line (or its first 120 characters), and who is connected.
- `send {to, text, say?}` — to a member by name or `all`, with the same limits and duplicate rule as every other
  road.
- `status {}` — the members, their role, and how each receives.

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

Still true: messages sit in clear on the Worker until read. A message from such a seat is a teammate's request,
never the user's approval: a permission prompt in the receiving session is still the user's to answer. Give the
seat a room of its own with the one session it drives, not a room where sessions run unattended. In `status` and
`list` it shows as « reads by itself (remote door) » once it has used the door.

### A seat made by the former URL door

Earlier versions had `relay remote-seat`, whose connector address carried the seat's token. That door is gone: a
request to `/mcp/<room>/<token>` gets the same 404 as any unknown path. Remove such a seat like any other,
`relay remove <room> <name>`, then delete its file `~/.config/relay/remote/<room>-<name>.url`. Its name is free
again for `relay connector`. An `Origin` is checked on the door only when the Worker's `MCP_ORIGINS` var lists
some.

The door writes one JSON line per request: time, method or tool, room, the member's name, status, duration, and for
`read` the wait asked, the wait used and how many messages it returned — never a token, a seat hash or a message's
text. The OAuth endpoints write one line each too (endpoint, room, status, duration), never a code or a token.
The Worker keeps its logs (Workers Logs, `observability` in `worker/wrangler.jsonc`; 3 days on the free plan, 7 on
paid), read in the dashboard under the Worker's Observability tab. No URL of the Worker carries a secret, so the
platform's own request log records none either.

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
  of them who are deciders.
- **Decide.** `relay decide <room> <n> <option> [--note "<text>"]`, or the remote door's `decide` tool. The option
  must be one of that request's labels exactly (case aside; nothing fuzzy); a note is at most 300 characters. The
  first valid decision wins; a second is refused with who decided and when. The room records who (the seat's name),
  through which door (`machine`, or `remote` with the name the connector gives itself) and when, tells the room, and
  delivers to the author a notice from `relay` that starts `DECISION #<n>:` and carries the decision as data (a
  `decision` field in the machine's frame). Nobody can send as `relay`, so a member's message cannot pass for a
  decision. If the request expires first, the author gets `DECISION #<n>: none — the request expired …`.
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
  prints the same line (and exits 0 whatever the state).
- **On the remote door.** `asks {}` lists the open requests this seat may decide (number, say line, options, who
  asks, time left) and takes nothing; `decide {ask, option, note?}` answers one. The door tells its client to read a
  request's say line aloud with its options, and to call `decide` only with an option the person has just spoken for
  that numbered request, repeating it back first — never on its own judgment.
- **In a session.** The plugin tells the session that a `DECISION #<n>` notice from `relay` is the answer of the
  member named in it, with the authority the room's owner gave that member as a decider, and answers only request
  `<n>`; a free-text message is still a teammate's request, never an approval.
- **Limits.** At most 20 open requests per seat and 30 requests per seat per minute; a say line of 500 characters,
  a detail of 4 KiB; a request names at most 16 members. Expiry is enforced whenever the room is read, and by the
  room's alarm (set only once a request exists). Closed requests are kept 7 days.

**What this does not do by itself.** It does not answer the coding assistant's own permission prompts. A
decision reaches a session as a notice; the session still asks the person at the keyboard. To let a decider answer
a permission prompt from afar, the machine's owner would have to configure a hook on that machine that calls
`relay ask --wait` and acts on its exit code — that is the machine owner's choice, and nothing here installs or
enables one. For example, the command of a `PreToolUse` hook could be:

```
relay ask ops --say "Allow the session on desk to run its next command?" --option allow --option deny \
  --for 120 --wait | grep -q '"option": "allow"' && exit 0 || exit 2
```

(exit 0 lets the tool run; exit 2 blocks it; an expired or withdrawn request blocks it too). Read the hook's own
documentation before using anything like it: such a hook gives every decider of that room the power to approve
commands on that machine.

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
