// relay, the hosted half: the post office between the lead sessions of several machines.
//
// One Worker in front, one Durable Object per ROOM behind (idFromName(room)), so a message can never leave
// its room. SQLite storage and hibernating sockets: an idle room costs nothing.
//
// Who a caller is comes from its bearer token only. The Worker hashes it and hands the hash to the room on
// headers it writes itself (every incoming X-Relay-* header is dropped first); the room never sees a raw
// token. A room knows its members by those hashes. Creating a room needs the admin key, whose hash is the
// plain var ADMIN.
//
// HTTP (JSON bodies and answers, {"error": "…"} on refusal):
//   POST /v1/rooms/<room>/create   admin    {"seat": "<sha256 hex>", "name": "…"}   the creator becomes owner
//   POST /v1/rooms/<room>/invite   member   → {"secret", "expires"}               one use, INVITE_SECONDS
//   POST /j/<room>/<secret>        nobody   {"seat", "name"}                      accepts an invitation
//   GET  /v1/rooms/<room>/hello    member   → the hello frame below
//   POST /v1/rooms/<room>/send     member   {"ref", "to": "<name>|all", "text"}   → {"type":"sent"…}
//   POST /v1/rooms/<room>/rename   member   {"name"}
//   POST /v1/rooms/<room>/remove   owner    {"name"}
//   POST /v1/rooms/<room>/leave    member
//   GET  /v1/rooms/<room>/connect  member   WebSocket, the seat's receiving connection (one per seat);
//                                           ?takeover=0 is refused with 409 while the seat has a live one
// WebSocket frames:
//   member → room  {"type":"send","ref","to","text"}   {"type":"ack","id":N}   the text "ping" (answered "pong")
//   room → member  {"type":"hello","room","name","role","members":[{"name","role","you","connected","host","ip"}]}
//                  {"type":"msg","id":N,"from","to","at","text"}   {"type":"sent","ref","to":[…],"duplicate"?}   {"type":"error","ref","error"}
// A refusal because the seat is not a member carries "code": "not_member", so the machine can forget the seat.
// Close codes: 4001 the seat was taken over by a newer connection, 4003 the seat left or was removed.
import { DurableObject } from "cloudflare:workers";

const ROOM_RE = /^[a-z0-9][a-z0-9-]{0,31}$/;
const NAME_RE = ROOM_RE;
const RESERVED = new Set(["all", "relay"]);
const SYSTEM = "relay"; // the name the room's own notices come from
const HEX64 = /^[0-9a-f]{64}$/;
const MAX_TEXT = 48 * 1024;
const MAX_BODY = 64 * 1024;
const KEEP_MS = 7 * 86400e3;
const RATE = 60;
const SAME_MS = 10e3;
const VERSION = 1; // a socket accepted under another version is closed, so its machine reconnects
const TAKEN_OVER = 4001;
const GONE = 4003;

const OPS = {
  create: "POST", invite: "POST", accept: "POST", hello: "GET", send: "POST",
  rename: "POST", remove: "POST", leave: "POST", connect: "GET",
};

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj) + "\n", { status, headers: { "content-type": "application/json" } });
}

async function sha256hex(s) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(s));
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function nameError(name) {
  if (!NAME_RE.test(name)) return "a name is 1 to 32 characters among a-z, 0-9 and -, starting with a letter or a digit";
  if (RESERVED.has(name)) return "« " + name + " » is reserved";
  return "";
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const parts = url.pathname.split("/").filter(Boolean);
    let room = "", op = "", secret = "";
    if (parts.length === 4 && parts[0] === "v1" && parts[1] === "rooms") [room, op] = [parts[2], parts[3]];
    else if (parts.length === 3 && parts[0] === "j") [room, op, secret] = [parts[1], "accept", parts[2]];
    if (!ROOM_RE.test(room) || !(op in OPS)) return json({ error: "not found" }, 404);
    if (op === "accept" && request.method === "GET") {
      return new Response("An invitation to the room " + room + ". On the machine that joins, run:  relay accept " + url.href + "\n");
    }
    if (request.method !== OPS[op]) return json({ error: "method not allowed" }, 405);

    // Built from nothing: no header the client sent reaches the room unless it is copied here on purpose.
    const inner = new Headers();
    inner.set("X-Relay-Room", room);
    inner.set("X-Relay-Ip", request.headers.get("CF-Connecting-IP") || "");
    inner.set("X-Relay-Host", (request.headers.get("X-Relay-Host") || "").replace(/[^A-Za-z0-9._-]/g, "").slice(0, 64));
    inner.set("X-Relay-Admin", "0");
    if (op === "accept") {
      if (!secret || secret.length > 128) return json({ error: "this invitation is not valid" }, 403);
      inner.set("X-Relay-Invite", await sha256hex(secret));
    } else {
      const auth = request.headers.get("Authorization") || "";
      if (!auth.startsWith("Bearer ") || !auth.slice(7).trim()) return json({ error: "unauthorized" }, 401);
      const hash = await sha256hex(auth.slice(7).trim());
      if (op === "create") {
        if (!env.ADMIN || hash !== env.ADMIN) return json({ error: "creating a room needs the admin key" }, 403);
        inner.set("X-Relay-Admin", "1");
      } else {
        inner.set("X-Relay-Seat", hash);
      }
    }
    let body = null;
    if (op === "connect") {
      if ((request.headers.get("Upgrade") || "").toLowerCase() !== "websocket") return json({ error: "expected a websocket" }, 426);
      inner.set("Upgrade", "websocket");
      inner.set("X-Relay-Takeover", url.searchParams.get("takeover") === "0" ? "0" : "1");
    } else if (request.method === "POST") {
      body = await request.text();
      if (body.length > MAX_BODY) return json({ error: "request over " + MAX_BODY + " bytes" }, 413);
      inner.set("content-type", "application/json");
    }
    const stub = env.ROOM.get(env.ROOM.idFromName(room));
    return stub.fetch(new Request("https://room.internal/" + op, { method: request.method, headers: inner, body }));
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    const sql = ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS seats (seat TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, role TEXT NOT NULL, since TEXT NOT NULL)");
    sql.exec("CREATE TABLE IF NOT EXISTS invites (code TEXT PRIMARY KEY, by_seat TEXT NOT NULL, by_name TEXT NOT NULL, expires INTEGER NOT NULL)");
    // sender and recipient are seat hashes, so a message survives a rename of either end; '' is the room itself.
    sql.exec(
      "CREATE TABLE IF NOT EXISTS mail (id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL, " +
        "addressed TEXT NOT NULL, ref TEXT NOT NULL, at TEXT NOT NULL, at_ms INTEGER NOT NULL, text TEXT NOT NULL, " +
        "UNIQUE (sender, ref, recipient))",
    );
    // One row per accepted send: the rate limit (units = recipients), the 10-second duplicate, a retry of a ref.
    sql.exec(
      "CREATE TABLE IF NOT EXISTS sends (sender TEXT NOT NULL, ref TEXT NOT NULL, addressed TEXT NOT NULL, digest TEXT NOT NULL, " +
        "at_ms INTEGER NOT NULL, targets TEXT NOT NULL, units INTEGER NOT NULL, PRIMARY KEY (sender, ref))",
    );
    // Message numbers never start again at 1: a machine that remembers low numbers as read would drop new
    // messages. A row written and removed at a high number moves the counter.
    if (!sql.exec("SELECT seq FROM sqlite_sequence WHERE name = 'mail'").toArray().length) {
      sql.exec("INSERT INTO mail (id, sender, recipient, addressed, ref, at, at_ms, text) VALUES (100000, '', '', '', 'seed', '', 0, '')");
      sql.exec("DELETE FROM mail WHERE id = 100000");
    }
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    // A socket accepted by an earlier version can outlive an update: it would look connected and receive
    // nothing. Close it; the machine reconnects at once.
    for (const ws of ctx.getWebSockets()) {
      const a = ws.deserializeAttachment();
      if (!a || a.v !== VERSION) {
        try {
          ws.close(1012, "the relay was updated");
        } catch {
          // already closing
        }
      }
    }
  }

  seats() {
    return this.ctx.storage.sql.exec("SELECT seat, name, role FROM seats ORDER BY name").toArray();
  }

  seat(hash) {
    return this.ctx.storage.sql.exec("SELECT seat, name, role FROM seats WHERE seat = ?", hash).toArray()[0] || null;
  }

  open(seat) {
    return this.ctx.getWebSockets("seat:" + seat).filter((ws) => ws.readyState === 1);
  }

  hello(seat, room) {
    const me = this.seat(seat);
    const members = this.seats().map((s) => {
      const ws = this.open(s.seat)[0];
      const a = (ws && ws.deserializeAttachment()) || null;
      return { name: s.name, role: s.role, you: s.seat === seat, connected: !!ws, host: (a && a.host) || "", ip: (a && a.ip) || "" };
    });
    return { type: "hello", room, name: me ? me.name : "", role: me ? me.role : "", members };
  }

  broadcast() {
    for (const ws of this.ctx.getWebSockets()) {
      const a = ws.deserializeAttachment();
      if (!a || a.v !== VERSION || ws.readyState !== 1) continue;
      try {
        ws.send(JSON.stringify(this.hello(a.seat, a.room)));
      } catch {
        // a closing socket learns the roster at its next connect
      }
    }
  }

  frame(r, names) {
    const from = r.sender === "" ? SYSTEM : names.get(r.sender) || "(a former member)";
    const to = r.addressed === "all" ? "all" : names.get(r.recipient) || "";
    return { type: "msg", id: r.id, from, to, at: r.at, text: r.text };
  }

  names() {
    return new Map(this.seats().map((s) => [s.seat, s.name]));
  }

  push(recipient, rows) {
    const names = this.names();
    for (const ws of this.open(recipient)) {
      for (const r of rows) {
        try {
          ws.send(JSON.stringify(this.frame(r, names)));
        } catch {
          // the message stays stored and is handed over at the next connect
        }
      }
    }
  }

  store(sender, recipient, addressed, ref, now, text) {
    const sql = this.ctx.storage.sql;
    const at = new Date(now).toISOString().replace(/\.\d+Z$/, "Z");
    sql.exec(
      "INSERT OR IGNORE INTO mail (sender, recipient, addressed, ref, at, at_ms, text) VALUES (?, ?, ?, ?, ?, ?, ?)",
      sender, recipient, addressed, ref, at, now, text,
    );
    const rows = sql.exec(
      "SELECT id, sender, recipient, addressed, at, text FROM mail WHERE sender = ? AND ref = ? AND recipient = ?",
      sender, ref, recipient,
    ).toArray();
    this.push(recipient, rows);
  }

  // Nothing enters or changes in a room silently: every member is told, absent ones when they come back.
  notice(text) {
    const now = Date.now();
    const ref = "n-" + crypto.randomUUID();
    for (const s of this.seats()) this.store("", s.seat, "one", ref, now, text);
  }

  prune(now) {
    const sql = this.ctx.storage.sql;
    sql.exec("DELETE FROM mail WHERE at_ms < ?", now - KEEP_MS);
    sql.exec("DELETE FROM sends WHERE at_ms < ?", now - KEEP_MS);
    // An expired code is kept a while longer, so that using it says « expired » rather than « not valid ».
    sql.exec("DELETE FROM invites WHERE expires < ?", now - KEEP_MS);
  }

  // A send, from the HTTP route or a socket. The digest is computed by the caller before this point so that
  // nothing awaits between the checks and the writes.
  post(me, m, digest) {
    const sql = this.ctx.storage.sql;
    const ref = String(m.ref || "");
    const to = String(m.to || "");
    const text = String(m.text || "");
    const refuse = (error) => ({ type: "error", ref, error });
    if (!ref || ref.length > 64) return refuse("a message needs a ref of at most 64 characters");
    if (!text) return refuse("empty message");
    if (new TextEncoder().encode(text).length > MAX_TEXT) return refuse("message over " + MAX_TEXT + " bytes");
    const now = Date.now();
    this.prune(now);
    const sent = (targets) => ({ type: "sent", ref, to: JSON.parse(targets) });
    const again = sql.exec("SELECT targets FROM sends WHERE sender = ? AND ref = ?", me.seat, ref).toArray()[0];
    if (again) return sent(again.targets);
    const same = sql.exec(
      "SELECT targets FROM sends WHERE sender = ? AND addressed = ? AND digest = ? AND at_ms > ? ORDER BY at_ms DESC LIMIT 1",
      me.seat, to, digest, now - SAME_MS,
    ).toArray()[0];
    // Said, not swallowed: the sender learns its second copy was not stored.
    if (same) return { ...sent(same.targets), duplicate: true };
    if (to === me.name) return refuse("a message to yourself");
    const others = this.seats().filter((s) => s.seat !== me.seat);
    const targets = to === "all" ? others : others.filter((s) => s.name === to);
    if (!targets.length) {
      return refuse(to === "all" ? "nobody else is in this room" :
        "unknown recipient " + JSON.stringify(to) + " (members: " + (others.map((s) => s.name).join(", ") || "none") + ", or all)");
    }
    // A message to all costs one unit per recipient, so a large room cannot be flooded by one sender.
    const recent = sql.exec("SELECT COALESCE(SUM(units), 0) AS n FROM sends WHERE sender = ? AND at_ms > ?", me.seat, now - 60e3).one().n;
    if (recent + targets.length > RATE) return refuse("too many messages this minute");
    const names = JSON.stringify(targets.map((s) => s.name));
    sql.exec("INSERT INTO sends (sender, ref, addressed, digest, at_ms, targets, units) VALUES (?, ?, ?, ?, ?, ?, ?)",
      me.seat, ref, to, digest, now, names, targets.length);
    for (const t of targets) this.store(me.seat, t.seat, to === "all" ? "all" : "one", ref, now, text);
    return sent(names);
  }

  close(seat, code, reason) {
    for (const ws of this.ctx.getWebSockets("seat:" + seat)) {
      try {
        ws.close(code, reason);
      } catch {
        // already closing
      }
    }
  }

  ttl() {
    return Number(this.env.INVITE_SECONDS) || 86400;
  }

  async fetch(request) {
    const op = new URL(request.url).pathname.slice(1);
    const h = request.headers;
    const room = h.get("X-Relay-Room") || "";
    const sql = this.ctx.storage.sql;
    let body = {};
    if (request.method === "POST") {
      try {
        body = JSON.parse((await request.text()) || "{}") || {};
      } catch {
        return json({ error: "the request is not JSON" }, 400);
      }
    }
    const digest = op === "send" ? await sha256hex(String(body.text || "")) : "";
    const now = Date.now();

    if (op === "create") {
      if (h.get("X-Relay-Admin") !== "1") return json({ error: "creating a room needs the admin key" }, 403);
      const seat = String(body.seat || ""), name = String(body.name || "");
      if (!HEX64.test(seat)) return json({ error: "a seat is the sha256 hex of its token" }, 400);
      const bad = nameError(name);
      if (bad) return json({ error: bad }, 400);
      if (this.seats().length) return json({ error: "the room " + room + " already exists" }, 409);
      sql.exec("INSERT INTO seats (seat, name, role, since) VALUES (?, ?, 'owner', ?)", seat, name, new Date(now).toISOString());
      return json({ ok: true, room, name, role: "owner" });
    }

    if (op === "accept") {
      const code = h.get("X-Relay-Invite") || "";
      const inv = sql.exec("SELECT code, by_seat, by_name, expires FROM invites WHERE code = ?", code).toArray()[0];
      if (!inv) return json({ error: "this invitation is not valid (already used, expired, or never issued)" }, 403);
      if (inv.expires <= now) {
        sql.exec("DELETE FROM invites WHERE code = ?", code);
        return json({ error: "this invitation has expired" }, 403);
      }
      const seat = String(body.seat || ""), name = String(body.name || "");
      if (!HEX64.test(seat)) return json({ error: "a seat is the sha256 hex of its token" }, 400);
      if (this.seat(seat)) return json({ error: "this seat is already a member of the room" }, 409);
      const bad = nameError(name);
      if (bad) return json({ error: bad }, 400);
      // The code is spent only once the seat can really join, so a taken name can be retried with --name.
      if (this.seats().some((s) => s.name === name)) return json({ error: "the name " + name + " is taken in this room: choose another with --name" }, 409);
      sql.exec("DELETE FROM invites WHERE code = ?", code);
      sql.exec("INSERT INTO seats (seat, name, role, since) VALUES (?, ?, 'member', ?)", seat, name, new Date(now).toISOString());
      const by = this.seat(inv.by_seat);
      this.notice(name + " joined the room, invited by " + (by ? by.name : inv.by_name));
      this.broadcast();
      return json({ ok: true, room, name, role: "member" });
    }

    const me = this.seat(h.get("X-Relay-Seat") || "");
    if (!me) return json({ error: "this seat is not a member of the room " + room, code: "not_member" }, 403);

    if (op === "hello") return json(this.hello(me.seat, room));

    if (op === "invite") {
      this.prune(now);
      const bytes = crypto.getRandomValues(new Uint8Array(24));
      const secret = btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
      const ttl = this.ttl();
      sql.exec("INSERT INTO invites (code, by_seat, by_name, expires) VALUES (?, ?, ?, ?)", await sha256hex(secret), me.seat, me.name, now + ttl * 1000);
      const valid = ttl % 3600 === 0 ? ttl / 3600 + " h" : ttl + " s";
      this.notice(me.name + " created an invitation (valid " + valid + ")");
      return json({ ok: true, secret, expires: new Date(now + ttl * 1000).toISOString() });
    }

    if (op === "send") {
      const r = this.post(me, body, digest);
      return json(r, r.type === "sent" ? 200 : 400);
    }

    if (op === "rename") {
      const name = String(body.name || "");
      const bad = nameError(name);
      if (bad) return json({ error: bad }, 400);
      if (name === me.name) return json({ ok: true, name });
      if (this.seats().some((s) => s.name === name)) return json({ error: "another member is named " + name }, 409);
      sql.exec("UPDATE seats SET name = ? WHERE seat = ?", name, me.seat);
      this.notice(me.name + " is now named " + name);
      this.broadcast();
      return json({ ok: true, name });
    }

    if (op === "remove") {
      if (me.role !== "owner") return json({ error: "only an owner can remove a member" }, 403);
      const name = String(body.name || "");
      const it = this.seats().find((s) => s.name === name);
      if (!it) return json({ error: "no member is named " + JSON.stringify(name) }, 404);
      if (it.seat === me.seat) return json({ error: "to give up your own seat, use leave-room" }, 400);
      this.drop(it.seat);
      this.close(it.seat, GONE, "removed from the room");
      this.notice(name + " was removed by " + me.name);
      this.broadcast();
      return json({ ok: true, removed: name });
    }

    if (op === "leave") {
      const owners = this.seats().filter((s) => s.role === "owner").length;
      if (me.role === "owner" && owners <= 1) return json({ error: "the last owner cannot leave the room" }, 409);
      this.drop(me.seat);
      this.close(me.seat, GONE, "left the room");
      this.notice(me.name + " left the room");
      this.broadcast();
      return json({ ok: true, left: room });
    }

    if (op === "connect") {
      // A seat receives on one connection only. An explicit join wins and the older one is told why; an
      // automatic join at a session's start never takes a seat another session holds.
      if (h.get("X-Relay-Takeover") === "0" && this.open(me.seat).length) {
        return json({ error: "this seat is held by another session" }, 409);
      }
      this.close(me.seat, TAKEN_OVER, "this seat was taken over by another session");
      const [client, server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server, ["seat:" + me.seat]);
      server.serializeAttachment({ v: VERSION, seat: me.seat, room, ip: h.get("X-Relay-Ip") || "", host: h.get("X-Relay-Host") || "" });
      this.prune(now);
      this.broadcast();
      const names = this.names();
      for (const r of sql.exec("SELECT id, sender, recipient, addressed, at, text FROM mail WHERE recipient = ? ORDER BY id", me.seat)) {
        server.send(JSON.stringify(this.frame(r, names)));
      }
      return new Response(null, { status: 101, webSocket: client });
    }

    return json({ error: "not found" }, 404);
  }

  drop(seat) {
    const sql = this.ctx.storage.sql;
    sql.exec("DELETE FROM seats WHERE seat = ?", seat);
    sql.exec("DELETE FROM mail WHERE recipient = ?", seat);
    sql.exec("DELETE FROM invites WHERE by_seat = ?", seat);
  }

  async webSocketMessage(ws, message) {
    const who = ws.deserializeAttachment();
    if (!who || who.v !== VERSION) return ws.close(1012, "the relay was updated");
    let m;
    try {
      m = JSON.parse(typeof message === "string" ? message : new TextDecoder().decode(message));
    } catch {
      return;
    }
    if (!m || typeof m !== "object") return;
    const digest = m.type === "send" ? await sha256hex(String(m.text || "")) : "";
    const me = this.seat(who.seat);
    if (!me) return ws.close(GONE, "this seat is no longer a member of the room");
    if (m.type === "ack") {
      this.ctx.storage.sql.exec("DELETE FROM mail WHERE id = ? AND recipient = ?", Number(m.id) || 0, me.seat);
      return;
    }
    if (m.type === "send") ws.send(JSON.stringify(this.post(me, m, digest)));
  }

  async webSocketClose(ws, code, reason) {
    try {
      ws.close(code, reason);
    } catch {
      // already closed
    }
    this.broadcast();
  }

  async webSocketError() {}
}
