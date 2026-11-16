#!/usr/bin/env python3
"""End-to-end checks of relay against a local Worker (wrangler dev --local), started and stopped here.

Each fake machine has its own config, state and home directories under tests/.run/<run>/; nothing outside the
repository is touched. Prints one PASS/FAIL line per check and a final count; exits non-zero on any failure.
"""
import base64, hashlib, json, os, queue, re, signal, socket, stat, subprocess, sys, threading, time
import http.client as httpclient
import urllib.error, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROG = os.path.join(ROOT, "plugin", "bin", "relay")
MODULE = os.path.join(ROOT, "plugin", "hooks", "register.ts")
PORT, DEAD = 28787, 28788
URL, DEAD_URL = "http://127.0.0.1:%d" % PORT, "http://127.0.0.1:%d" % DEAD
RUN = os.path.join(ROOT, "tests", ".run", time.strftime("%Y%m%d-%H%M%S"))
INVITE_SECONDS = 8
MCP_WINDOW = 5.5  # a little over the door's 5-second window (MCP_RATE_MS in worker/relay.js)
results = []


def check(name, ok, detail=""):
    ok = bool(ok)
    results.append(ok)
    print(("PASS " if ok else "FAIL ") + name + (" — " + str(detail).replace("\n", " | ")[:300] if detail else ""), flush=True)


class Machine:
    def __init__(self, name):
        self.name = name
        self.dir = os.path.join(RUN, name)
        self.config = os.path.join(self.dir, "config")
        self.state = os.path.join(self.dir, "state")
        self.home = os.path.join(self.dir, "home")
        self.proj = os.path.join(self.dir, "proj")
        self.bin = os.path.join(self.dir, "bin")
        for d in (self.config, self.state, self.home, self.proj):
            os.makedirs(d, exist_ok=True)
        self.proj = os.path.realpath(self.proj)

    def env(self, inbox=None):
        # Never the inbox of the session that runs these tests: an inbox of our own, or one that does not exist.
        env = dict(os.environ, RELAY_CONFIG_DIR=self.config, RELAY_STATE_DIR=self.state, HOME=self.home,
                   RELAY_BIN_DIR=self.bin, RELAY_INBOX=inbox or os.path.join(self.dir, "no-inbox.sock"))
        for k in ("CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN"):
            env.pop(k, None)
        return env

    def cli(self, *args, cwd=None, env=None):
        r = subprocess.run([PROG, *args], env=env or self.env(), cwd=cwd or self.proj, capture_output=True, text=True,
                           timeout=40, stdin=subprocess.DEVNULL)
        return r.returncode, r.stdout.strip(), r.stderr.strip()

    def seat(self, room):
        with open(os.path.join(self.config, "seats", room + ".json")) as fh:
            return json.load(fh)["token"]

    def set_url(self, url):
        with open(os.path.join(self.config, "config.json"), "w") as fh:
            json.dump({"url": url}, fh)

    def projects(self):
        try:
            with open(os.path.join(self.config, "projects.json")) as fh:
                return json.load(fh)
        except OSError:
            return {}

    def sockets(self):
        try:
            return sorted(os.listdir(os.path.join(self.state, "ctl")))
        except OSError:
            return []


class Session:
    """A lead session as the assistant sees it: an MCP client over the server's stdio."""

    def __init__(self, m, wrap=False, tag="", inbox=None):
        env = m.env(inbox)
        # wrap: the server's parent is a shell, not this test, so a command run from here is not its descendant.
        argv = ["sh", "-c", '"$0" serve; :', PROG] if wrap else [PROG]
        self.errpath = os.path.join(m.dir, "serve%s.err" % tag)
        self.err = open(self.errpath, "a")
        self.p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.err, env=env,
                                  cwd=m.proj, text=True)
        self.cv = threading.Condition()
        self.inbox, self.replies, self.n = [], {}, 0
        threading.Thread(target=self._read, daemon=True).start()
        self.init = self.call("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                             "clientInfo": {"name": "t", "version": "0"}})
        self._write({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _read(self):
        for line in self.p.stdout:
            try:
                m = json.loads(line)
            except ValueError:
                continue
            with self.cv:
                if "id" in m and "method" not in m:
                    self.replies[m["id"]] = m
                elif m.get("method") == "notifications/claude/channel":
                    self.inbox.append(m["params"])
                self.cv.notify_all()

    def _write(self, obj):
        self.p.stdin.write(json.dumps(obj) + "\n")
        self.p.stdin.flush()

    def call(self, method, params=None, timeout=30):
        self.n += 1
        mid = self.n
        self._write({"jsonrpc": "2.0", "id": mid, "method": method, "params": params or {}})
        with self.cv:
            self.cv.wait_for(lambda: mid in self.replies, timeout=timeout)
            return self.replies.pop(mid)

    def tool(self, name, args):
        r = self.call("tools/call", {"name": name, "arguments": args})["result"]
        return r["content"][0]["text"], bool(r.get("isError"))

    def send(self, to, text, room=None):
        a = {"to": to, "text": text}
        if room:
            a["room"] = room
        return self.tool("send", a)

    def wait(self, pred, timeout=8):
        """The first event matching pred (taken out of the inbox), or None."""
        end = time.time() + timeout
        with self.cv:
            while True:
                for i, ev in enumerate(self.inbox):
                    if pred(ev):
                        return self.inbox.pop(i)
                left = end - time.time()
                if left <= 0:
                    return None
                self.cv.wait(left)

    def text(self, text, timeout=8):
        return self.wait(lambda e: e["content"] == text, timeout)

    def drain(self, seconds=1.0):
        time.sleep(seconds)
        with self.cv:
            out, self.inbox = self.inbox, []
        return out

    def stderr(self):
        with open(self.errpath) as fh:
            return fh.read()

    def stop(self):
        try:
            self.p.stdin.close()
        except OSError:
            pass
        try:
            self.p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.p.terminate()
            self.p.wait(timeout=5)
        self.err.close()


class Inbox:
    """A stand-in for the session's inbox socket: collects every line written to it."""

    def __init__(self, path):
        self.s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.s.bind(path)
        self.s.listen(8)
        self.lines, self.cv = [], threading.Condition()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.s.accept()
            except OSError:
                return
            buf = b""
            with conn:
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
            with self.cv:
                self.lines += [l + "\n" for l in buf.decode().split("\n") if l]
                self.cv.notify_all()

    def wait(self, pred, timeout=10):
        with self.cv:
            self.cv.wait_for(lambda: any(pred(l) for l in self.lines), timeout=timeout)
            return next((l for l in self.lines if pred(l)), None)

    def close(self):
        self.s.close()


def exact_line(content):
    return json.dumps({"type": "user", "message": {"role": "user", "content": content}}, ensure_ascii=False,
                      separators=(",", ":")) + "\n"


MODULE_RUNNER = """
import { spawnSync } from 'node:child_process'
const mod = await import(process.argv[2])
const $ = { process: { run: async (argv) => {
  const r = spawnSync(argv[0], argv.slice(1), { encoding: 'utf8' })
  return { stdout: r.stdout, stderr: r.stderr, exitCode: r.status }
} } }
process.stdout.write(JSON.stringify(await mod.answer($, process.argv[3] || '')))
"""


def module_answer(env, typed):
    """What the /relay command module answers, run under node with a stand-in for the engine."""
    runner = os.path.join(RUN, "run-module.mjs")
    with open(runner, "w") as fh:
        fh.write(MODULE_RUNNER)
    r = subprocess.run(["node", runner, MODULE, typed], env=env, capture_output=True, text=True, timeout=30)
    try:
        return json.loads(r.stdout)["text"]
    except (ValueError, KeyError):
        return "(no answer: %s)" % r.stderr.strip()[-200:]


def http(method, path, token=None, body=None, headers=None):
    req = urllib.request.Request(URL + path, method=method, data=None if body is None else json.dumps(body).encode())
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as err:
        try:
            return err.code, json.loads(err.read() or b"{}")
        except ValueError:
            return err.code, {}


def ws_status(path, token):
    """The status the relay answers to a WebSocket upgrade."""
    c = httpclient.HTTPConnection("127.0.0.1", PORT, timeout=20)
    c.request("GET", path, headers={"Upgrade": "websocket", "Connection": "Upgrade", "Sec-WebSocket-Version": "13",
                                    "Sec-WebSocket-Key": base64.b64encode(os.urandom(16)).decode(),
                                    "Authorization": "Bearer " + token})
    status = c.getresponse().status
    c.close()
    return status


def hello(m, room, token=None):
    return http("GET", "/v1/rooms/%s/hello" % room, token=token or m.seat(room))


def member(m, room, name):
    s, h = hello(m, room)
    return next((x for x in h.get("members", []) if x["name"] == name), None)


def door(path, method="POST", body=None, headers=None, timeout=70):
    """One request to the remote MCP door: (status, parsed JSON or None, raw body)."""
    c = httpclient.HTTPConnection("127.0.0.1", PORT, timeout=timeout)
    data = None if body is None else (body if isinstance(body, str) else json.dumps(body))
    h = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
    h.update(headers or {})
    c.request(method, path, body=data, headers=h)
    r = c.getresponse()
    raw = r.read().decode()
    c.close()
    try:
        return r.status, json.loads(raw), raw
    except ValueError:
        return r.status, None, raw


class Remote:
    """A member that is not a machine, through its connector address only."""

    def __init__(self, url):
        self.path = url[len(URL):]
        self.n = 0

    def rpc(self, method, params=None, version="2025-06-18"):
        self.n += 1
        return door(self.path, body={"jsonrpc": "2.0", "id": self.n, "method": method, "params": params or {}},
                    headers={"MCP-Protocol-Version": version})

    def tool(self, name, args=None):
        s, j, raw = self.rpc("tools/call", {"name": name, "arguments": args or {}})
        r = (j or {}).get("result") or {}
        return (r.get("content") or [{}])[0].get("text", raw), bool(r.get("isError"))


def until(pred, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.2)
    return pred()


def ref():
    return os.urandom(8).hex()


def start_worker(admin_hash):
    log = open(os.path.join(RUN, "wrangler.log"), "w")
    w = subprocess.Popen(["npx", "--yes", "wrangler@4", "dev", "--local", "--ip", "127.0.0.1", "--port", str(PORT),
                          "--var", "ADMIN:" + admin_hash, "--var", "INVITE_SECONDS:%d" % INVITE_SECONDS,
                          "--persist-to", os.path.join(RUN, "wrangler-state"), "--show-interactive-dev-session=false"],
                         cwd=os.path.join(ROOT, "worker"), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                         stdin=subprocess.DEVNULL, env=dict(os.environ, WRANGLER_SEND_METRICS="false"))
    print("wrangler dev started, pid %d" % w.pid, flush=True)
    ready = threading.Event()

    def pump():
        for line in w.stdout:
            log.write(line)
            log.flush()
            if "Ready on" in line:
                ready.set()
    threading.Thread(target=pump, daemon=True).start()
    if not ready.wait(120):
        print("wrangler dev did not become ready; see %s" % os.path.join(RUN, "wrangler.log"))
        stop_worker(w.pid, w)
        sys.exit(2)
    return w


def stop_worker(pid, w):
    print("stopping wrangler dev, pid %d" % pid, flush=True)
    os.kill(pid, signal.SIGTERM)
    try:
        w.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.kill(pid, signal.SIGKILL)
        w.wait(timeout=10)


def main():
    os.makedirs(RUN, exist_ok=True)
    A, B, C, D, E, X = (Machine(n) for n in ("a", "b", "c", "d", "e", "x"))
    rc, admin_hash, _ = A.cli("admin-key")
    check("admin-key prints a sha256 and keeps the key 0600",
          rc == 0 and re.fullmatch(r"[0-9a-f]{64}", admin_hash)
          and stat.S_IMODE(os.stat(os.path.join(A.config, "admin")).st_mode) == 0o600, admin_hash)
    with open(os.path.join(A.config, "admin")) as fh:
        admin_token = fh.read().strip()
    w = start_worker(admin_hash)
    pid = w.pid
    sessions = []
    try:
        run_checks(A, B, C, D, E, X, admin_token, sessions)
    finally:
        for s in sessions:
            try:
                s.stop()
            except Exception:  # noqa: BLE001 — a session already gone
                pass
        stop_worker(pid, w)
    print("%d/%d passed" % (sum(results), len(results)))
    sys.exit(0 if all(results) else 1)


def run_checks(A, B, C, D, E, X, admin_token, sessions):
    def session(m, **kw):
        s = Session(m, **kw)
        sessions.append(s)
        return s

    # ---- rooms, seats, invitations --------------------------------------------------------------------
    rc, out, err = B.cli("create", "r1", "--url", URL)
    check("create needs the admin key", rc != 0 and "admin key" in err, err)
    with open(os.path.join(B.config, "admin"), "w") as fh:
        fh.write("not-the-admin-key\n")
    rc, out, err = B.cli("create", "r1", "--url", URL)
    check("create with a wrong admin key is refused", rc != 0 and "admin key" in err, err)
    rc, out, err = A.cli("create", "r1", "--name", "a", "--url", URL)
    mode = stat.S_IMODE(os.stat(os.path.join(A.config, "seats", "r1.json")).st_mode) if rc == 0 else 0
    check("create with the admin key makes the creator owner, seat file 0600", rc == 0 and mode == 0o600, out + err)
    s, h = hello(A, "r1")
    check("the relay knows the owner", s == 200 and h.get("role") == "owner" and h.get("name") == "a", h)
    seat = hashlib.sha256(b"another seat").hexdigest()
    s, r = http("POST", "/v1/rooms/r1/create", token=admin_token, body={"seat": seat, "name": "z"})
    check("create twice is refused", s == 409 and "already exists" in r.get("error", ""), r)
    rc, out, err = A.cli("create", "r1", "--url", URL)
    check("a second seat for the same room on one machine is refused", rc != 0 and "already has a seat" in err, err)

    rc, link, err = A.cli("invite", "r1")
    check("an owner invites: one link", rc == 0 and re.fullmatch(re.escape(URL) + r"/j/r1/[A-Za-z0-9_-]+", link), link + err)
    rc, out, err = B.cli("accept", link, "--name", "relay")
    check("the name relay cannot be taken", rc != 0 and "reserved" in err, err)
    rc, out, err = B.cli("accept", link, "--name", "b")
    check("accept takes a seat and records the relay url",
          rc == 0 and os.path.exists(os.path.join(B.config, "seats", "r1.json"))
          and json.load(open(os.path.join(B.config, "config.json"))).get("url") == URL, out + err)
    rc, out, err = C.cli("accept", link, "--name", "c")
    check("an invitation is used once", rc != 0 and "not valid" in err, err)
    rc, link2, err = B.cli("invite", "r1")
    check("a plain member can invite", rc == 0 and "/j/r1/" in link2, link2 + err)
    rc, out, err = C.cli("accept", link2, "--name", "b")
    check("a name already in the room is refused", rc != 0 and "taken" in err, err)
    rc, out, err = C.cli("accept", link2, "--name", "c")
    check("the code survives a refused name and is accepted with another", rc == 0, out + err)
    # X holds a seat file for r1 that the room never accepted.
    os.makedirs(os.path.join(X.config, "seats"), exist_ok=True)
    json.dump({"room": "r1", "token": "made-up-token"}, open(os.path.join(X.config, "seats", "r1.json"), "w"))
    X.set_url(URL)
    rc, out, err = X.cli("invite", "r1")
    check("a non-member cannot invite", rc != 0 and "not a member" in err, err)
    check("a seat the relay does not know is forgotten: its seat file is deleted, and the command says so",
          not os.path.exists(os.path.join(X.config, "seats", "r1.json")) and "seat file on this machine was deleted" in err, err)
    rc, late_link, err = A.cli("invite", "r1")  # used once it has expired, at the end
    late_at = time.time()
    rc, out, err = B.cli("rename", "r1", "relay")
    check("rename to relay is refused", rc != 0 and "reserved" in err, err)

    # ---- sessions -----------------------------------------------------------------------------------------
    rc, out, err = A.cli("join", "r1")
    check("join with no running session edits projects.json for the next start",
          rc == 0 and "next start" in out and A.projects().get(A.proj) == ["r1"], out + err)
    B.cli("join", "r1")
    C.cli("join", "r1")
    sa = session(A, wrap=True, tag="-a1")  # its parent is a shell: commands find it as the only control socket
    check("the tools are offered with no environment switch, and the channel capability as the fallback",
          "claude/channel" in sa.init["result"]["capabilities"]["experimental"] and "tools" in sa.init["result"]["capabilities"])
    sb = session(B)
    sc = session(C, tag="-c1")
    joins = sa.drain(2.5)
    texts = [e["content"] for e in joins if e["meta"].get("from") == "relay"]
    want = ["a created an invitation (valid %d s)" % INVITE_SECONDS, "b joined the room, invited by a",
            "b created an invitation (valid %d s)" % INVITE_SECONDS, "c joined the room, invited by b"]
    check("the mint and the join are announced to every member, including one that was offline",
          all(t in texts for t in want) and all(e["meta"].get("room") == "r1" for e in joins), texts)
    btexts = [e["content"] for e in sb.drain(0.2)]
    check("a member that joined later hears what happened after it joined",
          "c joined the room, invited by b" in btexts and "b joined the room, invited by a" in btexts, btexts)
    sc.drain(0.2)
    until(lambda: (member(A, "r1", "c") or {}).get("connected"), 10)

    t0 = time.time()
    txt, err = sa.send("b", "hello b")
    ev = sb.text("hello b")
    check("push delivery a → b", ev and ev["meta"]["from"] == "a" and ev["meta"]["room"] == "r1"
          and ev["meta"]["msg_id"] and ev["meta"]["sent_at"], "%.2f s, %s" % (time.time() - t0, txt))
    check("delivered once", sb.wait(lambda e: True, 1.5) is None)
    check("no inbox: the message arrives as a channel event, and the server said so once on stderr",
          ev and sb.stderr().count("no inbox socket") == 1, sb.stderr()[-200:])
    txt, err = sa.send("zz", "x")
    check("unknown recipient refused", err and "unknown recipient" in txt, txt)
    txt, err = sa.send("a", "x")
    check("a message to oneself refused", err, txt)
    txt, err = sa.send("all", "to everyone")
    check("all reaches every other member", sb.text("to everyone") and sc.text("to everyone") and sa.text("to everyone", 1) is None, txt)
    rc, out, err = A.cli("send", "b", "from", "the", "shell")
    check("relay send from a shell", rc == 0 and sb.text("from the shell"), out + err)

    # ---- the inbox socket ----------------------------------------------------------------------------------
    G = Machine("g")
    rc, glink, err = A.cli("invite", "r1")
    G.cli("accept", glink, "--name", "g")
    G.cli("join", "r1")
    ipath = os.path.join(G.dir, "inbox.sock")
    stuck = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stuck.bind(ipath)
    stuck.listen(0)
    plug = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    plug.connect(ipath)  # fills the backlog: the next writer waits to be accepted
    sg = session(G, inbox=ipath, tag="-g1")
    until(lambda: (member(A, "r1", "g") or {}).get("connected"), 10)
    time.sleep(1.5)
    early = sg.drain(0)
    sg.stop()
    plug.close()
    stuck.close()
    os.remove(ipath)
    until(lambda: not (member(A, "r1", "g") or {}).get("connected"), 10)
    inbox = Inbox(ipath)
    sg = session(G, inbox=ipath, tag="-g2")
    notice = inbox.wait(lambda l: "g joined the room, invited by a" in l)
    content = json.loads(notice)["message"]["content"] if notice else ""
    check("a message not yet written into the inbox is not acked: it comes again to the next session",
          early == [] and notice is not None, early)
    check("inbox: the exact line, with the header and no reply line for a notice",
          notice == exact_line(content) and re.fullmatch(
              r"\[relay · room r1 · from relay · #\d+ · \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\]\ng joined the room, invited by a", content),
          notice)
    txt, err = sa.send("g", "hello g")
    line = inbox.wait(lambda l: "hello g" in l)
    content = json.loads(line)["message"]["content"] if line else ""
    check("inbox: a member's message, with the reply line",
          line == exact_line(content) and re.fullmatch(
              r'\[relay · room r1 · from a · #\d+ · [0-9TZ:-]+\]\nhello g\n\(reply with the relay send tool, to "r1:a"\)', content)
          and sg.drain(0.5) == [], line)
    sg.stop()
    until(lambda: not (member(A, "r1", "g") or {}).get("connected"), 10)
    count = len(inbox.lines)
    sg = session(G, inbox=ipath, tag="-g3")
    until(lambda: (member(A, "r1", "g") or {}).get("connected"), 10)
    time.sleep(1.5)
    check("written into the inbox, then acked: not delivered again", len(inbox.lines) == count, inbox.lines[count:])
    sg.stop()
    inbox.close()

    sc.stop()
    txt, err = sa.send("c", "while c is away")
    sc = session(C, tag="-c2")
    ev = sc.text("while c is away")
    check("offline delivery when the member comes back", ev and ev["meta"]["from"] == "a", txt)
    sc.stop()
    sc = session(C, tag="-c3")
    check("after a restart, no duplicate", sc.wait(lambda e: e["content"] == "while c is away", 3) is None)

    txt, err = sb.send("a", "réponse — accents ✓ " + "x" * 3000)
    ev = sa.wait(lambda e: e["content"].startswith("réponse — accents ✓"))
    check("long text with non-ASCII", ev and len(ev["content"]) > 3000, txt)
    txt, err = sa.send("b", "y" * (49 * 1024))
    check("a message over 48 KiB is refused", err and "over" in txt, txt[:80])

    # queued while the relay cannot be reached, delivered when it can
    sb.stop()
    B.set_url(DEAD_URL)
    sb = session(B, tag="-b2")
    time.sleep(0.5)
    txt, err = sb.send("a", "written while the relay was away")
    check("unreachable relay: queued, not lost", not err and txt.startswith("QUEUED"), txt[:80])
    B.set_url(URL)
    ev = sa.text("written while the relay was away", 45)
    check("the queued message leaves when the link is back", ev and ev["meta"]["from"] == "b")

    # ---- names --------------------------------------------------------------------------------------------
    rc, out, err = B.cli("rename", "bee")
    ev = sa.text("b is now named bee")
    check("rename (room optional with one seat) is announced at once", rc == 0 and ev and ev["meta"]["from"] == "relay", out + err)
    time.sleep(0.3)
    txt, err = sa.send("bee", "to the new name")
    check("the new name is reachable at once, no restart", not err and sb.text("to the new name"), txt)
    txt, err = sa.send("b", "to the old name")
    check("the old name no longer exists", err, txt)
    txt, err = sb.send("a", "from the renamed one")
    ev = sa.text("from the renamed one")
    check("messages carry the new name", ev and ev["meta"]["from"] == "bee", ev and ev["meta"])
    rc, out, err = B.cli("rename", "r1", "a")
    check("a name another member holds is refused", rc != 0, err)
    B.cli("rename", "r1", "b")
    sa.text("bee is now named b")
    time.sleep(0.3)

    # ---- two rooms ------------------------------------------------------------------------------------------
    A.cli("create", "r2", "--name", "a")
    rc, link3, err = A.cli("invite", "r2")
    C.cli("accept", link3, "--name", "c")
    rc, out, err = C.cli("join", "r2")  # sc is this test's child: found by walking the ancestors
    ev = sc.wait(lambda e: e["meta"].get("room") == "r2" and e["content"] == "c joined the room, invited by a")
    check("join through the control socket (found by the ancestors): the running session listens at once, projects.json updated",
          rc == 0 and "now listens" in out and ev and C.projects().get(C.proj) == ["r1", "r2"], out + err)
    rc, out, err = A.cli("join", "r2")  # sa's parent is a shell: found as the only control socket of A
    check("join finds the only session of the machine", rc == 0 and "now listens to r2" in out, out + err)
    sa.drain(1)
    sc.drain(0.1)
    txt, err = sa.send("c", "which room?")
    check("in two rooms, send without a room is refused", err and "room:name" in txt, txt)
    txt, err = sa.send("r2:c", "in r2")
    ev = sc.text("in r2")
    check("room:name works, the event is tagged with its room", not err and ev and ev["meta"]["room"] == "r2", txt)
    txt, err = sa.send("c", "in r1 by field", room="r1")
    ev = sc.text("in r1 by field")
    check("the room field works too", not err and ev and ev["meta"]["room"] == "r1", txt)
    txt, err = sb.send("r2:a", "not my room")
    check("a session cannot send into a room it has no seat in", err and "no seat" in txt, txt)
    s, r = http("POST", "/v1/rooms/r2/send", token=B.seat("r1"), body={"ref": ref(), "to": "a", "text": "x"})
    check("a member of r1 cannot send into r2", s == 403, r)
    check("a member of r1 cannot connect to r2", ws_status("/v1/rooms/r2/connect", B.seat("r1")) == 403)
    s, r = http("POST", "/v1/rooms/r2/send", token=C.seat("r2"), body={"ref": ref(), "to": "b", "text": "x"})
    check("a recipient outside the room is refused", s == 400 and "unknown recipient" in r.get("error", ""), r)

    rc, out, err = C.cli("leave", "r2")
    sa.send("r2:c", "after leave")
    check("leave through the control socket stops the room at once, projects.json updated",
          rc == 0 and sc.wait(lambda e: e["meta"].get("room") == "r2", 2) is None and C.projects().get(C.proj) == ["r1"], out + err)
    C.cli("join", "r2")
    check("joining again hands over what waited", sc.text("after leave"))

    # ---- list and status --------------------------------------------------------------------------------------
    A.cli("leave", "r2")
    until(lambda: not (member(A, "r2", "a") or {}).get("connected"), 10)
    txt, err = sa.send("r2:c", "from a session not listening")
    check("a session not listening to a room still sends there, and takes nothing",
          not err and txt.startswith("sent in r2") and sc.text("from a session not listening")
          and not (member(A, "r2", "a") or {}).get("connected"), txt)
    stale = os.path.join(A.state, "ctl", "4194301.sock")  # a pid that does not run
    open(stale, "w").close()
    rc, out, err = A.cli("list")
    check("a stale control socket is ignored and removed", rc == 0 and not os.path.exists(stale), out + err)
    row = r"  • [a-z0-9-]+( \(you\))? — (connected( · [^ ]+)*|not connected)"
    lines = out.split("\n")
    check("list shape", rc == 0 and lines[0] == "r1" and "r2 (not joined)" in lines
          and all(re.fullmatch(row, l) for l in lines if l not in ("r1", "r2 (not joined)"))
          and re.fullmatch(r"  • a \(you\) — connected( · [^ ]+)+", lines[1])
          and "  • a (you) — not connected" in lines[lines.index("r2 (not joined)"):], out)
    rc, out2, err = A.cli("status")
    check("status shows the joined rooms only", rc == 0 and out2.split("\n")[0] == "r1" and "r2" not in out2
          and all(re.fullmatch(row, l) for l in out2.split("\n")[1:]), out2)
    rc, out3, err = A.cli("cmd")
    check("cmd with nothing is status", rc == 0 and out3 == out2, out3)
    refused = [A.cli("cmd", v) for v in ("serve", "admin-key")]
    check("cmd refuses serve and admin-key", all(r[0] != 0 and "not available" in r[2] for r in refused), refused[0][2])
    os.makedirs(os.path.join(D.config, "seats"), exist_ok=True)
    json.dump({"room": "r1", "token": "whatever"}, open(os.path.join(D.config, "seats", "r1.json"), "w"))
    D.set_url(DEAD_URL)
    rc, out, err = D.cli("list")
    check("a room that cannot be reached", rc == 0 and out.startswith("r1\n  (cannot reach the relay: "), out)

    # ---- forged headers ----------------------------------------------------------------------------------
    a_hash = hashlib.sha256(A.seat("r1").encode()).hexdigest()
    forged = {"X-Relay-Admin": "1", "X-Relay-Seat": a_hash, "X-Relay-Room": "r1", "X-Relay-Invite": "x"}
    s1, r1 = http("POST", "/v1/rooms/r9/create", token=C.seat("r1"), body={"seat": a_hash, "name": "c"}, headers=forged)
    s2, h2 = http("GET", "/v1/rooms/r1/hello", token=C.seat("r1"), headers=forged)
    s3, r3 = http("GET", "/v1/rooms/r1/hello", headers=forged)
    s4, r4 = http("POST", "/v1/rooms/r1/remove", token=C.seat("r1"), body={"name": "b"}, headers=forged)
    check("a forged X-Relay-* header changes nothing",
          s1 == 403 and s2 == 200 and h2.get("name") == "c" and s3 == 401 and s4 == 403, (s1, s2, h2.get("name"), s3, s4))

    # ---- guardrails -------------------------------------------------------------------------------------------
    A.cli("create", "r3", "--name", "a")
    rc, link4, err = A.cli("invite", "r3")
    B.cli("accept", link4, "--name", "b")
    rc, link5, err = A.cli("invite", "r3")
    C.cli("accept", link5, "--name", "c")
    B.cli("join", "r3")
    sb.drain(1)
    answers = [http("POST", "/v1/rooms/r3/send", token=A.seat("r3"), body={"ref": ref(), "to": "all", "text": "n%d" % i})
               for i in range(31)]
    one = http("POST", "/v1/rooms/r3/send", token=A.seat("r3"), body={"ref": ref(), "to": "b", "text": "one more"})
    check("at most 60 per sender per minute, a message to all counting one per recipient",
          [s for s, _ in answers[:30]] == [200] * 30 and answers[30][0] == 400
          and answers[30][1].get("error") == "too many messages this minute" and one[0] == 400, (answers[30], one))
    tok = C.seat("r1")
    s1, r1 = http("POST", "/v1/rooms/r1/send", token=tok, body={"ref": ref(), "to": "b", "text": "same text"})
    s2, r2 = http("POST", "/v1/rooms/r1/send", token=tok, body={"ref": ref(), "to": "b", "text": "same text"})
    got = [e for e in sb.drain(2) if e["content"] == "same text"]
    check("the same text to the same target within 10 s is stored once and answered as a duplicate",
          s1 == 200 and s2 == 200 and not r1.get("duplicate") and r2.get("duplicate") is True and len(got) == 1, (r2, len(got)))
    rc1, out1, _ = C.cli("send", "r1:b", "said", "twice")
    rc2, out2, _ = C.cli("send", "r1:b", "said", "twice")
    check("the client says a duplicate was not sent twice", rc1 == 0 and out1.startswith("sent in r1")
          and rc2 == 0 and "identical to your message of a few seconds ago — not sent twice" in out2, out2)

    # ---- the remote door (prototype) ------------------------------------------------------------------------
    A.cli("create", "r4", "--name", "a")
    rc, out, err = A.cli("remote-seat", "r4", "voice")
    upath = os.path.join(A.config, "remote", "r4-voice.url")
    url = open(upath).read().strip() if os.path.exists(upath) else ""
    tok = url.rsplit("/", 1)[-1]
    check("remote-seat writes the connector address to a 0600 file in a 0700 folder and prints only its path",
          rc == 0 and re.fullmatch(re.escape(URL) + r"/mcp/r4/[A-Za-z0-9_-]{40,}", url)
          and stat.S_IMODE(os.stat(upath).st_mode) == 0o600
          and stat.S_IMODE(os.stat(os.path.dirname(upath)).st_mode) == 0o700
          and upath in out and "voice" in out and "relay remove r4 voice" in out
          and tok not in out + err and "/mcp/" not in out + err, out + err)
    rc, out, err = B.cli("remote-seat", "r3", "nope")
    check("remote-seat needs an owner", rc != 0 and "only an owner" in err, err)
    A.cli("remote-seat", "r4", "voice2")
    v = Remote(url)
    v2 = Remote(open(os.path.join(A.config, "remote", "r4-voice2.url")).read().strip())
    # A seat alone in a quiet room, for a read asking more than the cap (it runs while the checks below do).
    A.cli("create", "r5", "--name", "a")
    A.cli("remote-seat", "r5", "voice3")
    v3 = Remote(open(os.path.join(A.config, "remote", "r5-voice3.url")).read().strip())
    v3.tool("read", {"wait_seconds": 0})  # the room's notices so far
    capped = {}

    def long_read():
        t = time.time()
        capped["r"] = v3.tool("read", {"wait_seconds": 40})
        capped["took"] = time.time() - t
    capped_th = threading.Thread(target=long_read)
    capped_th.start()
    s, j, raw = v.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                      "clientInfo": {"name": "t", "version": "0"}})
    res = (j or {}).get("result") or {}
    s2, j2, _ = v.rpc("initialize", {"protocolVersion": "1900-01-01"})
    check("door: initialize echoes a supported version, else answers its latest; tools capability",
          s == 200 and res.get("protocolVersion") == "2025-06-18" and res.get("capabilities") == {"tools": {}}
          and res.get("serverInfo", {}).get("name") == "relay" and j2["result"]["protocolVersion"] == "2025-11-25", raw)
    s, _, raw = door(v.path, body={"jsonrpc": "2.0", "method": "notifications/initialized"})
    check("door: a notification is 202 with no body", s == 202 and raw == "", (s, raw))
    s, j, raw = v.rpc("tools/list")
    read_tool = next((t for t in (j or {}).get("result", {}).get("tools", []) if t["name"] == "read"), {})
    check("door: tools/list offers send, status, read and news; read's wait is at most 25 s",
          s == 200 and [t["name"] for t in j["result"]["tools"]] == ["send", "status", "read", "news"]
          and read_tool["inputSchema"]["properties"]["wait_seconds"]["maximum"] == 25, raw[:200])
    s, j, _ = v.rpc("ping")
    s3, j3, _ = v.rpc("resources/list")
    s4, _, _ = door(v.path, method="GET")
    s5, j5, _ = door(v.path, body=[{"jsonrpc": "2.0", "id": 1, "method": "ping"}])
    s6, _, _ = v.rpc("ping", version="2099-01-01")
    check("door: ping; an unknown method is -32601; GET is 405; a batch is 400; an unknown version is still served",
          j.get("result") == {} and j3["error"]["code"] == -32601 and s4 == 405 and s5 == 400 and s6 == 200,
          (j, j3, s4, s5, s6))
    s, j, raw = door("/mcp/r4/" + "x" * 43, body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    s2, _, raw2 = door(v.path, body={"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers={"Origin": "https://example.com"})
    check("door: a wrong token is 403 with no detail; with no origin list an Origin is not refused",
          s == 403 and j == {"error": "forbidden"} and s2 == 200, (s, raw, s2))
    st, err = v.tool("status")
    check("door: status lists the members, never a token or a hash",
          not err and "• a (owner) — not connected" in st and "• voice (member, you) —" in st
          and tok not in st and not re.search(r"[0-9a-f]{64}", st), st.replace("\n", " | "))
    A.cli("join", "r4")
    until(lambda: (member(A, "r4", "a") or {}).get("connected"), 10)
    txt, err = v.tool("send", {"to": "a", "text": "hello from the voice"})
    ev = sa.text("hello from the voice")
    check("door: send reaches a machine's session, from the seat's name", not err and txt == "sent to a."
          and ev and ev["meta"]["from"] == "voice" and ev["meta"]["room"] == "r4", txt)
    txt2, err2 = v.tool("send", {"to": "a", "text": "hello from the voice"})
    txt3, err3 = v.tool("send", {"to": "nobody", "text": "x"})
    check("door: the same duplicate rule and refusals as the other roads",
          not err2 and "not sent twice" in txt2 and err3 and "unknown recipient" in txt3, (txt2, txt3))
    v.tool("read", {"wait_seconds": 0})  # the room's notices so far
    sa.send("r4:voice", "for the voice")
    first, _ = v.tool("read", {"wait_seconds": 0})
    second, _ = v.tool("read", {"wait_seconds": 0})
    again, _ = v.tool("read", {"again": True})
    check("door: read returns a waiting message once, then not again, and again with again:true",
          "[from a · " in first and "for the voice" in first and "SAY:" not in first and second == "no new message"
          and again.startswith("again: ") and "for the voice" in again, (first, second, again))
    t0 = time.time()
    none, _ = v.tool("read", {"wait_seconds": 2})
    took = time.time() - t0
    check("door: read with nothing waiting returns after the wait", none == "no new message" and 1.8 <= took < 6, took)
    got = {}
    th = threading.Thread(target=lambda: got.update(r=v2.tool("read", {"wait_seconds": 20})))
    v2.tool("read", {"wait_seconds": 0})
    t0 = time.time()
    th.start()
    time.sleep(1)
    v.tool("send", {"to": "voice2", "text": "between two voices"})
    th.join(25)
    took = time.time() - t0
    check("door: a waiting read returns as soon as a message arrives (voice → voice2)",
          "between two voices" in got.get("r", ("",))[0] and "[from voice · " in got["r"][0] and took < 8, (took, got))

    # ---- a line to say aloud, news, the wait cap, the log lines -------------------------------------------
    time.sleep(MCP_WINDOW)  # a fresh window of the door's limit for the checks below
    sa.drain(0.5)
    txt, err = v.tool("send", {"to": "a", "say": "The build is green.", "text": "All 42 tests passed\non both boxes."})
    ev = sa.text("The build is green.\n\nAll 42 tests passed\non both boxes.")
    check("say + detail, door → machine: a client that knows nothing of say shows the sentence, a blank line, the detail",
          not err and ev and ev["meta"]["from"] == "voice", (txt, sa.drain(0)))
    txt, err = sa.tool("send", {"to": "r4:voice", "say": "Done, merged.", "text": "Branch x landed at abc123."})
    first, _ = v.tool("read", {"wait_seconds": 5})
    again, _ = v.tool("read", {"again": True})
    check("say + detail, machine (tool) → door: read gives the SAY line, then the DETAIL",
          not err and re.search(r"\[from a · [^\]]+\]\nSAY: Done, merged\.\nDETAIL: Branch x landed at abc123\.", first)
          and "SAY: Done, merged." in again, (txt, first))
    rc, out, err = A.cli("send", "r4:voice", "--say", "From  the\nshell.", "more", "words")
    got, _ = v.tool("read", {"wait_seconds": 5})
    check("say + detail, machine (relay send --say) → door; the say-line is one line",
          rc == 0 and "SAY: From the shell.\nDETAIL: more words" in got, (out, err, got))
    txt, err = sa.tool("send", {"to": "r4:voice", "say": "Only this.", "text": ""})
    got, _ = v.tool("read", {"wait_seconds": 5})
    txt2, err2 = v.tool("send", {"to": "a", "say": "x" * 501, "text": "y"})
    txt3, err3 = v.tool("send", {"to": "a", "text": ""})
    check("a say-line alone is a message; a say-line over 500 characters and an empty message are refused",
          not err and re.search(r"\]\nSAY: Only this\.\n\n\(answer", got) and "DETAIL" not in got
          and err2 and "over 500" in txt2 and err3 and "empty message" in txt3, (got, txt2, txt3))
    sa.send("r4:voice", "plain " + "z" * 200)
    sa.tool("send", {"to": "r4:voice", "say": "The newest one.", "text": "its detail"})
    news, err = v.tool("news")
    news2, _ = v.tool("news")
    got, _ = v.tool("read", {"wait_seconds": 0})
    check("news: how many, from whom, oldest and newest time, the newest say-line, who is connected — and takes nothing",
          not err and news.startswith("2 messages waiting, from a (2)\noldest 20") and ", newest 20" in news
          and "newest, from a: SAY: The newest one." in news and "connected now: a" in news and news2 == news
          and "plain zzz" in got and "SAY: The newest one.\nDETAIL: its detail" in got, (news, got))
    sa.send("r4:voice", "w" * 300)
    news, _ = v.tool("news")
    v.tool("read", {"wait_seconds": 0})
    none, _ = v.tool("news")
    check("news without a say-line shows the first 120 characters; with nothing waiting it says so",
          ("newest, from a: " + "w" * 120 + "…\n") in news and none.startswith("no message waiting\nconnected now: "),
          (news, none))
    capped_th.join(35)
    check("a read asking 40 s waits 25 s (clamped, not refused)",
          capped.get("r") == ("no new message", False) and 24 <= capped.get("took", 0) < 29, capped)

    m = member(A, "r4", "voice") or {}
    rc, out, err = A.cli("list")
    check("hello marks a door seat pull and not connected; list says it reads by itself",
          m.get("pull") is True and m.get("connected") is False and "• voice — reads by itself (remote door)" in out
          and "pull" not in (member(A, "r4", "a") or {}), (m, out))
    time.sleep(MCP_WINDOW)  # the window of the limit, emptied of the reads above
    calls = [v2.tool("status")[1] for _ in range(20)]
    check("door: a per-seat limit on tool calls", calls[:15] == [False] * 15 and any(calls[15:]), calls)
    rc, out, err = A.cli("remove", "r4", "voice")
    s, j, _ = v.rpc("tools/list")
    check("relay remove revokes the door at once", rc == 0 and s == 403, (out, s))
    A.cli("leave", "r4")
    time.sleep(1)  # the last lines reach the dev server's output
    logs = []
    for l in open(os.path.join(RUN, "wrangler.log"), encoding="utf-8"):
        i = l.find('{"t":')
        if i >= 0 and '"door":"mcp"' in l:
            try:
                logs.append(json.loads(l[i:].strip()))
            except ValueError:
                pass
    toks = [tok, v2.path.rsplit("/", 1)[-1], v3.path.rsplit("/", 1)[-1]]
    raw_logs = json.dumps(logs)
    capped_line = next((x for x in logs if x.get("tool") == "read" and x.get("wait_asked") == 40), {})
    check("one log line per door request: time, method, tool, room, name, status, duration; read's wait and outcome",
          len(logs) >= 40 and all(set(x) >= {"t", "door", "room", "status", "ms"} for x in logs)
          and capped_line.get("wait") == 25 and capped_line.get("returned") is False and capped_line.get("name") == "voice3"
          and capped_line.get("room") == "r5" and capped_line.get("ms", 0) >= 24000
          and any(x.get("tool") == "read" and x.get("returned") is True and x.get("count", 0) >= 1 for x in logs)
          and any(x.get("status") == 403 and "name" not in x for x in logs)
          and any(x.get("method") == "GET" and x.get("status") == 405 for x in logs), (len(logs), capped_line))
    check("the log lines carry no token, no seat hash and no message text",
          not any(t in raw_logs for t in toks) and not re.search(r"[0-9a-f]{64}", raw_logs)
          and not any(w in raw_logs for w in ("hello from the voice", "for the voice", "Done, merged", "The newest one",
                                              "build is green", "between two voices")), raw_logs[:300])

    # ---- seat exclusivity --------------------------------------------------------------------------------
    sa2 = session(A, tag="-a2")  # same directory as sa; this test's child, so A's commands find it by their ancestors
    until(lambda: "held by another session" in sa2.stderr(), 10)
    held, _ = sa2.tool("status", {})
    txt, err = sb.send("r1:a", "still the first")
    check("two sessions in one directory: the second stays out of the held seat and says so in status",
          "(held by another session — `relay join r1` takes it)" in held and sa.text("still the first")
          and sa2.stderr().count("held by another session") == 1 and "taken over" not in sa.stderr(), held.replace("\n", " | "))
    rc, out, err = A.cli("join", "r1")
    until(lambda: "taken over" in sa.stderr(), 10)
    st2, _ = sa2.tool("status", {})
    check("…and takes it only on relay join", rc == 0 and "now listens to r1" in out and "held" not in st2, out + st2)
    sa2.drain(1)
    time.sleep(4)  # long enough for a reconnect, had there been one
    txt, err = sb.send("r1:a", "to the newer session")
    st, _ = sa.tool("status", {})
    st2, _ = sa2.tool("status", {})
    check("an explicit join takes the seat over; the first session does not come back",
          sa2.text("to the newer session") and sa.wait(lambda e: e["content"] == "to the newer session", 1) is None
          and "this seat was taken over by another session" in st and "taken over" not in st2, st.replace("\n", " | "))
    sa.stop()

    # ---- remove and leave-room ------------------------------------------------------------------------
    rc, out, err = B.cli("remove", "r1", "c")
    check("only an owner removes", rc != 0 and "only an owner" in err, err)
    tok = C.seat("r1")
    rc, out, err = A.cli("remove", "r1", "c")
    ev = sa2.text("c was removed by a")
    s, r = hello(C, "r1", token=tok)
    check("remove stops the seat at once, and is announced", rc == 0 and ev and s == 403
          and until(lambda: "no longer a member" in sc.stderr(), 5), out + err)
    check("the removed machine deletes its seat file and drops the room from projects.json, and says so once",
          until(lambda: not os.path.exists(os.path.join(C.config, "seats", "r1.json")), 5)
          and "r1" not in C.projects().get(C.proj, []) and sc.stderr().count("seat file on this machine was deleted") == 1,
          C.projects())
    txt, err = sb.send("r1:c", "to a removed member")
    check("a removed member is no longer a recipient", err, txt)
    rc, out, err = A.cli("leave-room", "r1")
    check("the last owner cannot leave", rc != 0 and "last owner" in err, err)
    rc, out, err = B.cli("leave-room", "r1")
    ev = sa2.text("b left the room")
    check("leave-room gives the seat up, deletes the seat file, and is announced",
          rc == 0 and ev and not os.path.exists(os.path.join(B.config, "seats", "r1.json")), out + err)

    # ---- inert on a machine with no seat -------------------------------------------------------------------
    sb.stop()
    sa2.stop()
    H = Machine("h")
    n = session(H, tag="-inert")
    time.sleep(1)
    tools = n.call("tools/list")["result"]["tools"]
    check("a machine with no seat: no tool, no connection, no control socket",
          tools == [] and "tools" not in n.init["result"]["capabilities"] and H.sockets() == [], tools)
    n.stop()

    # ---- invitation expiry ---------------------------------------------------------------------------
    time.sleep(max(0, INVITE_SECONDS + 1 - (time.time() - late_at)))
    E.set_url(URL)
    rc, out, err = E.cli("accept", late_link, "--name", "e")
    check("an expired invitation is refused", rc != 0 and "expired" in err, err)
    # ---- the link in the bin folder, help, the command module ----------------------------------------------
    me = os.path.realpath(PROG)
    link = os.path.join(H.bin, "relay")
    n = session(H, tag="-link1")
    n.stop()
    check("serve links the bin folder's relay to itself", os.path.islink(link) and os.path.realpath(link) == me, link)
    os.remove(link)
    os.symlink("/nonexistent/relay", link)
    n = session(H, tag="-link2")
    n.stop()
    check("serve repairs a link that points elsewhere", os.path.islink(link) and os.path.realpath(link) == me)
    os.remove(link)
    with open(link, "w") as fh:
        fh.write("#!/bin/sh\necho an older copy\n")
    n = session(H, tag="-link3")
    n.stop()
    check("serve replaces an older regular copy with the link, and says so once",
          os.path.islink(link) and os.path.realpath(link) == me and n.stderr().count("replaced the copy") == 1, n.stderr()[-200:])
    rc, out, err = H.cli("cmd", "help")
    lines = out.split("\n")
    verbs = ["status", "list", "join", "leave", "send", "rename", "create", "invite", "accept", "remove", "leave-room",
             "remote-seat", "help"]
    check("cmd help: one line per command, each with what it does",
          rc == 0 and [l.split(" ")[0] for l in lines] == verbs and all(" — " in l for l in lines), out)
    rc2, out2, _ = H.cli("cmd", "frobnicate")
    rc3, out3, err3 = H.cli("cmd", "install")
    check("an unknown word answers with the help", rc2 == 0 and out2 == out and rc3 == 0 and out3 == out, out2[:60])
    env = H.env()
    env["HOME"] = os.path.join(H.dir, "home-without-link")
    os.makedirs(env["HOME"], exist_ok=True)
    said = module_answer(env, "status")
    check("the command module, with no link yet, answers its one sentence",
          said == "\nthe relay program is not linked yet: start one session with the plugin enabled", said)
    env = H.env()
    os.makedirs(os.path.join(H.home, ".local", "bin"), exist_ok=True)
    os.symlink(me, os.path.join(H.home, ".local", "bin", "relay"))
    said = module_answer(env, "help")
    check("the command module runs the linked program's cmd", said == "\n" + out, said[:80])


if __name__ == "__main__":
    main()
