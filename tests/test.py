#!/usr/bin/env python3
"""End-to-end checks of relay against a local Worker (wrangler dev --local), started and stopped here.

Each fake machine has its own config, state and home directories under tests/.run/<run>/; nothing outside the
repository is touched. Prints one PASS/FAIL line per check and a final count; exits non-zero on any failure.
"""
import base64, hashlib, json, os, queue, re, signal, socket, stat, subprocess, sys, threading, time
import http.client as httpclient
import urllib.error, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROG = os.path.join(ROOT, "relay")
PORT, DEAD = 28787, 28788
URL, DEAD_URL = "http://127.0.0.1:%d" % PORT, "http://127.0.0.1:%d" % DEAD
RUN = os.path.join(ROOT, "tests", ".run", time.strftime("%Y%m%d-%H%M%S"))
INVITE_SECONDS = 8
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
        for d in (self.config, self.state, self.home, self.proj):
            os.makedirs(d, exist_ok=True)
        self.proj = os.path.realpath(self.proj)

    def env(self):
        env = dict(os.environ, RELAY_CONFIG_DIR=self.config, RELAY_STATE_DIR=self.state, HOME=self.home)
        env.pop("RELAY_LEAD", None)
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

    def __init__(self, m, lead=True, wrap=False, tag=""):
        env = m.env()
        if lead:
            env["RELAY_LEAD"] = "1"
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
    sa = session(A)
    check("the handshake offers the channel and the tools",
          "claude/channel" in sa.init["result"]["capabilities"]["experimental"] and "tools" in sa.init["result"]["capabilities"])
    sb = session(B)
    sc = session(C, wrap=True, tag="-c1")
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
    txt, err = sa.send("zz", "x")
    check("unknown recipient refused", err and "unknown recipient" in txt, txt)
    txt, err = sa.send("a", "x")
    check("a message to oneself refused", err, txt)
    txt, err = sa.send("all", "to everyone")
    check("all reaches every other member", sb.text("to everyone") and sc.text("to everyone") and sa.text("to everyone", 1) is None, txt)
    rc, out, err = A.cli("send", "b", "from", "the", "shell")
    check("relay send from a shell", rc == 0 and sb.text("from the shell"), out + err)

    sc.stop()
    txt, err = sa.send("c", "while c is away")
    sc = session(C, wrap=True, tag="-c2")
    ev = sc.text("while c is away")
    check("offline delivery when the member comes back", ev and ev["meta"]["from"] == "a", txt)
    sc.stop()
    sc = session(C, wrap=True, tag="-c3")
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
    rc, out, err = C.cli("join", "r2")  # sc's parent is a shell: found as the only control socket of C
    ev = sc.wait(lambda e: e["meta"].get("room") == "r2" and e["content"] == "c joined the room, invited by a")
    check("join through the control socket: the running session listens at once, projects.json updated",
          rc == 0 and "now listens" in out and ev and C.projects().get(C.proj) == ["r1", "r2"], out + err)
    rc, out, err = A.cli("join", "r2")  # sa is this test's child: found by walking the ancestors
    check("join finds the session by its ancestors", rc == 0 and "now listens to r2" in out, out + err)
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
    check("a session cannot send into a room it does not listen to", err, txt)
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
    refused = [A.cli("cmd", v) for v in ("serve", "admin-key", "install")]
    check("cmd refuses serve, admin-key, install", all(r[0] != 0 and "not available" in r[2] for r in refused), refused[0][2])
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
    B.cli("join", "r3")
    sb.drain(1)
    answers = [http("POST", "/v1/rooms/r3/send", token=A.seat("r3"), body={"ref": ref(), "to": "b", "text": "n%d" % i})
               for i in range(61)]
    ok = [s for s, _ in answers[:60]] == [200] * 60
    check("at most 60 messages per sender per minute", ok and answers[60][0] == 400
          and answers[60][1].get("error") == "too many messages this minute", answers[60])
    tok = C.seat("r1")
    s1, r1 = http("POST", "/v1/rooms/r1/send", token=tok, body={"ref": ref(), "to": "b", "text": "same text"})
    s2, r2 = http("POST", "/v1/rooms/r1/send", token=tok, body={"ref": ref(), "to": "b", "text": "same text"})
    got = [e for e in sb.drain(2) if e["content"] == "same text"]
    check("the same text to the same target within 10 s is answered sent and stored once",
          s1 == 200 and s2 == 200 and r2.get("type") == "sent" and len(got) == 1, (s1, s2, len(got)))

    # ---- seat exclusivity --------------------------------------------------------------------------------
    sa2 = session(A, wrap=True, tag="-a2")
    until(lambda: "taken over" in sa.stderr(), 10)
    sa2.drain(1)
    time.sleep(4)  # long enough for a reconnect, had there been one
    txt, err = sb.send("r1:a", "to the newer session")
    st, _ = sa.tool("status", {})
    st2, _ = sa2.tool("status", {})
    check("a second receiving connection takes the seat over; the first does not come back",
          sa2.text("to the newer session") and sa.wait(lambda e: e["content"] == "to the newer session", 1) is None
          and "this seat was taken over by another session" in st and "taken over" not in st2, st.replace("\n", " | "))
    sa.stop()

    # ---- remove and leave-room ------------------------------------------------------------------------
    rc, out, err = B.cli("remove", "r1", "c")
    check("only an owner removes", rc != 0 and "only an owner" in err, err)
    rc, out, err = A.cli("remove", "r1", "c")
    ev = sa2.text("c was removed by a")
    s, r = hello(C, "r1")
    check("remove stops the seat at once, and is announced", rc == 0 and ev and s == 403
          and until(lambda: "no longer a member" in sc.stderr(), 5), out + err)
    txt, err = sb.send("r1:c", "to a removed member")
    check("a removed member is no longer a recipient", err, txt)
    rc, out, err = A.cli("leave-room", "r1")
    check("the last owner cannot leave", rc != 0 and "last owner" in err, err)
    rc, out, err = B.cli("leave-room", "r1")
    ev = sa2.text("b left the room")
    check("leave-room gives the seat up, deletes the seat file, and is announced",
          rc == 0 and ev and not os.path.exists(os.path.join(B.config, "seats", "r1.json")), out + err)

    # ---- inert without RELAY_LEAD -----------------------------------------------------------------------
    sb.stop()
    sa2.stop()
    n = session(B, lead=False, tag="-inert")
    time.sleep(2)
    tools = n.call("tools/list")["result"]["tools"]
    check("no RELAY_LEAD: no tool, no connection, no control socket",
          tools == [] and "tools" not in n.init["result"]["capabilities"] and B.sockets() == []
          and not (member(A, "r3", "b") or {}).get("connected", True), tools)
    n.stop()

    # ---- invitation expiry, install ---------------------------------------------------------------------
    time.sleep(max(0, INVITE_SECONDS + 1 - (time.time() - late_at)))
    E.set_url(URL)
    rc, out, err = E.cli("accept", late_link, "--name", "e")
    check("an expired invitation is refused", rc != 0 and "expired" in err, err)
    env = dict(E.env())
    env.pop("RELAY_CONFIG_DIR")
    rc, out, err = E.cli("install", env=env)
    files = [os.path.join(E.home, p) for p in (".local/bin/relay", ".local/bin/relay-lead", ".config/relay/mod/.claude-plugin/plugin.json",
                                               ".config/relay/mod/hooks/hooks.json", ".config/relay/mod/hooks/register.ts")]
    check("install copies the program, the launcher and the extension", rc == 0 and all(os.path.isfile(f) for f in files)
          and stat.S_IMODE(os.stat(files[0]).st_mode) == 0o755, out + err)


if __name__ == "__main__":
    main()
