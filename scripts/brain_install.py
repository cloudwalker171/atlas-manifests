#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- /brain installer (additive, PG-safe, idempotent). Extracts the
stdlib-only brain package to /opt/atlas/brain, runs init -> seed, creates the
/var/lib/brain/logs/inbox.jsonl file bridge (seeding one atlas_outcome + one
deploy_roadbump demo event on first install to prove the wiring), then
drain -> reflect -> stats. Publishes status/hetzner/brain-stats.json so the
result is verifiable from origin/main. Touches NO existing prod service; no
lifeline restart. Run with --reflect-only to just drain+reflect+stats.
"""
import os, sys, json, time, tarfile, subprocess, base64, urllib.request, urllib.error

CODE = os.environ.get("BRAIN_CODE_DIR", "/opt/atlas/brain")
DATA = os.environ.get("BRAIN_ROOT", "/var/lib/brain")
TARBALL = os.environ.get("BRAIN_TARBALL", "/opt/atlas/brain-system.tar.gz")
PY = sys.executable  # /opt/atlas/venv/bin/python
NODE = os.environ.get("NODE_ID", "hetzner")


def run_brain(*args, timeout=120):
    env = dict(os.environ); env["BRAIN_ROOT"] = DATA
    p = subprocess.run([PY, "-m", "brain", *args, "--root", DATA],
                       cwd=CODE, env=env, capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()


def gh_put(path, obj):
    tok = os.environ.get("STATUS_TOKEN"); repo = os.environ.get("STATUS_REPO")
    api = os.environ.get("STATUS_API_BASE", "https://api.github.com"); br = os.environ.get("STATUS_BRANCH", "main")
    if not tok or not repo: return "skipped_no_token"
    url = "%s/repos/%s/contents/%s" % (api, repo, path); sha = None
    try:
        req = urllib.request.Request(url + "?ref=" + br, headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json", "User-Agent": "atlas-autopull"})
        sha = json.load(urllib.request.urlopen(req, timeout=20)).get("sha")
    except Exception: sha = None
    body = {"message": "brain stats %s" % NODE, "content": base64.b64encode(json.dumps(obj, indent=2).encode()).decode(), "branch": br}
    if sha: body["sha"] = sha
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="PUT", headers={"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json", "User-Agent": "atlas-autopull", "Content-Type": "application/json"})
    try: urllib.request.urlopen(req, timeout=20); return "put_ok"
    except urllib.error.HTTPError as e: return "put_http_%s" % e.code
    except Exception as e: return "put_err_%s" % type(e).__name__


def main():
    out = {"step": "brain_install", "node": NODE, "code_dir": CODE, "data_dir": DATA, "ts": int(time.time())}
    reflect_only = "--reflect-only" in sys.argv

    if not reflect_only:
        # extract (strip top-level component, like --strip-components=1)
        os.makedirs(CODE, exist_ok=True)
        # extract AS-IS (tarball already has brain/ at top level; stripping the
        # first component would flatten away the package dir and break -m brain)
        with tarfile.open(TARBALL, "r:gz") as tf:
            safe = [m for m in tf.getmembers() if not (m.name.startswith("/") or ".." in m.name.split("/"))]
            tf.extractall(CODE, members=safe)
        out["extracted"] = os.path.isdir(os.path.join(CODE, "brain"))
        rc, so, se = run_brain("init"); out["init"] = so or se
        rc, so, se = run_brain("seed"); out["seed"] = (so[:300] if so else se[:300])
        # file bridge + first-install demo events
        logs = os.path.join(DATA, "logs"); os.makedirs(logs, exist_ok=True)
        inbox = os.path.join(logs, "inbox.jsonl")
        marker = os.path.join(DATA, ".bridge_demo")
        if not os.path.exists(marker):
            with open(inbox, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"type": "atlas_outcome", "industry": "roofing", "source": "socrata_chicago", "converted": True}) + "\n")
                fh.write(json.dumps({"type": "deploy_roadbump", "blocker": "in-apply sleep wedged the autopull pipe (>90s service timeout)", "resolution": "keep run_allowlisted steps fast / non-blocking; never long-sleep in an apply", "severity": 85, "project": "atlas"}) + "\n")
            open(marker, "w").write(str(int(time.time())))
            out["bridge_seeded"] = True
        else:
            out["bridge_seeded"] = False
        if not os.path.exists(inbox):
            open(inbox, "a").close()
        out["inbox_bridge"] = inbox

    rc, so, se = run_brain("drain"); out["drain"] = (so[:300] if so else se[:200])
    rc, so, se = run_brain("reflect"); out["reflect"] = (so[:500] if so else se[:200])
    rc, so, se = run_brain("stats"); out["stats"] = so or se
    try: out["stats_parsed"] = json.loads(so)
    except Exception: pass
    out["publish"] = gh_put("status/%s/brain-stats.json" % NODE, out)
    print("BRAIN_INSTALL=" + json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()
