#!/opt/atlas/venv/bin/python
"""
atlas_enrich_pool.py -- THREADED enrichment pool worker (gather/apply split).

Reuses atlas_enrich_worker's TESTED helpers verbatim (discover_domain, crawl,
parse_page, mx_info, record, fill_if_empty, xref_sources, claim_batch, finish_row,
the .gov/.mil suppression, resolve_columns). It ONLY re-orchestrates them so that:

  * a SMALL pool of processes (atlas-enrich-pool@1..@N, sized to cores) each runs
    ATLAS_POOL_CONCURRENCY threads,
  * each round: claim a batch (1 PG conn, committed immediately), GATHER each
    business concurrently in the thread pool (PURE NETWORK -- no DB, so no PG
    connection is held during the slow DNS/HTTP/MX waits), then APPLY all results
    sequentially on the single main-thread connection and finish each row.

=> M processes x T threads = M*T in-flight lanes on only M Postgres connections
   (one per process), instead of one connection per lane. Coexists with the sync
   atlas-enrich-worker@ fleet via FOR UPDATE SKIP LOCKED (no row is double-claimed).

Modes: --loop (default), --once, --selftest (exit 0 ok / 3 broken; the deploy gate).
DB creds + STATUS_* come from the same /etc/atlas/db.env + /etc/atlas/autopull.env.
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# import the deployed sync worker as a library of tested helpers
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, "/opt/atlas/enrich")
import atlas_enrich_worker as w  # noqa: E402

CONC      = int(os.environ.get("ATLAS_POOL_CONCURRENCY", "16"))   # threads / process
CLAIM     = int(os.environ.get("ATLAS_POOL_CLAIM", str(max(CONC * 2, 8))))  # rows / round
IDLE_SEC  = float(os.environ.get("ATLAS_POOL_IDLE_SEC", "10"))
REPORT_SEC = int(getattr(w, "REPORT_SEC", 300))
AUTOPULL_ENV = getattr(w, "AUTOPULL_ENV_PATH", "/etc/atlas/autopull.env")
DB_ENV = getattr(w, "DB_ENV_PATH", "/etc/atlas/db.env")


def log(m):
    w.log("[pool] " + m)


def gather(vals, cols):
    """PURE NETWORK. Mirrors atlas_enrich_worker.enrich_one's probe logic but writes
    NOTHING to the DB -- returns observations + business fills to be applied later.
    Preserves the non-overridable .gov/.mil contact suppression exactly."""
    name = vals.get(cols.get("name")) or ""
    website = vals.get(cols.get("website")) or ""
    category = vals.get(cols.get("category")) or ""
    region = vals.get(cols.get("region")) or ""
    obs = []    # (field, value, source, method, confidence, url)
    fills = []  # (logical_col, value)

    domain, dsrc, dconf = w.discover_domain(name, website)
    gov = w.is_gov_mil(domain) if domain else False
    if domain:
        obs.append(("domain", domain, dsrc, "domain_resolve", dconf, None))
        if dsrc != "existing_website":
            fills.append(("website", "https://%s" % domain))

    page = None
    if domain:
        for path in ("/", "/contact", "/contact-us", "/about"):
            if not w.robots_allows(domain, path):
                continue
            res = w.http_get("https://%s%s" % (domain, path), timeout=w.HTTP_TIMEOUT)
            if not res or res[1] >= 400:
                if path == "/":
                    res = w.http_get("http://%s%s" % (domain, path), timeout=w.HTTP_TIMEOUT)
                if not res or res[1] >= 400:
                    continue
            info = w.parse_page(res[0], res[2], res[3])
            if page is None:
                page = info
            else:
                page["emails"] |= info["emails"]
                page["phones"] |= info["phones"]
                page["tech"] |= info["tech"]
                for k, v in info["socials"].items():
                    page["socials"].setdefault(k, v)
            if path == "/":
                page["title"] = info.get("title")
                page["meta_desc"] = info.get("meta_desc")

    if page and page["tech"]:
        obs.append(("tech", ",".join(sorted(page["tech"])[:12]),
                    "homepage_crawl", "html_signature", 0.7, "https://%s/" % domain))

    if domain and not gov:
        has_mx, provider, hosts = w.mx_info(domain)
        if has_mx:
            obs.append(("mx", ",".join(hosts[:4]), "dns_mx", "dns", 0.9, None))
            if provider:
                obs.append(("email_provider", provider, "dns_mx", "mx_host", 0.8, None))

    if gov:
        obs.append(("contact_suppressed", "gov_mil", "policy",
                    "non_overridable_suppression", 1.0, None))
    elif page:
        company_emails = sorted(e for e in page["emails"] if domain and e.endswith("@" + domain))
        role_emails = [e for e in company_emails if e.split("@")[0] in w.ROLE_LOCALPARTS]
        if role_emails:
            obs.append(("email", ",".join(role_emails[:3]),
                        "homepage_crawl", "role_email", 0.75, "https://%s/" % domain))
            fills.append(("email", role_emails[0]))
        pat, pconf = w.infer_email_pattern(page["emails"], domain) if domain else (None, 0)
        if pat:
            obs.append(("email_pattern", pat, "email_pattern", "inferred", pconf, None))
        phones = sorted({w.norm_phone(p) for p in page["phones"]} - {None})
        if phones:
            obs.append(("phone", ",".join(phones[:3]),
                        "homepage_crawl", "regex_nanp", 0.7, "https://%s/" % domain))
            fills.append(("phone", phones[0]))
        for net, surl in page["socials"].items():
            obs.append(("social_%s" % net, surl, "homepage_crawl", "link", 0.8,
                        "https://%s/" % domain))

    title = page.get("title") if page else None
    meta = page.get("meta_desc") if page else None
    ind, iconf = w.infer_industry(category, title, meta)
    if ind:
        obs.append(("industry", ind, "classifier", "keyword", iconf, None))
        fills.append(("industry", ind))
    sz, sconf = w.size_cues(" ".join(x for x in (title, meta) if x))
    if sz:
        obs.append(("size_cue", sz, "homepage_crawl", "heuristic", sconf, None))

    return {"obs": obs, "fills": fills, "name": name, "region": region,
            "domain": domain, "gov": gov}


def _safe_gather(vals, cols):
    try:
        return gather(vals, cols)
    except Exception as e:  # noqa: BLE001
        return e


def apply_obs(conn, cols, ref, g):
    """DB write phase (main thread, single connection). Mirrors enrich_one writes."""
    biz_schema, biz_table = w.BUSINESS_TBL
    cur = conn.cursor()
    filled = 0
    prov = 0
    for (field, value, source, method, conf, url) in g["obs"]:
        w.record(cur, ref, field, value, source, method, conf, url)
        prov += 1
    for (logical, value) in g["fills"]:
        col = cols.get(logical)
        if col and w.fill_if_empty(cur, biz_schema, biz_table, cols["pk"], ref, col, value):
            filled += 1
    try:
        xhits = w.xref_sources(cur, cols["sr_cols"], g["name"], g["region"])
        if xhits:
            xval = ";".join("%s:%s" % (src, m) for src, m, _ in xhits[:5])
            xconf = max((c for _, _, c in xhits), default=0.5)
            w.record(cur, ref, "xref", xval, "source_xref", "name_match", xconf)
            prov += 1
    except Exception:  # noqa: BLE001
        conn.rollback()
    conn.commit()
    cur.close()
    return {"fields_filled": filled, "prov_rows": prov,
            "suppressed": g["gov"], "domain": g["domain"]}


def _fetch_vals(conn, cols, ids):
    biz_schema, biz_table = w.BUSINESS_TBL
    sel = [c for c in (cols.get("name"), cols.get("website"), cols.get("phone"),
                       cols.get("email"), cols.get("category"), cols.get("region"),
                       cols.get("locality")) if c]
    sel_sql = ", ".join('"%s"' % c for c in sel)
    out = {}
    cur = conn.cursor()
    for bid in ids:
        cur.execute('SELECT %s FROM "%s"."%s" WHERE "%s"=%%s'
                    % (sel_sql, biz_schema, biz_table, cols["pk"]), (int(bid),))
        r = cur.fetchone()
        out[bid] = dict(zip(sel, r)) if r else None
    cur.close()
    conn.commit()
    return out


def run_loop(once=False):
    w.load_env_file(AUTOPULL_ENV)
    w.load_env_file(DB_ENV)
    inst = os.environ.get("ATLAS_WORKER_INSTANCE", str(os.getpid()))
    worker_id = "pool-%s:%s" % (inst, os.getpid())
    node = os.environ.get("NODE_ID", "hetzner")
    conn = w.connect_pg()
    cols = w.resolve_columns(conn)
    pool = ThreadPoolExecutor(max_workers=CONC)
    log("up worker_id=%s conc=%d claim=%d" % (worker_id, CONC, CLAIM))
    done_total = 0
    t0 = time.time()
    last_report = t0
    while True:
        rows = w.claim_batch(conn, worker_id, CLAIM)  # committed; [(qid,bid,task)]
        if not rows:
            if once:
                break
            time.sleep(IDLE_SEC)
            continue
        vals_by = _fetch_vals(conn, cols, [bid for (_, bid, _) in rows])
        futs = {}
        for (qid, bid, task) in rows:
            v = vals_by.get(bid)
            if v is not None:
                futs[qid] = pool.submit(_safe_gather, v, cols)
        for (qid, bid, task) in rows:
            v = vals_by.get(bid)
            if v is None:
                w.finish_row(conn, qid, "done", result_extra={"missing": True})
                continue
            gd = futs[qid].result()
            if isinstance(gd, Exception) or gd is None:
                w.finish_row(conn, qid, "failed", err="gather:%s" % str(gd)[:300])
                continue
            try:
                oc = apply_obs(conn, cols, bid, gd)
                w.finish_row(conn, qid, "done", result_extra=oc)
                done_total += 1
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                w.finish_row(conn, qid, "failed", err=str(e)[:500])
        now = time.time()
        if now - last_report >= REPORT_SEC:
            try:
                rate = done_total / max((now - t0) / 60.0, 1e-6)
                w.report(conn, cols, worker_id, node, rate, done_total)
            except Exception:  # noqa: BLE001
                pass
            last_report = now
        if once:
            break
    pool.shutdown(wait=False)
    conn.close()


def do_selftest():
    w.load_env_file(AUTOPULL_ENV)
    w.load_env_file(DB_ENV)
    ok = []
    try:
        conn = w.connect_pg()
        ok.append("PG connect")
    except Exception as e:  # noqa: BLE001
        log("selftest PG connect FAILED: %s" % e)
        sys.exit(3)
    try:
        cols = w.resolve_columns(conn)
        ok.append("resolve_columns pk=%s" % cols.get("pk"))
    except Exception as e:  # noqa: BLE001
        log("selftest resolve_columns FAILED: %s" % e)
        sys.exit(3)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM atlas.enrich_queue WHERE status IN ('pending','error') "
                    "ORDER BY priority,id FOR UPDATE SKIP LOCKED LIMIT 1")
        cur.fetchone()
        conn.rollback()
        cur.close()
        ok.append("SKIP LOCKED claim probe (rolled back)")
    except Exception as e:  # noqa: BLE001
        conn.rollback()
        log("selftest claim probe FAILED: %s" % e)
        sys.exit(3)
    try:
        g = gather({cols.get("name"): "Example Holdings LLC"}, cols)
        assert isinstance(g, dict) and "obs" in g and "fills" in g
        ok.append("gather OK (no-DB) obs=%d fills=%d" % (len(g["obs"]), len(g["fills"])))
    except Exception as e:  # noqa: BLE001
        log("selftest gather FAILED: %s" % e)
        sys.exit(3)
    for line in ok:
        log("selftest PASS: " + line)
    print("POOL SELFTEST: PASS (%d checks); conc=%d claim=%d" % (len(ok), CONC, CLAIM))
    conn.close()
    sys.exit(0)


def main():
    args = set(sys.argv[1:])
    if "--selftest" in args:
        do_selftest()
    elif "--once" in args:
        run_loop(once=True)
    else:
        run_loop(once=False)


if __name__ == "__main__":
    main()
