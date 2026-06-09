#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A.T.L.A.S. -- LIVE/MINED channel views builder (schema-INTROSPECTIVE, read-only).

Replaces the earlier static metrics_channels_patch.sql, which hardcoded
source_record.source_code + first_seen and FAILED on the live schema (seq-42).
This version introspects information_schema at runtime and resolves the real
column names, so it compiles against whatever the live schema exposes.

Creates two SELECT-only views (no DDL on tables, no new columns, no data
mutation -- fully reversible with DROP VIEW):
  * atlas.v_channel_classification(business_id, channel)   channel in (live, mined)
  * atlas.v_channel_metrics(channel, n)

LIVE-ALWAYS source codes  = real-time spine (CT new-SSL, new-MX, NRD/new-domain):
   presence == live by construction.
LIVE-IF-FRESH source codes = SoS new-reg / city license / EDGAR Form D / ...:
   live only when a per-row freshness ts is within 72h; if no usable freshness
   column exists they fall back to MINED (never an unprovable same-day number).
MINED = everything else (bulk/backfill/periodic).
business_total unchanged (LIVE union MINED == all rows).

--selftest: pure-logic gate (classifier + view-SQL builder), no DB/net.
Normal run: connects via /etc/atlas/db.env, builds views, prints CHANNEL_RESULT={...}.
"""
import os, sys, re, json

LIVE_ALWAYS_PAT = re.compile(
    r"(ct[_-]?ssl|crt[_-]?log|cert|"
    r"(?:^|[_-])ssl(?:[_-]|$)|(?:^|[_-])mx(?:[_-]|$)|(?:^|[_-])ct(?:[_-]|$)|"
    r"nrd|new[_-]?domain|newdomain|rdap)",
    re.I)
LIVE_IF_FRESH_PAT = re.compile(
    r"(sos[_-]?new|new[_-]?business|new[_-]?biz|newbiz|city[_-]?license|\blicense\b|"
    r"edgar|form[_-]?d|formd|registration|registered|new[_-]?reg)",
    re.I)

FRESH_CANDIDATES = ["first_seen", "first_seen_at", "seen_at", "fetched_at",
                    "created_at", "inserted_at", "discovered_at", "captured_at",
                    "observed_at", "registry_date", "filed_at", "last_seen",
                    "last_verified", "ts", "row_ts"]
SRC_CANDIDATES = ["source_code", "source", "source_name", "src", "src_code"]


def classify_codes(distinct_codes):
    always, if_fresh = [], []
    for c in distinct_codes:
        if c is None:
            continue
        c = str(c)
        if LIVE_ALWAYS_PAT.search(c):
            always.append(c)
        elif LIVE_IF_FRESH_PAT.search(c):
            if_fresh.append(c)
    return sorted(set(always)), sorted(set(if_fresh))


def _sql_in(codes):
    return ", ".join("'" + c.replace("'", "''") + "'" for c in codes)


def build_view_sql(srccol, freshcol, always, if_fresh):
    conds = []
    if always:
        conds.append("sr.%s IN (%s)" % (srccol, _sql_in(always)))
    if if_fresh and freshcol:
        conds.append("(sr.%s IN (%s) AND sr.%s >= now() - interval '72 hours')"
                     % (srccol, _sql_in(if_fresh), freshcol))
    if not conds:
        live_predicate = "FALSE"
    else:
        live_predicate = ("EXISTS (SELECT 1 FROM atlas.source_record sr "
                          "WHERE sr.business_id = b.id AND (%s))" % " OR ".join(conds))
    v1 = (
        "CREATE OR REPLACE VIEW atlas.v_channel_classification AS\n"
        "SELECT b.id AS business_id,\n"
        "       CASE WHEN %s THEN 'live' ELSE 'mined' END AS channel\n"
        "FROM atlas.business b;" % live_predicate
    )
    v2 = (
        "CREATE OR REPLACE VIEW atlas.v_channel_metrics AS\n"
        "SELECT channel, count(*)::bigint AS n\n"
        "FROM atlas.v_channel_classification\n"
        "GROUP BY channel;"
    )
    return v1 + "\n" + v2


def selftest():
    ok = True
    def chk(n, c):
        nonlocal ok
        print(("  ok  " if c else "  FAIL") + " " + n); ok = ok and c
    codes = ["ct_ssl", "mx_new", "nrd", "sos_new_business", "city_license",
             "edgar_formd", "overture", "osm", "chicago_business", "irs_eo_bmf",
             "socrata_cities", "source_catalog", "nonprofit_seed", "new_mx", "ct"]
    a, f = classify_codes(codes)
    chk("ct_ssl always-live", "ct_ssl" in a)
    chk("mx_new always-live", "mx_new" in a)
    chk("new_mx always-live", "new_mx" in a)
    chk("ct token always-live", "ct" in a)
    chk("nrd always-live", "nrd" in a)
    chk("sos_new_business if-fresh", "sos_new_business" in f)
    chk("city_license if-fresh", "city_license" in f)
    chk("edgar_formd if-fresh", "edgar_formd" in f)
    chk("overture mined", "overture" not in a and "overture" not in f)
    chk("irs mined", "irs_eo_bmf" not in a and "irs_eo_bmf" not in f)
    chk("chicago mined", "chicago_business" not in a and "chicago_business" not in f)
    sql = build_view_sql("source_code", "first_seen", a, f)
    chk("view names present", "v_channel_classification" in sql and "v_channel_metrics" in sql)
    chk("uses 72h window when freshcol present", "72 hours" in sql)
    chk("always codes in IN-list", "'ct_ssl'" in sql)
    chk("if-fresh codes in IN-list", "'sos_new_business'" in sql)
    chk("no table DDL", "ALTER TABLE" not in sql.upper() and "DROP TABLE" not in sql.upper()
        and "DELETE" not in sql.upper())
    chk("exactly two views", sql.upper().count("CREATE OR REPLACE VIEW") == 2)
    sql2 = build_view_sql("source", None, a, f)
    chk("no 72h window without freshcol", "72 hours" not in sql2)
    chk("if-fresh NOT live without freshcol", "'sos_new_business'" not in sql2)
    chk("always still live without freshcol", "'ct_ssl'" in sql2)
    sql3 = build_view_sql("source", None, [], [])
    chk("all-mined view valid", "FALSE" in sql3 and "v_channel_metrics" in sql3)
    print("SELFTEST", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())

    import psycopg2
    e = {}
    with open("/etc/atlas/db.env") as fh:
        for ln in fh:
            ln = ln.strip()
            if "=" in ln and not ln.startswith("#"):
                k, v = ln.split("=", 1)
                e[k.strip()] = v.strip().strip("'\"")
    conn = psycopg2.connect(host=e.get("PGHOST", "localhost"),
                            dbname=e.get("PGDATABASE", "tuanichat_atlas"),
                            user=e.get("PGUSER"), password=e.get("PGPASSWORD"),
                            port=e.get("PGPORT", "5432"))
    conn.autocommit = False
    cur = conn.cursor()

    def cols(table):
        cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                       WHERE table_schema='atlas' AND table_name=%s""", (table,))
        return {r[0]: r[1] for r in cur.fetchall()}

    biz = cols("business")
    sr = cols("source_record")
    if not biz or "id" not in biz:
        print("CHANNEL_RESULT=" + json.dumps({"status": "abort_no_business_table"})); sys.exit(20)
    if not sr:
        print("CHANNEL_RESULT=" + json.dumps({"status": "abort_no_source_record_table"})); sys.exit(21)

    srccol = next((c for c in SRC_CANDIDATES if c in sr), None)
    if not srccol:
        print("CHANNEL_RESULT=" + json.dumps({"status": "abort_no_source_column",
              "source_record_cols": sorted(sr.keys())})); sys.exit(22)
    freshcol = next((c for c in FRESH_CANDIDATES
                     if c in sr and ("timestamp" in sr[c] or "date" in sr[c])), None)

    cur.execute('SELECT DISTINCT "%s" FROM atlas.source_record LIMIT 500' % srccol)
    distinct_codes = [r[0] for r in cur.fetchall()]
    always, if_fresh = classify_codes(distinct_codes)

    cur.execute(build_view_sql(srccol, freshcol, always, if_fresh))
    conn.commit()
    cur.execute("SELECT channel, count(*) FROM atlas.v_channel_classification GROUP BY channel")
    rollup = {r[0]: int(r[1]) for r in cur.fetchall()}
    conn.close()

    out = {"status": "ok", "source_col": srccol, "fresh_col": freshcol,
           "live_always": always, "live_if_fresh": (if_fresh if freshcol else []),
           "if_fresh_demoted_to_mined": ([] if freshcol else if_fresh),
           "distinct_codes_seen": len(distinct_codes), "rollup": rollup}
    print("CHANNEL_RESULT=" + json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()
