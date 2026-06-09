#!/opt/atlas/venv/bin/python
"""
socrata_catalog_discover.py -- NO-AUTH Socrata catalog auto-discovery -> atlas.source_catalog.

WHY (Meta-Brain fit ~80/100): a FORCE-MULTIPLIER, not a row source. Instead of a human
hand-adding each city's business dataset to socrata_cities_import.py, this queries the
public Socrata discovery API and AUTO-FINDS new business-license / new-business-registration
datasets across ALL US Socrata domains -- making the source list self-expanding. It writes
the discovered catalog (domain, fourbyfour, name, frequency, last-updated, est. row hint)
into atlas.source_catalog so the operator / Smart Brain can promote the freshest, daily-
updated, US business datasets into the active collectors automatically.

VERIFIED LIVE (web-checked 2026-06-09 from this workspace via web_fetch):
  * https://api.us.socrata.com/api/catalog/v1?q=business+license&only=dataset
      200 OK -> 582 datasets (Chicago r5kz-chrr, Delaware 5zy2-grhr, Berkeley, Richmond, ...)
  * https://api.us.socrata.com/api/catalog/v1?q=new+business+registrations&only=dataset
      200 OK -> 79 datasets (Oregon SoS esjy-u4fc "New Businesses Registered Last Month", ...)

It does NOT fetch business rows and does NOT contact anyone -- it indexes metadata only.
.gov-domain datasets are catalogued (firmographic discovery) but the non-overridable
downstream contact-suppression gate still governs any outreach derived from them later.

US-scoped: only domains ending in .gov / .us / known US municipal domains are kept (configurable
via CATALOG_US_ONLY=1 default; CATALOG_ALLOW_NONUS=1 to disable). Person-level dataset names
(tax preparer, license holder roster of individuals, voter, employee) are EXCLUDED to keep it
firmographic. Capped (CATALOG_LIMIT, default 200 datasets per query). FAIL-LOUD: exits non-zero
if 0 datasets returned from ALL queries. Re-runs are idempotent (ON CONFLICT on domain+fxf).

--selftest : pure-logic (no network/DB) -- exercises US-filter + firmographic-exclusion +
             row-builder and exits 0; exits 3 only if invariants break.
"""

import os
import sys
import json
import time
import datetime
import urllib.parse
import urllib.request
import urllib.error

DB_ENV_PATH   = os.environ.get("ATLAS_DB_ENV", "/etc/atlas/db.env")
CATALOG_TBL   = ("atlas", "source_catalog")
HTTP_TIMEOUT  = int(os.environ.get("CATALOG_HTTP_TIMEOUT", "60"))
REQ_SLEEP     = float(os.environ.get("CATALOG_SLEEP", "0.3"))
PER_QUERY_CAP = int(os.environ.get("CATALOG_LIMIT", "200"))
US_ONLY       = os.environ.get("CATALOG_ALLOW_NONUS", "0").strip() not in ("1", "true", "yes")
USER_AGENT    = "atlas-socrata-catalog/1.0 (+https://github.com/cloudwalker171/atlas-manifests)"
APP_TOKEN     = os.environ.get("SOCRATA_APP_TOKEN", "").strip()
SOURCE_NAME   = "socrata_catalog"
CATALOG_API   = "https://api.us.socrata.com/api/catalog/v1"

# The discovery queries (firmographic business-discovery intent).
QUERIES = [q.strip() for q in os.environ.get(
    "CATALOG_QUERIES",
    "business license,new business registrations,active businesses,licensed businesses,"
    "business registrations,registered businesses").split(",") if q.strip()]

# Firmographic guard: dataset NAMES containing these are person-level -> excluded.
PERSON_EXCLUDE = ["tax preparer", "tax return preparer", "voter", "employee salaries",
                  "payroll", "individual", "resident", "patient", "student", "officer roster",
                  "license holder", "professional license"]

# US heuristic: keep gov/us domains + well-known US municipal/state open-data domains.
US_DOMAIN_HINTS = (".gov", ".us", "cityofnewyork", "cityofchicago", "lacity", "sfgov",
                   "seattle", "austintexas", "everettwa", "richmondgov", "cityofberkeley",
                   "data.ny", "data.ca", "data.tx", "data.wa", "data.or", "data.colorado")
NONUS_DOMAIN_HINTS = ("calgary", ".ca/", "data.calgary", "toronto", "ontario", ".uk", ".au",
                      ".eu", "amsterdam", "barcelona")

COUNTS_PATH = os.environ.get(
    "ATLAS_COUNTS_PATH",
    os.path.join(os.environ.get("ATLAS_AUTOPULL_STATE", "/var/lib/atlas/autopull"),
                 "last_counts.json"))
COUNTS_PATH_SRC = os.path.join(os.path.dirname(COUNTS_PATH),
                               f"last_counts_{SOURCE_NAME}.json")


def log(msg):
    print(f"[{SOURCE_NAME}] {msg}", flush=True)


def is_us_domain(domain):
    if not US_ONLY:
        return True
    d = (domain or "").lower()
    if any(h in d for h in NONUS_DOMAIN_HINTS):
        return False
    return any(h in d for h in US_DOMAIN_HINTS)


def is_firmographic(name):
    n = (name or "").lower()
    return not any(bad in n for bad in PERSON_EXCLUDE)


def freq_of(res, classification):
    # Socrata exposes update cadence in a few metadata spots; probe them all.
    for md in (classification or {}).get("domain_metadata", []):
        k = (md.get("key") or "").lower()
        if "frequency" in k or "refresh" in k or "update" in k:
            return md.get("value")
    return None


def build_catalog_row(result, query, now):
    res = result.get("resource", {}) or {}
    meta = result.get("metadata", {}) or {}
    classification = result.get("classification", {}) or {}
    domain = meta.get("domain")
    fxf = res.get("id")
    name = res.get("name")
    return {
        "domain": domain,
        "fourbyfour": fxf,
        "dataset_name": name,
        "data_updated_at": res.get("data_updated_at"),
        "update_frequency": freq_of(res, classification),
        "page_views_last_month": ((res.get("page_views") or {}).get("page_views_last_month")),
        "discovered_via": query,
        "permalink": result.get("permalink"),
        "license": meta.get("license"),
        "discovered_at": now,
    }


# --------------------------------------------------------------------------- #
# --selftest : pure logic, no network / no DB
# --------------------------------------------------------------------------- #
def selftest():
    assert is_us_domain("data.cityofchicago.org") is True
    assert is_us_domain("data.seattle.gov") is True
    assert is_us_domain("data.calgary.ca") is False        # Canada
    assert is_firmographic("Business Licenses") is True
    assert is_firmographic("NY State Registered Tax Return Preparers") is False  # person-level
    now = datetime.datetime.now(datetime.timezone.utc)
    sample = {
        "resource": {"id": "r5kz-chrr", "name": "Business Licenses",
                     "data_updated_at": "2026-06-09T10:28:36.000Z",
                     "page_views": {"page_views_last_month": 6090}},
        "metadata": {"domain": "data.cityofchicago.org", "license": "See Terms of Use"},
        "classification": {"domain_metadata": [{"key": "Metadata_Frequency",
                                                "value": "Data is updated daily"}]},
        "permalink": "https://data.cityofchicago.org/d/r5kz-chrr",
    }
    row = build_catalog_row(sample, "business license", now)
    assert row["domain"] == "data.cityofchicago.org" and row["fourbyfour"] == "r5kz-chrr", row
    assert row["update_frequency"] == "Data is updated daily", row
    assert row["discovered_via"] == "business license", row
    print("[socrata_catalog] --selftest OK (us-filter + firmographic-exclusion + row-builder)")
    sys.exit(0)


# --------------------------------------------------------------------------- #
# DB env + connection (verbatim pattern) + ensure catalog table
# --------------------------------------------------------------------------- #
def load_db_env(path):
    if not os.path.exists(path):
        log(f"WARNING: {path} not found; relying on existing environment.")
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def connect_pg():
    import psycopg2
    def pick(*names, default=None):
        for n in names:
            if os.environ.get(n):
                return os.environ[n]
        return default
    conn = psycopg2.connect(
        host=pick("PGHOST", "DB_HOST", "ATLAS_DB_HOST", default="localhost"),
        port=pick("PGPORT", "DB_PORT", "ATLAS_DB_PORT", default="5432"),
        dbname=pick("PGDATABASE", "DB_NAME", "ATLAS_DB_NAME", default="tuanichat_atlas"),
        user=pick("PGUSER", "DB_USER", "ATLAS_DB_USER", default="atlas"),
        password=pick("PGPASSWORD", "DB_PASSWORD", "ATLAS_DB_PASSWORD", default=None),
    )
    conn.autocommit = False
    return conn


def ensure_catalog_table(cur):
    """Create atlas.source_catalog if absent (idempotent). Introspect first; create-if-missing."""
    cur.execute("SELECT to_regclass('atlas.source_catalog')")
    if cur.fetchone()[0] is None:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS atlas.source_catalog ("
            " id BIGSERIAL PRIMARY KEY,"
            " domain TEXT NOT NULL,"
            " fourbyfour TEXT NOT NULL,"
            " dataset_name TEXT,"
            " data_updated_at TEXT,"
            " update_frequency TEXT,"
            " page_views_last_month INTEGER,"
            " discovered_via TEXT,"
            " permalink TEXT,"
            " license TEXT,"
            " discovered_at TIMESTAMPTZ DEFAULT now(),"
            " UNIQUE (domain, fourbyfour))")
        log("created atlas.source_catalog")


def fetch_catalog(query, offset):
    qs = {"q": query, "only": "dataset", "limit": min(PER_QUERY_CAP, 100), "offset": offset}
    url = f"{CATALOG_API}?" + urllib.parse.urlencode(qs)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, timeout=HTTP_TIMEOUT)
    data = json.loads(resp.read().decode("utf-8", "replace"))
    return data.get("results", []), data.get("resultSetSize", 0)


def write_counts(counts):
    for path in (COUNTS_PATH, COUNTS_PATH_SRC):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(counts, fh)
            log(f"wrote counts -> {path}")
        except OSError as e:
            log(f"WARNING: could not write {path}: {e}")


def main():
    if "--selftest" in sys.argv:
        selftest()

    load_db_env(DB_ENV_PATH)
    conn = connect_pg()
    cur = conn.cursor()
    from psycopg2 import Error as PgError
    ensure_catalog_table(cur)
    conn.commit()

    now = datetime.datetime.now(datetime.timezone.utc)
    total_seen = 0
    inserted = 0
    skipped_nonus = 0
    skipped_person = 0
    per_query = {}

    for query in QUERIES:
        n_q = 0
        offset = 0
        try:
            while n_q < PER_QUERY_CAP:
                results, total = fetch_catalog(query, offset)
                if not results:
                    break
                for result in results:
                    if n_q >= PER_QUERY_CAP:
                        break
                    total_seen += 1
                    n_q += 1
                    row = build_catalog_row(result, query, now)
                    if not row["domain"] or not row["fourbyfour"]:
                        continue
                    if not is_us_domain(row["domain"]):
                        skipped_nonus += 1
                        continue
                    if not is_firmographic(row["dataset_name"]):
                        skipped_person += 1
                        continue
                    cols = list(row.keys())
                    vals = [row[c] for c in cols]
                    col_list = ", ".join(f'"{c}"' for c in cols)
                    ph = ", ".join(["%s"] * len(cols))
                    sql = (f'INSERT INTO atlas.source_catalog ({col_list}) VALUES ({ph}) '
                           f'ON CONFLICT (domain, fourbyfour) DO UPDATE SET '
                           f'data_updated_at=EXCLUDED.data_updated_at, '
                           f'update_frequency=EXCLUDED.update_frequency, '
                           f'page_views_last_month=EXCLUDED.page_views_last_month, '
                           f'discovered_at=EXCLUDED.discovered_at')
                    try:
                        cur.execute(sql, vals)
                        if cur.rowcount and cur.rowcount > 0:
                            inserted += cur.rowcount
                    except PgError as e:
                        conn.rollback()
                        log(f"[{query}] insert error {row['domain']}:{row['fourbyfour']}: "
                            f"{e.pgerror or e}")
                        continue
                conn.commit()
                if len(results) < min(PER_QUERY_CAP, 100):
                    break
                offset += len(results)
                time.sleep(REQ_SLEEP)
            per_query[query] = n_q
            log(f"[{query}] seen {n_q}")
        except urllib.error.HTTPError as e:
            conn.rollback()
            per_query[query] = f"HTTP {e.code}"
            log(f"[{query}] HTTP {e.code}: {e.reason} -- skip query")
        except Exception as e:                       # noqa: BLE001
            conn.rollback()
            per_query[query] = f"ERR {e}"
            log(f"[{query}] FAILED: {e}")
        time.sleep(REQ_SLEEP)

    cur.execute("SELECT count(*) FROM atlas.source_catalog")
    catalog_total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM atlas.source_catalog "
                "WHERE update_frequency ILIKE '%daily%'")
    daily_total = cur.fetchone()[0]

    log("=" * 70)
    log(f"SUMMARY queries={QUERIES} seen={total_seen} upserted={inserted} "
        f"skipped_nonus={skipped_nonus} skipped_person={skipped_person}")
    log(f"per_query={per_query}")
    log(f"READ-BACK atlas.source_catalog total={catalog_total} (daily-updated={daily_total})")
    log("=" * 70)

    counts = {
        "lane": SOURCE_NAME, "seen": total_seen, "upserted": inserted,
        "catalog_total": catalog_total, "daily_total": daily_total,
        "skipped_nonus": skipped_nonus, "skipped_person": skipped_person,
        "per_query": per_query, "ts": int(time.time()),
    }
    write_counts(counts)
    cur.close()
    conn.close()

    if total_seen == 0:
        log("FAIL: 0 datasets returned from ALL catalog queries.")
        sys.exit(2)
    log(f"PASS: seen={total_seen}; catalog_total={catalog_total}; daily={daily_total}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
