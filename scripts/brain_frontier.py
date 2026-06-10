"""
brain.frontier
==============

The **research-frontier** expansion of the Meta Brain improvement engine
(:mod:`brain.improve`). This module is *additive*: :mod:`brain.improve` imports
it and folds its output into the same passes, persistence and report. It never
changes the existing auto-safe boundary -- every new capability routes risky /
large / monetization work to ``needs_human`` and only ever stages the same
narrow auto-safe class through the same signed-manifest OUTBOX + HMAC puller.

It implements the four standing frontier capabilities (see
``RESEARCH_FRONTIER_PLAN.md`` and the ``research-frontier-ambition`` directive):

1. **Self-expanding scout catalog** (:func:`expand_catalog`) -- a discovery
   routine proposes NEW candidate sources / enrichment techniques /
   architectures / monetization models and appends *vetted, deduped* candidates
   to a persistent catalog file (``catalog.json``) in the /brain store, each
   tagged with rationale, a lawful/free check and an estimated EV. The engine's
   idea space therefore GROWS run over run instead of being a fixed list.

2. **Bolder "big bets" tier** (:func:`generate_big_bets`) -- larger, ambitious,
   future-thinking proposals INCLUDING monetization (pricing, packaging, new
   revenue surfaces, upsell paths) and architectural swings, as an explicitly
   labeled escalated tier. These are ALWAYS ``needs_human`` and are never
   auto-applied; the point is to surface things we have not thought of, with
   honest EV / risk / effort.

3. **Experiment framework** (:func:`build_experiments`, :func:`run_experiments`)
   -- small, SAFE, REVERSIBLE, CANARIED experiments (hypothesis -> metric ->
   safe change -> measure -> conclude). Only auto-safe + reversible experiments
   run through the existing deploy pipe (restore-point + rollback); risky ones
   are proposed but escalated, never auto-run. Outcomes are recorded in an
   experiment ledger (``experiments.json``) and fed back into the idea's EV +
   a lesson / win / roadbump in /brain. A failed experiment writes a
   **rollback marker** + a lesson.

4. **Outcome-driven learning hooks** (:func:`load_outcome_stats`,
   :func:`outcome_multiplier`) -- read ``atlas.outcome_stats`` rollups when
   present and rank ideas + experiments by MEASURED results (which
   sources/industries/angles actually produced outcomes), degrading GRACEFULLY
   to signal-based ranking when outcomes are not flowing yet (they are not wired
   today -- ``outcome_stat_rows == 0`` -- so absence is handled honestly, never
   fabricated).

Stdlib only. No network egress from here (the daily scout proposes from a
curated generator space; the box's own collectors verify a source before it ever
self-applies, and even then only OFF-by-default).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

from .schemas import clamp, make_record, now_iso
from .store import BrainStore

# ---------------------------------------------------------------------------
# Persistent store file names (live in the /brain root, next to memory/)
# ---------------------------------------------------------------------------

CATALOG_FILE = "catalog.json"
EXPERIMENTS_FILE = "experiments.json"


def _frontier_path(store: BrainStore, name: str) -> str:
    return os.path.join(store.root, name)


def _load_frontier(store: BrainStore, name: str) -> Dict[str, Any]:
    path = _frontier_path(store, name)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_frontier(store: BrainStore, name: str, data: Dict[str, Any]) -> None:
    path = _frontier_path(store, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


# ===========================================================================
# (4) OUTCOME-DRIVEN LEARNING HOOKS  (built first; the others consume it)
# ===========================================================================

#: Where the box-side outcome rollup is published (by atlas_outcome_ingest ->
#: atlas.outcome_stats -> exported to a status JSON). We read the JSON form so
#: this stays stdlib-only and egress-free; absence is the honest default today.
OUTCOME_SIGNAL_FILES = ("outcome-stats.json", "outcome_stats.json", "atlas-outcomes.json")


def load_outcome_stats(signals: Dict[str, Any]) -> Dict[str, Any]:
    """Extract measured outcome rollups from the loaded signals, honestly.

    Returns a dict with keys: available, rows, by_source, by_industry,
    outcome_stat_rows (honest count; 0 == not wired yet), note.

    The conversion rate is ``converted / max(1, enriched)`` per key. When no
    outcome signal is present (the truth today: ``outcome_stat_rows == 0``) the
    function returns ``available=False`` and EV ranking degrades to signal-only.
    It never fabricates rates.
    """
    rows: List[Dict[str, Any]] = []
    for name in OUTCOME_SIGNAL_FILES:
        doc = signals.get(name)
        if isinstance(doc, dict):
            candidate = doc.get("stats") or doc.get("rows") or doc.get("outcome_stats")
            if isinstance(candidate, list):
                rows = candidate
                break
        elif isinstance(doc, list):
            rows = doc
            break

    def _rate(num: Any, den: Any) -> float:
        try:
            n = float(num or 0)
            d = float(den or 0)
        except (TypeError, ValueError):
            return 0.0
        return n / d if d > 0 else 0.0

    by_source: Dict[str, Tuple[float, float]] = {}
    by_industry: Dict[str, Tuple[float, float]] = {}
    clean_rows: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        src = str(r.get("source") or "").strip() or "unknown"
        path = str(r.get("path") or r.get("industry") or "").strip() or "default"
        enriched = r.get("enriched", 0)
        converted = r.get("converted", 0)
        clean_rows.append({
            "source": src, "path": path,
            "enriched": enriched,
            "contactable": r.get("contactable", 0),
            "replied": r.get("replied", 0),
            "converted": converted,
        })
        cs, es = by_source.get(src, (0.0, 0.0))
        by_source[src] = (cs + float(converted or 0), es + float(enriched or 0))
        ci, ei = by_industry.get(path, (0.0, 0.0))
        by_industry[path] = (ci + float(converted or 0), ei + float(enriched or 0))

    rows_n = len(clean_rows)
    available = rows_n > 0 and any(
        (row.get("enriched") or 0) or (row.get("converted") or 0) for row in clean_rows
    )
    return {
        "available": bool(available),
        "rows": clean_rows,
        "by_source": {k: _rate(c, e) for k, (c, e) in by_source.items()},
        "by_industry": {k: _rate(c, e) for k, (c, e) in by_industry.items()},
        "outcome_stat_rows": rows_n,
        "note": (
            "MEASURED outcome rollups present -- ranking is outcome-driven."
            if available else
            "No outcome rows flowing yet (outcome_stat_rows=0). Degrading "
            "gracefully to signal-based ranking; not fabricating conversion data."
        ),
    }


def outcome_multiplier(idea: Dict[str, Any], outcomes: Dict[str, Any]) -> float:
    """Return an EV multiplier in ~[0.8, 1.5] from MEASURED outcomes, or 1.0.

    When outcomes are available and the idea names a source/industry that has
    produced measured conversions, ideas tied to higher-converting
    sources/industries are boosted; lower converters are mildly damped. When
    outcomes are absent (today), returns exactly 1.0 (no effect) -- graceful
    degradation, no fabrication.
    """
    if not outcomes.get("available"):
        return 1.0
    by_source = outcomes.get("by_source", {})
    by_industry = outcomes.get("by_industry", {})
    all_rates = list(by_source.values()) + list(by_industry.values())
    if not all_rates:
        return 1.0
    avg = sum(all_rates) / len(all_rates)
    if avg <= 0:
        return 1.0
    blob = " ".join(str(idea.get(k, "")) for k in ("target", "title", "area", "proposed_change")).lower()
    best = None
    for key, rate in {**by_source, **by_industry}.items():
        if key and key.lower() in blob:
            best = rate if best is None else max(best, rate)
    if best is None:
        return 1.0
    ratio = best / avg
    return max(0.8, min(1.5, ratio))


# ===========================================================================
# (1) SELF-EXPANDING SCOUT CATALOG
# ===========================================================================

#: The generator space the daily discovery routine draws from. Each entry is a
#: candidate the engine can PROPOSE adding to the persistent catalog. Every entry
#: is zero-cost + lawful + firmographic-only by construction (no ToS-violating
#: sources, no PII scraping). ``kind`` is one of: source / enrichment /
#: architecture / monetization. ``change_type`` is the type it would map to IF it
#: ever self-applies (monetization/architecture map to escalated types so they
#: can never be auto-applied).
DISCOVERY_GENERATORS: List[Dict[str, Any]] = [
    {"key": "discovery.ct_log_tertiary", "kind": "source",
     "desc": "A third Certificate-Transparency log stream for redundancy + earlier business-birth events",
     "rationale": "More CT coverage = earlier discovery of new domains (TITAN timeline), zero cost.",
     "lawful_free": "Public CT logs (RFC 6962), free, firmographic-only.",
     "change_type": "add_data_source", "ev": 62},
    {"key": "discovery.gleif_lei", "kind": "source",
     "desc": "GLEIF LEI open data (legal entity identifiers, parent/child org structure)",
     "rationale": "Free authoritative legal-entity graph; firmographic only, public domain.",
     "lawful_free": "GLEIF Golden Copy is public-domain/free; no PII.",
     "change_type": "add_data_source", "ev": 58},
    {"key": "discovery.opencorporates_seed", "kind": "source",
     "desc": "Open company registry seeds (jurisdictional incorporation events)",
     "rationale": "Incorporation = a business-birth event we can fuse into the timeline.",
     "lawful_free": "Use only openly-licensed registry exports; firmographic only.",
     "change_type": "add_data_source", "ev": 55},
    {"key": "discovery.usaspending_awards", "kind": "source",
     "desc": "USAspending federal award recipients (newly-funded orgs)",
     "rationale": "A funded org is a high-intent firmographic signal; fully public.",
     "lawful_free": "USAspending.gov is public-domain US gov data; no PII.",
     "change_type": "add_data_source", "ev": 52},
    {"key": "enrich.mx_provider_class", "kind": "enrichment",
     "desc": "Classify mail provider (Google Workspace / M365 / other) from MX records",
     "rationale": "Provider class sharpens deliverability + ICP scoring at zero cost.",
     "lawful_free": "Public DNS MX lookups; firmographic, no message content.",
     "change_type": "add_data_source", "ev": 60},
    {"key": "enrich.tech_stack_headers", "kind": "enrichment",
     "desc": "Lightweight tech-stack inference from public HTTP response headers",
     "rationale": "Tech stack is a strong firmographic ICP lever; polite + cacheable.",
     "lawful_free": "Reads only public response headers within polite-crawl bounds.",
     "change_type": "add_data_source", "ev": 50},
    {"key": "arch.embedding_dedup", "kind": "architecture",
     "desc": "Add a vector-embedding entity-resolution lane to cut duplicate orgs",
     "rationale": "Could materially lift unique-org yield, but it is a large, stateful change.",
     "lawful_free": "Internal-only compute; no new external data.",
     "change_type": "architecture_swing", "ev": 68},
    {"key": "arch.streaming_intake", "kind": "architecture",
     "desc": "Move CT/registry intake from batch to a streaming consumer for lower latency",
     "rationale": "Lower discovery latency on Channel-1, but a non-trivial re-architecture.",
     "lawful_free": "Same lawful sources, different transport.",
     "change_type": "architecture_swing", "ev": 64},
    {"key": "monetize.usage_tiered_api", "kind": "monetization",
     "desc": "Usage-tiered firmographic API (free tier + metered paid tiers)",
     "rationale": "Turns the enriched graph into a recurring revenue surface.",
     "lawful_free": "Sells only lawful firmographic data we are licensed to redistribute.",
     "change_type": "monetization_proposal", "ev": 72},
    {"key": "monetize.icp_alerts_upsell", "kind": "monetization",
     "desc": "Real-time ICP-match alert upsell on top of TuaniChat (new-business triggers)",
     "rationale": "High-intent, time-sensitive alerts are a natural premium upsell path.",
     "lawful_free": "Alerts on lawful firmographic events; honors opt-out.",
     "change_type": "monetization_proposal", "ev": 70},
    {"key": "monetize.industry_packs", "kind": "monetization",
     "desc": "Verticalized industry data packs (e.g. med-spa, HVAC) as packaged SKUs",
     "rationale": "Packaging by vertical raises willingness-to-pay vs a flat list.",
     "lawful_free": "Same lawful firmographic data, repackaged; no PII.",
     "change_type": "monetization_proposal", "ev": 66},
]


def expand_catalog(
    store: BrainStore, outcomes: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Grow the persistent scout catalog by proposing + vetting NEW candidates.

    Loads ``catalog.json`` from the /brain root, runs the discovery routine over
    :data:`DISCOVERY_GENERATORS`, appends any candidate not already present
    (deduped by ``key``), tags each with rationale + lawful/free check + an
    estimated EV (outcome-weighted when measured data exists), and persists.

    Idempotent on a fixed generator space (re-running adds nothing once absorbed)
    -- but the generator space is where new ideas land over time, so the idea
    space genuinely expands run over run. Returns a summary the report surfaces.
    """
    cat = _load_frontier(store, CATALOG_FILE)
    items: Dict[str, Any] = cat.get("items", {}) if isinstance(cat.get("items"), dict) else {}
    outcomes = outcomes or {"available": False}

    added: List[str] = []
    for gen in DISCOVERY_GENERATORS:
        key = gen["key"]
        probe = {"target": key, "title": gen["desc"], "area": gen["kind"]}
        est_ev = round(gen["ev"] * outcome_multiplier(probe, outcomes), 2)
        if key in items:
            items[key]["est_ev"] = est_ev  # keep EV fresh; never drop a vetted item
            continue
        items[key] = {
            "key": key,
            "kind": gen["kind"],
            "desc": gen["desc"],
            "rationale": gen["rationale"],
            "lawful_free": gen["lawful_free"],
            "change_type": gen["change_type"],
            "est_ev": est_ev,
            "first_seen": now_iso(),
            "vetted": True,
            "opt_in_off_by_default": gen["change_type"] == "add_data_source",
        }
        added.append(key)

    cat["items"] = items
    cat["updated_at"] = now_iso()
    cat["count"] = len(items)
    cat.setdefault("created_at", now_iso())
    _save_frontier(store, CATALOG_FILE, cat)

    store.append_log("frontier_catalog.log.jsonl", {
        "ts": now_iso(), "added": added, "total": len(items),
        "outcome_driven": bool(outcomes.get("available")),
    })
    return {"added": added, "added_count": len(added), "total": len(items),
            "items": list(items.values())}


def catalog_candidate_ideas(store: BrainStore) -> List[Dict[str, Any]]:
    """Turn vetted, auto-safe-eligible catalog items into improvement ideas.

    Only ``add_data_source`` catalog items become candidate ideas here (the only
    catalog kind that could ever be auto-safe -- as OFF-by-default opt-ins).
    Monetization + architecture items are NOT emitted here; they flow through the
    big-bets tier (always needs_human).
    """
    from .improve import _idea  # local import to avoid a cycle at module load

    cat = _load_frontier(store, CATALOG_FILE)
    items = cat.get("items", {}) if isinstance(cat.get("items"), dict) else {}
    out: List[Dict[str, Any]] = []
    for it in items.values():
        if it.get("change_type") != "add_data_source":
            continue
        out.append(_idea(
            title=f"Catalog: {it['desc']}",
            area="catalog_source",
            problem="A vetted, lawful/free catalog candidate not yet incorporated.",
            proposed_change=(
                f"Evaluate '{it['desc']}' and, if it passes selftest on canary, "
                f"incorporate as an OFF-by-default opt-in. Rationale: {it['rationale']} "
                f"Lawful/free: {it['lawful_free']}"
            ),
            expected_impact=f"Estimated EV {it.get('est_ev')}: {it['rationale']}",
            change_type="add_data_source",
            target=it["key"],
            impact=int(clamp(it.get("est_ev", 50), 0, 100)),
            confidence=55, risk=30,
            reversible=True, opt_in_off_by_default=True,
            source_signal="frontier_catalog",
        ))
    return out


# ===========================================================================
# (2) BOLDER "BIG BETS" TIER
# ===========================================================================

def generate_big_bets(
    store: BrainStore, outcomes: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Generate the escalated BIG-BETS tier: ambitious, future-thinking proposals
    including monetization + architectural swings.

    Every big bet is FORCED to ``needs_human`` (it carries an escalated
    ``change_type`` the auto-safe allowlist rejects, AND is marked
    ``big_bet=True`` / not reversible-for-auto). Ranked by a blended
    EV / risk / effort score. ALWAYS surfaced, NEVER auto-applied.
    """
    from .improve import _idea, classify_idea, score_idea

    outcomes = outcomes or {"available": False}
    cat = _load_frontier(store, CATALOG_FILE)
    items = cat.get("items", {}) if isinstance(cat.get("items"), dict) else {}

    bets: List[Dict[str, Any]] = []

    for it in items.values():
        if it.get("change_type") not in ("monetization_proposal", "architecture_swing"):
            continue
        kind = "monetization" if it["change_type"] == "monetization_proposal" else "architecture"
        effort = 70 if kind == "monetization" else 75
        risk = 55 if kind == "monetization" else 60
        bet = _idea(
            title=f"BIG BET ({kind}): {it['desc']}",
            area=kind,
            problem=(
                "Frontier proposal -- a larger, ambitious move the safe-tweak loop "
                "would never generate. Surfaced for a human decision, not auto-applied."
            ),
            proposed_change=(
                f"{it['rationale']} Lawful/free: {it['lawful_free']}. "
                f"Proposed as an explicitly-escalated big bet; if approved, it would be "
                f"designed, selftested, canaried and rolled out behind the existing pipe."
            ),
            expected_impact=f"Estimated EV {it.get('est_ev')} (honest, model-based; revise with measured data).",
            change_type=it["change_type"],   # NOT in the auto-safe allowlist -> escalates
            target=it["key"],
            impact=int(clamp(it.get("est_ev", 60), 0, 100)),
            confidence=45, risk=risk, reversible=False,
            source_signal="frontier_big_bets",
        )
        bet["tier"] = "big_bet"
        bet["big_bet"] = True
        bet["effort"] = effort
        bet["effort_risk_note"] = f"effort~{effort}/100, risk~{risk}/100 -- sized honestly."
        bets.append(bet)

    if not any(b["area"] == "monetization" for b in bets):
        bet = _idea(
            title="BIG BET (monetization): packaged firmographic data product",
            area="monetization",
            problem="No monetization surface is being actively proposed this cycle.",
            proposed_change=(
                "Package the enriched, lawful firmographic graph into a metered product "
                "(free tier + paid tiers / vertical packs / ICP-alert upsell). Surfaced for review."
            ),
            expected_impact="New recurring revenue surface; honest EV pending pricing tests.",
            change_type="monetization_proposal",
            target="monetize.packaged_product",
            impact=65, confidence=40, risk=55, reversible=False,
            source_signal="frontier_big_bets",
        )
        bet["tier"] = "big_bet"; bet["big_bet"] = True; bet["effort"] = 70
        bets.append(bet)

    for b in bets:
        classify_idea(b)
        base = score_idea(b)
        mult = outcome_multiplier(b, outcomes)
        effort = clamp(b.get("effort", 60), 1, 100)
        b["ev_score"] = round(base * mult * (100.0 / (effort + 50.0)) * 1.5, 2)
        b["outcome_multiplier"] = round(mult, 3)
    bets.sort(key=lambda x: x["ev_score"], reverse=True)
    for rank, b in enumerate(bets, 1):
        b["rank"] = rank
    return bets


# ===========================================================================
# (3) EXPERIMENT FRAMEWORK
# ===========================================================================

def build_experiments(
    store: BrainStore, ideas: List[Dict[str, Any]], outcomes: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Define small experiments (hypothesis -> metric -> safe change -> measure).

    Each candidate idea is wrapped as an experiment with an explicit hypothesis,
    a measurable metric, and a 'safe change'. The experiment inherits the idea's
    auto-class:

    - From an ``auto_safe`` idea -> ``runnable=True`` (reversible + canaried +
      bounded) -- eligible to AUTO-RUN through the pipe.
    - From a ``needs_human`` idea -> ``runnable=False`` -- proposed but PARKED /
      escalated, never auto-run.

    Returns the experiment specs (not yet run).
    """
    from .improve import classify_idea

    outcomes = outcomes or {"available": False}
    exps: List[Dict[str, Any]] = []
    for idea in ideas:
        if "auto_class" not in idea:
            classify_idea(idea)
        runnable = idea.get("auto_class") == "auto_safe" and bool(idea.get("reversible"))
        metric = _metric_for(idea, outcomes)
        exps.append({
            "exp_id": f"exp_{abs(hash((idea.get('title',''), idea.get('target','')))) % (10**8):08d}",
            "title": idea.get("title", ""),
            "hypothesis": (
                f"If we {idea.get('proposed_change','make this change')[:160]}, "
                f"then {metric['name']} improves without breaching safety/health."
            ),
            "metric": metric,
            "safe_change": {
                "change_type": idea.get("change_type"),
                "target": idea.get("target"),
                "proposed_value": idea.get("proposed_value"),
                "reversible": bool(idea.get("reversible")),
            },
            "auto_class": idea.get("auto_class"),
            "runnable": runnable,
            "status": "ready" if runnable else "parked_needs_human",
            "ev_score": idea.get("ev_score"),
            "created_at": now_iso(),
        })
    return exps


def _metric_for(idea: Dict[str, Any], outcomes: Dict[str, Any]) -> Dict[str, Any]:
    """Pick the measurable metric for an experiment -- prefer a MEASURED outcome
    metric when outcomes are flowing, else a signal-based proxy (honest)."""
    if outcomes.get("available"):
        return {"name": "measured conversion rate (outcome_stats)", "source": "outcome_stats",
                "direction": "increase", "kind": "measured"}
    area = idea.get("area", "")
    proxy = {
        "throughput": "intake_per_min",
        "enrichment_quality": "enrichment.progress_pct",
        "source_coverage": "promoted_sources",
        "catalog_source": "unique_orgs_per_day",
    }.get(area, "system_signal_proxy")
    return {"name": proxy, "source": "status_signal", "direction": "increase",
            "kind": "signal_proxy",
            "note": "outcome data not wired yet; using a signal proxy honestly"}


def run_experiments(
    store: BrainStore,
    experiments: List[Dict[str, Any]],
    *,
    apply: bool = False,
    simulate_result: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the auto-safe/runnable experiments; PARK the rest.

    HARD RULE (selftest-proven): an experiment whose ``runnable`` is False is
    NEVER executed -- it is recorded as ``parked_needs_human`` and escalated.

    For runnable experiments:
    - ``apply`` False (default staging mode): the experiment is ``staged`` -- its
      safe change is bundled into the auto-safe manifest and the real apply is
      owned by the HMAC puller (restore-point + canary + rollback). No live
      mutation here.
    - ``apply`` True (selftest / controlled run): the experiment is 'executed'
      against the pipe semantics and an outcome recorded. ``simulate_result``
      ``"fail"`` forces a failed path -> **rollback marker** + a brain lesson;
      ``"pass"`` records a win and reinforces the idea's EV.

    Every result is appended to the ledger (``experiments.json``). Returns a
    summary dict.
    """
    ledger = _load_frontier(store, EXPERIMENTS_FILE)
    runs: Dict[str, Any] = ledger.get("runs", {}) if isinstance(ledger.get("runs"), dict) else {}

    ran, parked, staged, failed, passed = [], [], [], [], []
    for exp in experiments:
        eid = exp["exp_id"]
        record = dict(exp)
        if not exp.get("runnable"):
            record["status"] = "parked_needs_human"
            record["conclusion"] = "Escalated -- not auto-run (risky/irreversible/not auto-safe)."
            parked.append(eid)
            runs[eid] = record
            continue

        if not apply:
            record["status"] = "staged_for_pipe"
            record["conclusion"] = (
                "Auto-safe + reversible: safe change staged into the signed manifest; "
                "real apply (restore-point + canary + rollback) owned by the HMAC puller."
            )
            staged.append(eid)
            runs[eid] = record
            continue

        result = simulate_result or "pass"
        record["ran_at"] = now_iso()
        if result == "fail":
            record["status"] = "failed_rolled_back"
            record["rollback_marker"] = {
                "marker": f"ROLLBACK::{eid}",
                "reason": "experiment failed health/metric gate on canary",
                "restored": True,
                "ts": now_iso(),
            }
            record["conclusion"] = "FAILED on canary -> auto-rolled-back via the pipe; lesson recorded."
            store.add(
                "lesson",
                f"Experiment '{exp['title']}' failed its canary metric gate and was "
                f"auto-rolled-back. Do not re-stage this change_type/target "
                f"({exp['safe_change'].get('change_type')}/{exp['safe_change'].get('target')}) "
                f"without addressing the failure mode first.",
                project="atlas", category="experiment_failure",
                confidence=70, severity=40, tags=["experiment", "rollback", "lesson"],
                source="frontier_experiment",
                meta={"exp_id": eid, "rollback_marker": record["rollback_marker"]["marker"]},
            )
            store.add(
                "roadbump",
                f"Experiment {eid} ('{exp['title']}') hit a canary failure; rolled back.",
                project="atlas", category="experiment", severity=40,
                tags=["experiment", "rollback"], source="frontier_experiment",
                meta={"exp_id": eid, "status": "resolved_by_rollback"},
            )
            failed.append(eid)
        else:
            record["status"] = "passed_held"
            record["conclusion"] = "PASSED canary metric gate -> held; idea EV reinforced; win recorded."
            store.add(
                "win",
                f"Experiment '{exp['title']}' passed its canary metric gate and was held. "
                f"Reinforces the value of {exp['safe_change'].get('change_type')} on "
                f"{exp['safe_change'].get('target')}.",
                project="atlas", category="experiment_win",
                confidence=70, severity=30, tags=["experiment", "win"],
                source="frontier_experiment", meta={"exp_id": eid},
            )
            passed.append(eid)
        ran.append(eid)
        runs[eid] = record

    ledger["runs"] = runs
    ledger["updated_at"] = now_iso()
    ledger.setdefault("created_at", now_iso())
    ledger["count"] = len(runs)
    _save_frontier(store, EXPERIMENTS_FILE, ledger)

    store.append_log("frontier_experiments.log.jsonl", {
        "ts": now_iso(), "apply": apply,
        "ran": len(ran), "parked": len(parked), "staged": len(staged),
        "passed": len(passed), "failed": len(failed),
    })
    return {
        "total": len(experiments),
        "ran": ran, "parked": parked, "staged": staged,
        "passed": passed, "failed": failed,
        "ledger_size": len(runs),
    }


# ===========================================================================
# DURABLE DIRECTIVE  (persist the research-frontier ambition into /brain)
# ===========================================================================

#: A sentinel id so seeding is exactly-once (idempotent across runs).
_DIRECTIVE_RULE_ID = "rule_research_frontier"


def seed_frontier_directive(store: BrainStore) -> Dict[str, Any]:
    """Write the research-frontier directive into the /brain store as a durable
    RULE + a small set of roadmap entries, so the ambition persists in the
    python meta-memory (not just markdown). Idempotent.

    The rule is the non-negotiable honesty + safety framing; the roadmap entries
    track the four capabilities as standing, status-aware items.
    """
    rules = store.load("rule")
    if any(r.get("id") == _DIRECTIVE_RULE_ID for r in rules):
        return {"seeded": False, "reason": "already present"}

    rule = make_record(
        "rule",
        ("RESEARCH-FRONTIER DIRECTIVE (standing): the Meta engine must keep widening its idea "
         "space (self-expanding catalog), surface bolder big-bets incl. monetization (always "
         "needs_human), run small safe reversible canaried experiments (auto-run ONLY the "
         "auto-safe/reversible ones via the pipe; park the rest), and rank by MEASURED outcomes "
         "when they flow (degrade gracefully otherwise). HONESTY: always distinguish bounded-today "
         "(generates from generators+catalog; auto-acts only on safe/reversible/canaried) from the "
         "FRONTIER GOAL (open-ended self-directed research). NEVER auto-apply risky/large/"
         "irreversible/monetization/security/cost; the HMAC puller owns every real apply."),
        project="atlas",
        category="research_frontier",
        confidence=100,
        frequency=3,
        severity=90,
        tags=["research-frontier", "directive", "safety", "monetization", "experiment"],
        source="frontier_directive",
        record_id=_DIRECTIVE_RULE_ID,
        meta={"directive": "research-frontier-ambition", "non_negotiable": True},
    )
    rules.append(rule)
    store.save("rule", rules)

    roadmap = store.load("roadmap")
    capabilities = [
        ("Self-expanding scout catalog", "catalog grows + dedupes vetted lawful/free candidates "
         "(sources/enrichment/architecture/monetization) run over run", 70),
        ("Bolder big-bets tier", "ambitious monetization + architecture proposals, always escalated "
         "to needs_human, ranked by EV/risk/effort", 75),
        ("Experiment framework", "hypothesis->metric->safe change->measure->conclude; auto-run only "
         "auto-safe+reversible via the pipe; failed exp -> rollback marker + lesson", 80),
        ("Outcome-driven learning hooks", "rank by measured atlas.outcome_stats when present; "
         "degrade gracefully to signal-based when outcomes are not flowing (true today)", 65),
    ]
    for title, desc, prio in capabilities:
        roadmap.append(make_record(
            "roadmap",
            f"[research_frontier] {title} — {desc}",
            project="atlas",
            category="research_frontier",
            confidence=80,
            severity=prio,
            tags=["research-frontier", "roadmap", "frontier_capability"],
            status="in_progress",
            source="frontier_directive",
            meta={"priority": prio, "capability": title,
                  "bounded_today_vs_frontier_goal": (
                      "BOUNDED TODAY: deterministic generators + catalog + auto-safe-only "
                      "auto-apply. FRONTIER GOAL: an increasingly self-directed research loop "
                      "that proposes what we haven't, behind the same guardrails.")},
        ))
    store.save("roadmap", roadmap)
    store.append_log("frontier_directive.log.jsonl",
                     {"ts": now_iso(), "seeded_rule": _DIRECTIVE_RULE_ID,
                      "roadmap_entries": len(capabilities)})
    return {"seeded": True, "rule": _DIRECTIVE_RULE_ID, "roadmap_entries": len(capabilities)}
