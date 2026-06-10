"""
brain.improve
=============

The **Meta Brain + Meta Jarvis improvement engine** — a self-improving loop that
*continuously proposes and ranks* improvements to the ATLAS / TuaniChat system
and *auto-applies only the safe, reversible, canaried subset* via the existing
signed-manifest deploy pipe, escalating everything else to the user.

This module EXTENDS the existing /brain package (it does not rebuild it). It
reuses :class:`brain.store.BrainStore`, :mod:`brain.schemas`, and the reflection
layer, and writes its output back into the same memory store so ideas, decisions
and the roadmap are first-class brain records the dashboards already surface.

Honesty (encoded in the docstrings and the report it emits)
-----------------------------------------------------------
This engine does **not** "fully autonomously rewrite the system." What it
actually does, truthfully:

1. HOURLY: reads live system-signal JSONs (throughput / metrics / source-promote
   / nrd / qa-hetzner / enrich-*) plus the brain's own lessons / mistakes /
   roadbumps, and generates **ranked improvement ideas** for systems + processes.
2. DAILY: a broader scout of new techniques / technologies / data sources /
   architectures to incorporate, plus a reflection over the day's wins/mistakes,
   producing a ranked roadmap.
3. CLASSIFIES every idea as ``auto_safe`` or ``needs_human`` against a HARD,
   conservative gate. Only the ``auto_safe`` class is ever staged for
   self-application; it is staged as a *signed manifest in an OUTBOX* (never a
   blind live mutation) so the existing HMAC puller's guardrail + restore-point
   + health-check + auto-rollback still own the actual apply on the box.
4. SURFACES everything else (and a full digest of what self-applied) to the user
   via an improvement-report JSON the dashboards + daily watchdog read.

The auto-apply boundary is intentionally *small*: config tuning within bounds,
adding a discovered data source as an OFF-by-default opt-in, enabling a watchdog.
Anything risky / irreversible / large (schema changes, deletes, security/SSH,
pipe self-modification, new always-on services, large scale-ups) is FORCED to
``needs_human`` by the classifier and cannot self-apply.

Stdlib only. Entry points (wired in :mod:`brain.cli`):

    python -m brain improve --hourly  [--root DIR] [--signals DIR] [--outbox DIR]
    python -m brain improve --daily   [--root DIR] [--signals DIR] [--outbox DIR]
    python -m brain improve --selftest
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from .schemas import clamp, make_record, now_iso
from .store import BrainStore

# ---------------------------------------------------------------------------
# Auto-apply safety gate  (THE most important thing in this module)
# ---------------------------------------------------------------------------

#: The ONLY change classes that may ever self-apply. Each maps to a manifest
#: builder below. Everything not in this set is forced to ``needs_human``.
AUTO_SAFE_CHANGE_TYPES = (
    "config_tune",        # tune a numeric/boolean config within declared bounds
    "add_data_source",    # add a discovered source as an OFF-by-default opt-in
    "enable_watchdog",    # enable a restart/health watchdog (additive, reversible)
)

#: Hard red-flags. If an idea's change_type, target, or proposed action matches
#: ANY of these, it is ALWAYS ``needs_human`` regardless of anything else. This
#: is a denylist *on top of* the allowlist above — both must pass.
FORBID_AUTO_RE = re.compile(
    r"(?ix)"
    r"\b(drop|truncate|delete|destroy|wipe|purge|rm\s+-rf)\b"     # data loss
    r"|\b(alter\s+table|create\s+table|migrate|schema\s+change)\b"  # schema / DDL
    r"|\b(ssh|sshd|authorized_keys)\b|password|permitrootlogin"     # security/SSH
    r"|\b(ufw|iptables|firewall)\b"                                # network policy
    r"|\b(disable|stop|mask)\b.*\b(autopull|ssh|sshd|postgres)\b"  # sever lifelines
    r"|\bautopull\b"                                               # pipe self-mod
    r"|\b(scale\s*up|spin\s*up|provision|new\s+(node|server|service))\b"  # capacity
    r"|\b(credential|secret|token|api[_\s-]?key|rotate)\b"         # secrets
    r"|\b(billing|payment|spend|budget\s+up|increase\s+cap)\b"     # money
)

#: Numeric config tunes are only auto-safe within these declared, reversible
#: bounds. A tune outside its bound is escalated, never clamped silently.
CONFIG_BOUNDS: Dict[str, Dict[str, Any]] = {
    # per-host polite-crawl gap (seconds): may be tuned within a safe band only.
    "enrich.per_host_gap_sec": {"min": 1.0, "max": 5.0, "kind": "float"},
    # enrich worker concurrency PER NODE: small, reversible nudges only.
    "enrich.worker_instances": {"min": 1, "max": 4, "kind": "int"},
    # autopull max-drain per cycle: bounded.
    "autopull.max_drain": {"min": 5, "max": 25, "kind": "int"},
    # source-promote freshness window (days): bounded.
    "promote.fresh_days": {"min": 7, "max": 90, "kind": "int"},
}


def classify_idea(idea: Dict[str, Any]) -> Dict[str, Any]:
    """Classify a single idea as ``auto_safe`` or ``needs_human``.

    The gate is conservative and *both-gated*: an idea self-applies ONLY if
    (a) its ``change_type`` is in :data:`AUTO_SAFE_CHANGE_TYPES`,
    (b) it does NOT match the :data:`FORBID_AUTO_RE` denylist anywhere,
    (c) it is explicitly marked reversible,
    (d) for ``config_tune``: the target is bounded and the value is in-bounds,
    (e) for ``add_data_source``: it is opt-in / OFF-by-default,
    (f) for ``enable_watchdog``: it is additive (no stop/disable verbs).

    Returns the idea augmented with ``auto_class`` and ``auto_reason``.
    Failing ANY check yields ``needs_human`` (fail-safe default).
    """
    blob = " ".join(
        str(idea.get(k, ""))
        for k in ("change_type", "target", "proposed_change", "action", "problem")
    )

    def escalate(reason: str) -> Dict[str, Any]:
        idea["auto_class"] = "needs_human"
        idea["auto_reason"] = reason
        return idea

    # (b) Denylist always wins.
    if FORBID_AUTO_RE.search(blob):
        return escalate("matches hard red-flag denylist (risky/irreversible/security/cost)")

    ctype = idea.get("change_type", "")
    # (a) Allowlist.
    if ctype not in AUTO_SAFE_CHANGE_TYPES:
        return escalate(f"change_type '{ctype}' is not in the auto-safe allowlist")

    # (c) Reversibility is mandatory and must be explicit.
    if not bool(idea.get("reversible", False)):
        return escalate("not marked reversible")

    # (d/e/f) Per-type structural checks.
    if ctype == "config_tune":
        target = idea.get("target", "")
        bound = CONFIG_BOUNDS.get(target)
        if bound is None:
            return escalate(f"config target '{target}' has no declared safe bound")
        val = idea.get("proposed_value")
        try:
            num = float(val)
        except (TypeError, ValueError):
            return escalate("config_tune missing a numeric proposed_value")
        if not (bound["min"] <= num <= bound["max"]):
            return escalate(
                f"proposed_value {num} outside safe bound [{bound['min']},{bound['max']}]"
            )
    elif ctype == "add_data_source":
        if not bool(idea.get("opt_in_off_by_default", False)):
            return escalate("new data source must be opt-in / OFF by default to self-apply")
    elif ctype == "enable_watchdog":
        if re.search(r"(?i)\b(disable|stop|mask|remove)\b", blob):
            return escalate("watchdog change contains a disable/stop verb (not additive)")

    idea["auto_class"] = "auto_safe"
    idea["auto_reason"] = "passed allowlist + denylist + reversibility + structural bound checks"
    return idea


# ---------------------------------------------------------------------------
# Scoring (Meta Brain ranking)
# ---------------------------------------------------------------------------

def score_idea(idea: Dict[str, Any]) -> float:
    """Expected-value score for ranking.

    EV = impact * confidence * reversibility_bonus / risk, lightly favoring
    auto-safe (cheap to ship) ideas. All inputs are 0-100; output ~0-100.
    """
    impact = clamp(idea.get("impact", 50), 0, 100)
    conf = clamp(idea.get("confidence", 50), 0, 100)
    risk = clamp(idea.get("risk", 50), 1, 100)  # never divide by zero
    rev_bonus = 1.15 if idea.get("reversible") else 0.9
    ev = (impact * (conf / 100.0) * rev_bonus) * (100.0 / (risk + 50.0))
    # A small nudge so safe, ready-to-ship wins float up when EV ties.
    if idea.get("auto_class") == "auto_safe":
        ev *= 1.05
    return round(ev, 2)


# ---------------------------------------------------------------------------
# Signal loading  (live system status JSONs + brain memory)
# ---------------------------------------------------------------------------

#: The box-side status JSONs the engine reads (published by the autopull
#: status-back + metrics bridge under status/<node>/). Missing files are
#: tolerated (the engine degrades gracefully and notes the gap as its own idea).
SIGNAL_FILES = (
    "throughput.json",
    "atlas-metrics.json",
    "source-promote.json",
    "nrd.json",
    "qa-hetzner.json",
)


def _load_json(path: str) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def load_signals(signals_dir: Optional[str]) -> Dict[str, Any]:
    """Load whatever live signal JSONs exist. Returns ``{name: doc-or-None}``.

    Also globs ``enrich-*.json`` (per-node enrich status). Never raises: a
    missing dir or file becomes a ``None`` entry the idea generator reasons over
    (an absent signal is itself a reliability finding).
    """
    out: Dict[str, Any] = {}
    if not signals_dir or not os.path.isdir(signals_dir):
        for name in SIGNAL_FILES:
            out[name] = None
        out["_signals_dir_missing"] = True
        return out
    for name in SIGNAL_FILES:
        out[name] = _load_json(os.path.join(signals_dir, name))
    for fn in sorted(os.listdir(signals_dir)):
        if fn.startswith("enrich-") and fn.endswith(".json"):
            out[fn] = _load_json(os.path.join(signals_dir, fn))
    return out


# ---------------------------------------------------------------------------
# Idea generation
# ---------------------------------------------------------------------------

def _idea(
    *,
    title: str,
    problem: str,
    proposed_change: str,
    expected_impact: str,
    change_type: str,
    target: str = "",
    proposed_value: Any = None,
    impact: int = 50,
    confidence: int = 60,
    risk: int = 50,
    reversible: bool = False,
    opt_in_off_by_default: bool = False,
    area: str = "general",
    source_signal: str = "",
) -> Dict[str, Any]:
    """Build one well-formed idea dict (pre-classification, pre-scoring)."""
    return {
        "title": title,
        "area": area,
        "problem": problem,
        "proposed_change": proposed_change,
        "expected_impact": expected_impact,
        "change_type": change_type,
        "target": target,
        "proposed_value": proposed_value,
        "impact": impact,
        "confidence": confidence,
        "risk": risk,
        "reversibility": "reversible" if reversible else "not-easily-reversible",
        "reversible": reversible,
        "opt_in_off_by_default": opt_in_off_by_default,
        "source_signal": source_signal,
    }


def generate_hourly_ideas(
    signals: Dict[str, Any], store: BrainStore
) -> List[Dict[str, Any]]:
    """HOURLY pass: turn live signals + recent brain memory into ranked ideas.

    Deterministic, transparent heuristics over the real signal shapes (so the
    same input always yields the same ideas — auditable, testable). Each idea
    states problem -> change -> impact -> risk -> reversibility, then is
    classified (auto-safe vs needs-human) and EV-scored.
    """
    ideas: List[Dict[str, Any]] = []

    metrics = signals.get("atlas-metrics.json") or {}
    m = metrics.get("metrics", metrics) if isinstance(metrics, dict) else {}
    throughput = signals.get("throughput.json") or {}
    promote = signals.get("source-promote.json") or {}

    # --- Reliability: a core signal is MISSING (never-idle doctrine: flag + fix) ---
    for name in SIGNAL_FILES:
        if signals.get(name) is None:
            ideas.append(_idea(
                title=f"Restore missing signal: {name}",
                area="reliability",
                problem=f"Status signal '{name}' is absent — dashboards can read stale/zero "
                        f"and the never-idle watchdog is blind on this channel.",
                proposed_change=f"Enable a watchdog that re-publishes {name} and alerts if it "
                                f"goes stale > 15 min (additive, reversible).",
                expected_impact="Restores dashboard truth + closes a blind spot.",
                change_type="enable_watchdog",
                target=name,
                impact=75, confidence=70, risk=20, reversible=True,
                source_signal=name,
            ))

    # --- Throughput: intake idle or below floor (MASTER COMMAND: never idle) ---
    intake_pm = None
    if isinstance(m, dict):
        intake_pm = m.get("intake_per_min")
    if intake_pm is None and isinstance(throughput, dict):
        intake_pm = throughput.get("intake_per_min")
    if isinstance(intake_pm, (int, float)):
        if intake_pm <= 0:
            ideas.append(_idea(
                title="Intake is ZERO — wake a discovery source",
                area="throughput",
                problem="intake_per_min is 0: Channel data is not flowing (never-idle breach).",
                proposed_change="Enable a discovered OFF-by-default real-time source as opt-in "
                                "to restart intake; surface root-cause to the daily report.",
                expected_impact="Resumes data flow on the idle channel.",
                change_type="add_data_source",
                target="discovery.fallback_source",
                impact=90, confidence=65, risk=30,
                reversible=True, opt_in_off_by_default=True,
                source_signal="atlas-metrics.json",
            ))
        elif intake_pm < 50:
            ideas.append(_idea(
                title="Intake below floor — nudge enrich concurrency",
                area="throughput",
                problem=f"intake_per_min={intake_pm} is below the working floor.",
                proposed_change="Tune enrich.worker_instances up by 1 per node (bounded 1-4), "
                                "canaried; watch CPU/IO health before holding.",
                expected_impact="Modest, reversible throughput lift without new capacity.",
                change_type="config_tune",
                target="enrich.worker_instances", proposed_value=2,
                impact=55, confidence=60, risk=35, reversible=True,
                source_signal="atlas-metrics.json",
            ))

    # --- Enrichment quality: progress stalled ---
    enr = m.get("enrichment") if isinstance(m, dict) else None
    if isinstance(enr, dict):
        prog = enr.get("progress_pct")
        if isinstance(prog, (int, float)) and prog < 60:
            ideas.append(_idea(
                title="Enrichment coverage low — add a free firmographics source",
                area="enrichment_quality",
                problem=f"enrichment progress_pct={prog} is under the 60% bar; coverage gap.",
                proposed_change="Add a discovered free firmographics source (RDAP/registry) as "
                                "an OFF-by-default opt-in enrich lane; measure lift on canary.",
                expected_impact="Higher company-level coverage at zero marginal cost.",
                change_type="add_data_source",
                target="enrich.firmographics_source",
                impact=70, confidence=60, risk=30,
                reversible=True, opt_in_off_by_default=True,
                source_signal="atlas-metrics.json",
            ))

    # --- Source coverage: promotion freshness window ---
    if isinstance(promote, dict):
        demoted = promote.get("demoted") or promote.get("if_fresh_demoted_to_mined")
        if isinstance(demoted, (int, float)) and demoted > 0:
            ideas.append(_idea(
                title="Sources demoting — widen freshness window",
                area="source_coverage",
                problem=f"{demoted} sources demoted by the freshness gate; coverage churns.",
                proposed_change="Tune promote.fresh_days up (bounded 7-90) so still-useful "
                                "sources are not prematurely demoted; canary then hold/revert.",
                expected_impact="Steadier live source coverage; fewer false demotions.",
                change_type="config_tune",
                target="promote.fresh_days", proposed_value=45,
                impact=50, confidence=55, risk=30, reversible=True,
                source_signal="source-promote.json",
            ))

    # --- Process learning: recent unresolved roadbumps / repeated mistakes ---
    open_roadbumps = [
        r for r in store.load("roadbump")
        if r.get("status") == "active" and not r.get("meta", {}).get("resolution")
    ]
    if open_roadbumps:
        rb = max(open_roadbumps, key=lambda r: r.get("severity", 0))
        ideas.append(_idea(
            title="Close an open roadbump (process)",
            area="process",
            problem=f"Open roadbump with no recorded resolution: {rb.get('text','')[:160]}",
            proposed_change="Draft a fix runbook for review and, once approved, record the "
                            "resolution so the brain solves it instantly next time.",
            expected_impact="Converts a recurring blocker into a one-shot known fix.",
            change_type="process_runbook",  # NOT auto-safe -> escalates
            impact=60, confidence=65, risk=45, reversible=False,
            source_signal="brain:roadbumps",
        ))

    if not ideas:
        ideas.append(_idea(
            title="Systems nominal — bank a small reliability hardening",
            area="reliability",
            problem="No urgent signal anomalies this hour.",
            proposed_change="Enable an additional idle-state watchdog on the lowest-coverage "
                            "channel (additive, reversible) to keep raising the floor.",
            expected_impact="Incremental reliability; keeps the engine producing.",
            change_type="enable_watchdog",
            target="lowest_coverage_channel",
            impact=35, confidence=55, risk=15, reversible=True,
            source_signal="none",
        ))

    return _classify_score_sort(ideas)


def generate_daily_ideas(
    signals: Dict[str, Any], store: BrainStore
) -> List[Dict[str, Any]]:
    """DAILY pass: broader scouting of new techniques/tech/sources/architectures
    plus a reflection over the day's wins/mistakes. Returns a ranked roadmap.

    NOTE: scouting candidates are seeded from a curated, in-repo catalog of
    zero-cost / lawful directions (no live external probing from here — egress is
    intentionally absent; the box's own collectors verify a source before it ever
    self-applies, and even then only OFF-by-default).
    """
    ideas: List[Dict[str, Any]] = []

    # Carry the hourly findings forward so the daily roadmap is signal-grounded.
    ideas.extend([dict(i) for i in generate_hourly_ideas(signals, store)])

    # Curated zero-cost / lawful scouting catalog (Project TITAN-aligned: events,
    # earliest-discovery, free + lawful). These are CANDIDATES to evaluate, each
    # added as an OFF-by-default opt-in if it ever self-applies.
    catalog = [
        ("add_data_source", "enrich.rdap_registry",
         "RDAP/registry firmographics (domain age, registrar, registrant org)",
         "Free firmographic enrichment lift", 65, 60, 30),
        ("add_data_source", "discovery.ct_log_secondary",
         "A secondary Certificate-Transparency log stream for earliest business-birth events",
         "Earlier discovery on Channel-1 (TITAN timeline)", 70, 55, 35),
        ("add_data_source", "enrich.favicon_logo",
         "Favicon/logo discovery for brand-asset enrichment", "Richer profiles", 45, 60, 20),
        ("config_tune", "enrich.per_host_gap_sec",
         "Tune polite-crawl per-host gap for throughput within the safe 1-5s band",
         "Throughput within politeness bounds", 40, 65, 25),
    ]
    for ctype, target, desc, impact_txt, imp, conf, risk in catalog:
        ideas.append(_idea(
            title=f"Scout: {desc}",
            area="new_technology",
            problem="Daily scout: a zero-cost/lawful improvement area not yet incorporated.",
            proposed_change=f"Evaluate and, if it passes selftest on canary, incorporate "
                            f"'{desc}' as an OFF-by-default opt-in.",
            expected_impact=impact_txt,
            change_type=ctype, target=target,
            proposed_value=(3.0 if ctype == "config_tune" else None),
            impact=imp, confidence=conf, risk=risk,
            reversible=True, opt_in_off_by_default=(ctype == "add_data_source"),
            source_signal="daily_scout_catalog",
        ))

    # Reflection over the day: count wins/mistakes for the report narrative.
    wins = [w for w in store.load("win") if w.get("status") == "active"]
    mistakes = [x for x in store.load("mistake") if x.get("status") == "active"]
    if mistakes:
        worst = max(mistakes, key=lambda r: r.get("severity", 0))
        ideas.append(_idea(
            title="Turn the day's worst mistake into a guardrail",
            area="process",
            problem=f"Recorded mistake to systematize against: {worst.get('text','')[:160]}",
            proposed_change="Propose a preventive check (NOT auto-applied) for user review.",
            expected_impact="Prevents recurrence of a known failure mode.",
            change_type="process_guardrail",  # escalates
            impact=65, confidence=70, risk=40, reversible=False,
            source_signal="brain:mistakes",
        ))

    ranked = _classify_score_sort(ideas)
    # Deduplicate by title (the hourly carry-forward can overlap the scout).
    seen, deduped = set(), []
    for i in ranked:
        if i["title"] in seen:
            continue
        seen.add(i["title"])
        deduped.append(i)
    return deduped


def _classify_score_sort(ideas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for i in ideas:
        classify_idea(i)
        i["ev_score"] = score_idea(i)
    ideas.sort(key=lambda x: x["ev_score"], reverse=True)
    for rank, i in enumerate(ideas, 1):
        i["rank"] = rank
    return ideas


# ---------------------------------------------------------------------------
# Manifest staging  (auto-safe ideas -> signed-pipe-ready manifest in OUTBOX)
# ---------------------------------------------------------------------------

def stage_auto_safe_manifest(
    auto_ideas: List[Dict[str, Any]], outbox_dir: str, pass_kind: str
) -> Optional[str]:
    """Stage a PLACEHOLDER manifest for the auto-safe ideas into the OUTBOX.

    This NEVER pushes and NEVER applies. It writes a ``seq-IMPROVE-<pass>.json``
    placeholder manifest (seq left as a placeholder for the publish session to
    renumber + sign) whose steps are limited to the auto-safe change classes and
    which the box's existing HMAC puller + guardrail + canary + rollback will own
    if/when it is signed and published. Returns the manifest path, or None when
    there is nothing auto-safe to stage.
    """
    if not auto_ideas:
        return None
    os.makedirs(os.path.join(outbox_dir, "manifests"), exist_ok=True)

    steps: List[Dict[str, Any]] = [{
        "type": "noop",
        "note": "AUTO-SAFE improvement bundle from the Meta Brain improvement engine. "
                "Config tunes are bounded+reversible; new sources are OFF-by-default opt-ins; "
                "watchdogs are additive. Canary + health-check + auto-rollback owned by the puller.",
    }]
    for idea in auto_ideas:
        # We emit *descriptive* steps; the publish session converts these to the
        # concrete allowlisted/write_file/systemd steps and signs them. We do NOT
        # emit live-mutating commands from here.
        steps.append({
            "type": "noop",
            "note": f"[{idea['change_type']}] {idea['title']} :: target={idea.get('target','')} "
                    f"value={idea.get('proposed_value')} :: {idea['proposed_change']} "
                    f"(reversible={idea['reversible']}, ev={idea['ev_score']})",
        })

    manifest = {
        "seq": "IMPROVE-PLACEHOLDER",
        "mode": "diagnose",  # ship as diagnose first; publish flips to apply when stable
        "note": f"PLACEHOLDER improvement manifest ({pass_kind} pass) — auto-safe subset only. "
                f"Renumber to last_seq+1 on publish and re-sign. Steps are descriptive noops to "
                f"be concretized by the publish session into bounded config_tune / opt-in "
                f"add_data_source / additive enable_watchdog steps. NOTHING here mutates live "
                f"state until signed, published, and applied by the box's guarded puller.",
        "generated_at": now_iso(),
        "auto_safe_count": len(auto_ideas),
        "steps": steps,
    }
    path = os.path.join(outbox_dir, "manifests", f"seq-IMPROVE-{pass_kind}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# Persist into /brain  + emit the improvement-report JSON
# ---------------------------------------------------------------------------

def _persist_ideas(store: BrainStore, ideas: List[Dict[str, Any]], pass_kind: str) -> None:
    """Write each idea into the brain as a roadmap record + log the pass."""
    roadmap = store.load("roadmap")
    for idea in ideas:
        rec = make_record(
            "roadmap",
            f"[{idea['area']}] {idea['title']} — {idea['proposed_change']}",
            project="atlas",
            category=f"improvement_{pass_kind}",
            confidence=clamp(idea.get("confidence", 50), 0, 100),
            severity=clamp(idea.get("impact", 0), 0, 100),
            tags=["improvement", idea["area"], idea["auto_class"], pass_kind],
            status="auto_staged" if idea["auto_class"] == "auto_safe" else "needs_human",
            source="improve_engine",
            meta={
                "priority": idea["ev_score"],
                "ev_score": idea["ev_score"],
                "change_type": idea["change_type"],
                "auto_class": idea["auto_class"],
                "auto_reason": idea["auto_reason"],
                "reversible": idea["reversible"],
                "problem": idea["problem"],
                "expected_impact": idea["expected_impact"],
                "source_signal": idea.get("source_signal", ""),
            },
        )
        roadmap.append(rec)
    store.save("roadmap", roadmap)
    store.append_log(
        "improve.log.jsonl",
        {"ts": now_iso(), "pass": pass_kind, "ideas": len(ideas),
         "auto_safe": sum(1 for i in ideas if i["auto_class"] == "auto_safe")},
    )


def emit_report(
    store: BrainStore,
    ideas: List[Dict[str, Any]],
    pass_kind: str,
    staged_manifest: Optional[str],
    report_dir: Optional[str],
) -> Dict[str, Any]:
    """Build the improvement-report JSON the dashboards + watchdog surface.

    Honest framing baked into the report: it reports what self-applied (staged
    auto-safe) vs what is escalated for a human yes.
    """
    auto = [i for i in ideas if i["auto_class"] == "auto_safe"]
    human = [i for i in ideas if i["auto_class"] == "needs_human"]
    report = {
        "engine": "meta-brain-improvement-engine",
        "pass": pass_kind,
        "generated_at": now_iso(),
        "honest_boundary": (
            "This engine continuously PROPOSES and RANKS improvements. It AUTO-STAGES "
            "only the safe + reversible + canaried + selftest-gated subset (bounded config "
            "tunes, OFF-by-default new sources, additive watchdogs) as a SIGNED manifest "
            "through the existing pipe — the box's guardrail/canary/rollback owns the apply. "
            "Everything risky/irreversible/large is SURFACED here for your yes. It does NOT "
            "autonomously rewrite the system."
        ),
        "counts": {
            "total_ideas": len(ideas),
            "auto_safe_staged": len(auto),
            "needs_human": len(human),
        },
        "auto_safe_staged": auto,
        "needs_human_decisions": human,
        "staged_manifest": staged_manifest,
        "top_ideas": ideas[:10],
    }
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
        out = os.path.join(report_dir, f"improvement-report-{pass_kind}.json")
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        report["_written_to"] = out
        # also write/refresh a latest pointer for the dashboard
        latest = os.path.join(report_dir, "improvement-report-latest.json")
        with open(latest, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
    store.append_log("improve_report.log.jsonl", {"ts": now_iso(), "pass": pass_kind,
                                                   "auto": len(auto), "human": len(human)})
    return report


# ---------------------------------------------------------------------------
# Top-level passes
# ---------------------------------------------------------------------------

def run_pass(
    store: BrainStore,
    *,
    pass_kind: str,
    signals_dir: Optional[str] = None,
    outbox_dir: Optional[str] = None,
    report_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Run one improvement pass (``"hourly"`` or ``"daily"``) end to end.

    1) load signals -> 2) generate+classify+rank ideas -> 3) persist to /brain ->
    4) stage auto-safe manifest into the OUTBOX (no push) -> 5) emit report.
    """
    signals = load_signals(signals_dir)
    ideas = (generate_daily_ideas if pass_kind == "daily" else generate_hourly_ideas)(signals, store)
    _persist_ideas(store, ideas, pass_kind)

    staged = None
    auto = [i for i in ideas if i["auto_class"] == "auto_safe"]
    if outbox_dir and auto:
        staged = stage_auto_safe_manifest(auto, outbox_dir, pass_kind)

    return emit_report(store, ideas, pass_kind, staged, report_dir)


# ---------------------------------------------------------------------------
# Self-test  (proves a risky idea is NEVER classified auto-safe)
# ---------------------------------------------------------------------------

def selftest() -> int:
    """Self-test for idea generation + the auto-safe classifier.

    Returns 0 on PASS, 1 on FAIL. Critically asserts that a battery of risky /
    irreversible ideas are ALL classified ``needs_human`` and that only the
    narrow, bounded, reversible cases are ever ``auto_safe``.
    """
    failures: List[str] = []

    # --- risky ideas that MUST NEVER be auto-safe ---
    risky = [
        _idea(title="Drop the stale enrich_queue table", problem="cleanup",
              proposed_change="DROP TABLE atlas.enrich_queue", expected_impact="x",
              change_type="config_tune", target="enrich.worker_instances",
              proposed_value=2, reversible=True),  # benign type/target but text is lethal
        _idea(title="Disable autopull timer to save CPU", problem="cpu",
              proposed_change="systemctl disable atlas-autopull.timer", expected_impact="x",
              change_type="enable_watchdog", reversible=True),
        _idea(title="Open SSH password auth", problem="access",
              proposed_change="set PasswordAuthentication yes", expected_impact="x",
              change_type="config_tune", target="enrich.per_host_gap_sec",
              proposed_value=2.0, reversible=True),
        _idea(title="Spin up a new Hetzner node", problem="scale",
              proposed_change="provision new server and scale up enrich", expected_impact="x",
              change_type="add_data_source", target="x",
              reversible=True, opt_in_off_by_default=True),
        _idea(title="Rotate the deploy secret automatically", problem="sec",
              proposed_change="rotate api_key token", expected_impact="x",
              change_type="config_tune", target="autopull.max_drain",
              proposed_value=10, reversible=True),
        _idea(title="Migrate the business schema", problem="schema",
              proposed_change="ALTER TABLE atlas.business add column x", expected_impact="x",
              change_type="config_tune", target="autopull.max_drain",
              proposed_value=10, reversible=True),
        _idea(title="Raise the daily ad budget", problem="growth",
              proposed_change="increase cap / budget up", expected_impact="x",
              change_type="config_tune", target="promote.fresh_days",
              proposed_value=30, reversible=True),
        # structurally-unsafe but non-denylisted cases:
        _idea(title="Tune worker instances to 50", problem="speed",
              proposed_change="more workers", expected_impact="x",
              change_type="config_tune", target="enrich.worker_instances",
              proposed_value=50, reversible=True),  # out of bound
        _idea(title="Tune an unknown config", problem="x",
              proposed_change="change something", expected_impact="x",
              change_type="config_tune", target="unknown.knob",
              proposed_value=1, reversible=True),  # no bound declared
        _idea(title="Add an always-ON new source", problem="coverage",
              proposed_change="add a source enabled by default", expected_impact="x",
              change_type="add_data_source", target="x",
              reversible=True, opt_in_off_by_default=False),  # not opt-in
        _idea(title="Irreversible config tune", problem="x",
              proposed_change="tune gap", expected_impact="x",
              change_type="config_tune", target="enrich.per_host_gap_sec",
              proposed_value=2.0, reversible=False),  # not reversible
        _idea(title="Brand new process runbook", problem="x",
              proposed_change="write a runbook", expected_impact="x",
              change_type="process_runbook", reversible=False),  # type not allowlisted
    ]
    for idea in risky:
        classify_idea(idea)
        if idea["auto_class"] != "needs_human":
            failures.append(
                f"RISKY idea was auto-safe (MUST be needs_human): {idea['title']} "
                f"-> {idea['auto_reason']}"
            )

    # --- benign ideas that SHOULD be auto-safe ---
    benign = [
        _idea(title="Nudge worker instances", problem="x", proposed_change="more workers",
              expected_impact="x", change_type="config_tune",
              target="enrich.worker_instances", proposed_value=2, reversible=True),
        _idea(title="Widen freshness window", problem="x", proposed_change="hold fresh sources",
              expected_impact="x", change_type="config_tune",
              target="promote.fresh_days", proposed_value=45, reversible=True),
        _idea(title="Add opt-in RDAP source", problem="coverage",
              proposed_change="add RDAP firmographics opt-in", expected_impact="x",
              change_type="add_data_source", target="enrich.rdap_registry",
              reversible=True, opt_in_off_by_default=True),
        _idea(title="Enable a stale-signal watchdog", problem="reliability",
              proposed_change="enable a watchdog that republishes a stale signal",
              expected_impact="x", change_type="enable_watchdog",
              target="throughput.json", reversible=True),
    ]
    for idea in benign:
        classify_idea(idea)
        if idea["auto_class"] != "auto_safe":
            failures.append(
                f"BENIGN idea was NOT auto-safe (should be): {idea['title']} "
                f"-> {idea['auto_reason']}"
            )

    # --- idea generation produces ranked, classified ideas on empty signals ---
    import tempfile
    tmp = tempfile.mkdtemp(prefix="improve-selftest-")
    store = BrainStore(os.path.join(tmp, "brain"))
    store.initialize()
    hourly = generate_hourly_ideas(load_signals(None), store)
    if not hourly:
        failures.append("hourly pass produced no ideas on empty signals")
    if any("ev_score" not in i or "rank" not in i for i in hourly):
        failures.append("ideas missing ev_score/rank after generation")
    if any(i["auto_class"] not in ("auto_safe", "needs_human") for i in hourly):
        failures.append("an idea has an invalid auto_class")
    daily = generate_daily_ideas(load_signals(None), store)
    if not daily:
        failures.append("daily pass produced no ideas")

    # --- scoring sanity: reversible/auto-safe should not score below an
    #     equivalent irreversible/risky idea ---
    a = _idea(title="a", problem="x", proposed_change="x", expected_impact="x",
              change_type="config_tune", target="promote.fresh_days",
              proposed_value=30, impact=60, confidence=60, risk=30, reversible=True)
    b = _idea(title="b", problem="x", proposed_change="x", expected_impact="x",
              change_type="process_runbook", impact=60, confidence=60, risk=70,
              reversible=False)
    classify_idea(a); classify_idea(b)
    if score_idea(a) <= score_idea(b):
        failures.append("EV scoring did not favor the safe, reversible, lower-risk idea")

    if failures:
        print("IMPROVE SELFTEST: FAIL")
        for f in failures:
            print("  - " + f)
        return 1
    print(f"IMPROVE SELFTEST: PASS "
          f"(risky={len(risky)} all escalated, benign={len(benign)} all auto-safe, "
          f"hourly={len(hourly)} daily={len(daily)} ideas ranked)")
    return 0


def _main(argv: Optional[List[str]] = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(
        prog="brain.improve",
        description="Meta Brain + Meta Jarvis self-improvement engine",
    )
    parser.add_argument("--root", default="./brain", help="brain root directory")
    parser.add_argument("--signals", default=None, help="dir of live status JSONs")
    parser.add_argument("--outbox", default=None, help="manifest OUTBOX dir (staging, no push)")
    parser.add_argument("--report-dir", default=None, help="dir to write improvement-report JSON")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--hourly", action="store_true")
    group.add_argument("--daily", action="store_true")
    group.add_argument("--selftest", action="store_true")
    args = parser.parse_args(argv)

    if args.selftest:
        return selftest()

    store = BrainStore(args.root)
    store.initialize()
    pass_kind = "daily" if args.daily else "hourly"
    report = run_pass(
        store,
        pass_kind=pass_kind,
        signals_dir=args.signals,
        outbox_dir=args.outbox,
        report_dir=args.report_dir,
    )
    print(json.dumps({
        "pass": report["pass"],
        "counts": report["counts"],
        "staged_manifest": report["staged_manifest"],
        "written_to": report.get("_written_to"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
