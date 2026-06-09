#!/usr/bin/env python3
"""
ATLAS Smart Brain (V2) Proof Harness  [fail-loud --selftest gate]
================================================================
Byte-for-byte the proven TuaniChat V2 harness (8/8, seed=42), with flat
imports for /opt/atlas/brain and a writable results path. Exits nonzero on
ANY failed proof so the auto-pull apply step fails closed and rolls back.
Seeded, deterministic head-to-head: V2 component vs the V1 baseline it
evolves. Every proof states the claim, runs both, and PASSes only if V2
measurably beats (or catches what V1 missed). Writes proof_results.json.

Run:  python3 proof_harness.py
"""

import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from enrichment_brain_v2 import (OutcomeFeedbackRanker, WaterfallStep,
                                    CostAwareWaterfall, CorroborationScorer,
                                    SelfTuningResolver)
from jarvis_v2 import JarvisV2
from guardians_v2 import ClosedLoopGuardian, AnomalyGuardian, MetaGuardian
from v1_baseline import (FIFOEnricher, FullPipelineEnricher,
                         FixedThresholdResolver, ThresholdGuardianV1,
                         ReactiveJarvisV1)

SEED = 42
RESULTS = []


def proof(name, claim):
    def deco(fn):
        def wrapper():
            random.seed(SEED)
            ok, detail = fn()
            RESULTS.append({"proof": name, "claim": claim,
                            "pass": ok, "detail": detail})
            flag = "PASS" if ok else "FAIL"
            print(f"[{flag}] {name}")
            for k, v in detail.items():
                print(f"        {k}: {v}")
        return wrapper
    return deco


# ---------------------------------------------------------------------------
# P1 - Outcome-feedback EV ranking beats FIFO
# ---------------------------------------------------------------------------
@proof("P1_outcome_feedback_ranking",
       "EV-ranked queue enriches the known-best source first and yields more "
       "conversions in the first 100 records than FIFO")
def p1():
    TRUE = {  # (p_contactable, p_convert|contactable)
        "ct_lottery_winners": (0.80, 0.15),
        "biz_registrations": (0.60, 0.06),
        "web_scrape":        (0.35, 0.02),
    }
    ranker = OutcomeFeedbackRanker()
    # historical outcome stream (what live V2 would have learned by now)
    for src, (pc, pv) in TRUE.items():
        for _ in range(200):
            contact = random.random() < pc
            convert = contact and random.random() < pv
            ranker.record_outcome(src, contactable=contact, converted=convert)
    # mixed pending queue, shuffled arrival
    queue = [{"id": i, "source": src, "queued_at": i}
             for i, src in enumerate(
                 [s for s in TRUE for _ in range(100)])]
    random.shuffle(queue)
    for i, r in enumerate(queue):
        r["queued_at"] = i

    def expected_conversions(batch):
        return sum(TRUE[r["source"]][0] * TRUE[r["source"]][1] for r in batch)

    v1_batch = FIFOEnricher.next_batch(queue, 100)
    v2_batch = ranker.rank_queue(queue)[:100]
    v1_conv = expected_conversions(v1_batch)
    v2_conv = expected_conversions(v2_batch)
    best_first = v2_batch[0]["source"] == "ct_lottery_winners"
    top_share = sum(1 for r in v2_batch if r["source"] == "ct_lottery_winners") / 100
    ok = best_first and v2_conv > v1_conv * 1.5
    return ok, {
        "v1_expected_conversions_first100": round(v1_conv, 2),
        "v2_expected_conversions_first100": round(v2_conv, 2),
        "uplift": f"{(v2_conv / v1_conv - 1) * 100:.0f}%",
        "v2_ranked_best_source_first": best_first,
        "best_source_share_of_v2_batch": top_share,
        "learned_ev_table": ranker.source_report(),
    }


# ---------------------------------------------------------------------------
# P2 - Cost-aware waterfall cuts ops/record
# ---------------------------------------------------------------------------
@proof("P2_cost_aware_waterfall",
       "Early-stop waterfall resolves the same records at materially fewer "
       "ops/record than V1's run-everything pipeline")
def p2():
    def make_steps():
        return [
            WaterfallStep("cache_lookup", 1,
                          lambda r: (0.9, {"hit": True}) if r["tier"] == "cached" else (0.05, None)),
            WaterfallStep("atlas_local_db", 2,
                          lambda r: (0.9, {"hit": True}) if r["tier"] == "local" else (0.05, None)),
            WaterfallStep("pattern_inference", 3,
                          lambda r: (0.85, {"hit": True}) if r["tier"] == "pattern" else (0.05, None)),
            WaterfallStep("deep_crawl", 10,
                          lambda r: (0.95, {"hit": True})),
        ]
    # realistic resolution mix
    tiers = (["cached"] * 30 + ["local"] * 30 + ["pattern"] * 25 + ["deep"] * 15)
    records_v1 = [{"id": i, "tier": t} for i, t in enumerate(tiers)]
    records_v2 = [dict(r) for r in records_v1]

    v1 = FullPipelineEnricher(make_steps())
    v2 = CostAwareWaterfall(make_steps(), sufficient_confidence=0.85)

    v1_ops = sum(v1.enrich(r)["ops_spent"] for r in records_v1)
    v2_results = [v2.enrich(r) for r in records_v2]
    v2_ops = sum(x["ops_spent"] for x in v2_results)
    v2_resolved = sum(1 for x in v2_results if x["resolved"])
    savings = 1 - v2_ops / v1_ops
    ok = savings >= 0.40 and v2_resolved == len(records_v2)
    return ok, {
        "v1_ops_per_record": v1_ops / len(records_v1),
        "v2_ops_per_record": round(v2_ops / len(records_v2), 2),
        "ops_savings": f"{savings * 100:.0f}%",
        "v2_records_resolved": f"{v2_resolved}/{len(records_v2)}",
        "daily_capacity_at_620k_ops": {
            "v1_records": int(620_000 / (v1_ops / len(records_v1))),
            "v2_records": int(620_000 / (v2_ops / len(records_v2))),
        },
    }


# ---------------------------------------------------------------------------
# P3 - Cross-source corroboration improves precision of "high confidence"
# ---------------------------------------------------------------------------
@proof("P3_corroboration_scoring",
       "Corroboration-ranked top-half has higher true-validity precision than "
       "V1's flat confidence (which cannot rank at all)")
def p3():
    scorer = CorroborationScorer()
    population = []
    for i in range(200):
        if i < 100:  # corroborated: CT + registration + 990 agree
            obs = {
                "ct_registry":   {"name": f"Biz{i}", "address": f"{i} Main St", "phone": f"860{i:07d}", "ein": f"06{i:07d}"},
                "biz_reg":       {"name": f"biz{i}", "address": f"{i} MAIN ST", "phone": f"860{i:07d}", "ein": f"06{i:07d}"},
                "irs_990":       {"name": f"Biz{i}", "address": f"{i} main st", "phone": f"860{i:07d}", "ein": f"06{i:07d}"},
            }
            truly_valid = random.random() < 0.95
        else:        # single scrape, no corroboration
            obs = {"web_scrape": {"name": f"Biz{i}", "address": f"{i} Elm St",
                                  "phone": f"203{i:07d}", "ein": None}}
            truly_valid = random.random() < 0.50
        score, _ = scorer.score(obs)
        population.append({"score": score, "valid": truly_valid})

    ranked = sorted(population, key=lambda x: -x["score"])
    top = ranked[:100]
    v2_precision = sum(1 for x in top if x["valid"]) / 100
    v1_precision = sum(1 for x in population if x["valid"]) / 200  # flat = random pick
    sep = ranked[0]["score"] - ranked[-1]["score"]
    ok = v2_precision >= v1_precision + 0.15 and sep > 0.3
    return ok, {
        "v1_flat_precision_any_100": round(v1_precision, 3),
        "v2_top100_precision": round(v2_precision, 3),
        "corroborated_score_example": ranked[0]["score"],
        "single_source_score_example": ranked[-1]["score"],
    }


# ---------------------------------------------------------------------------
# P4 - Self-tuning ER converges and beats the fixed threshold
# ---------------------------------------------------------------------------
@proof("P4_self_tuning_entity_resolution",
       "Threshold learned from corrections converges near the true optimum "
       "(0.83) and makes fewer errors than the fixed 0.70 threshold")
def p4():
    TRUE_OPT = 0.83

    def sample_pair():
        """Returns (similarity, is_same_entity). Pairs above/below the true
        boundary, with noise."""
        same = random.random() < 0.5
        if same:
            sim = min(0.99, max(0.5, random.gauss(0.90, 0.06)))
        else:
            sim = min(0.99, max(0.5, random.gauss(0.74, 0.06)))
        return sim, same

    v2 = SelfTuningResolver(initial_threshold=0.70)
    # learning phase: corrections arrive when V2's call disagrees with truth
    for _ in range(300):
        sim, same = sample_pair()
        decided_match = v2.is_match(sim)
        if decided_match and not same:
            v2.record_correction("false_merge")
        elif not decided_match and same:
            v2.record_correction("false_split")

    # evaluation phase: fresh pairs, V1 fixed vs V2 learned
    v1 = FixedThresholdResolver(0.70)
    v1_err = v2_err = 0
    n_eval = 2000
    for _ in range(n_eval):
        sim, same = sample_pair()
        if v1.is_match(sim) != same:
            v1_err += 1
        if v2.is_match(sim) != same:
            v2_err += 1
    converged = abs(v2.threshold - TRUE_OPT) <= 0.05
    ok = converged and v2_err < v1_err * 0.8
    return ok, {
        "learned_threshold": round(v2.threshold, 3),
        "true_optimum": TRUE_OPT,
        "converged_within_0.05": converged,
        "v1_fixed_error_rate": round(v1_err / n_eval, 3),
        "v2_learned_error_rate": round(v2_err / n_eval, 3),
        "corrections_consumed": v2.corrections,
    }


# ---------------------------------------------------------------------------
# P5 - Closed-loop guardian catches a fake-resolve V1 misses
# ---------------------------------------------------------------------------
@proof("P5_closed_loop_guardian",
       "When remediation silently fails, V1 claims 'self_healed' (dishonest); "
       "V2 verifies, refuses the claim, and escalates")
def p5():
    # Scenario: stuck outbound queue. 'restart_worker' remediation is BROKEN
    # (logs success, queue stays stuck) - the bench-test fake-resolve.
    def make_state():
        return {"queue_stuck": True}

    def detect(state):
        return state["queue_stuck"]

    def broken_remediate(state):
        pass  # logs success in V1's world, fixes nothing

    def verify(state):
        return not state["queue_stuck"]

    escalations = []
    state_v1, state_v2 = make_state(), make_state()
    v1 = ThresholdGuardianV1("outbound_queue", detect, broken_remediate)
    v2 = ClosedLoopGuardian("outbound_queue", detect, broken_remediate, verify,
                            max_retries=1, escalate_fn=escalations.append)

    r1 = v1.run(state_v1, tick=1)
    r2 = v2.run(state_v2, tick=1)

    # Also prove V2 does verify a WORKING remediation (no false escalation)
    state_ok = make_state()
    def working_remediate(state):
        state["queue_stuck"] = False
    v2b = ClosedLoopGuardian("outbound_queue_ok", detect, working_remediate, verify)
    r3 = v2b.run(state_ok, tick=1)

    ok = (r1["status"] == "self_healed"            # V1's dishonest claim
          and r2["status"] == "escalated"          # V2 catches the lie
          and len(escalations) == 1
          and v2.counters["remediation_verified"] == 0
          and v1.counters["self_healed_CLAIMED"] == 1
          and r3["status"] == "verified_healed")   # and no false alarms
    return ok, {
        "v1_claim_on_broken_fix": r1["status"],
        "v2_result_on_broken_fix": r2["status"],
        "v2_honest_counters": v2.honest_report(),
        "v1_dishonest_counters": v1.counters,
        "v2_on_working_fix": r3["status"],
    }


# ---------------------------------------------------------------------------
# P6 - Anomaly guardian catches in-band anomaly threshold guardian misses
# ---------------------------------------------------------------------------
@proof("P6_anomaly_detection",
       "Throughput sags 100->70 (static alert only fires <50): V1 threshold "
       "guardian stays silent, V2 z-score guardian flags it")
def p6():
    v1_fired = []
    v1 = ThresholdGuardianV1(
        "throughput", detect_fn=lambda s: s["tp"] < 50,
        remediate_fn=lambda s: v1_fired.append(s["tp"]))
    v2 = AnomalyGuardian("throughput", window=30, z_limit=3.5, min_points=10)

    caught = None
    for tick in range(40):
        tp = 100 + random.gauss(0, 2.5)
        if tick >= 30:
            tp = 70 + random.gauss(0, 2.5)   # in-band sag: above 50, way off baseline
        v1.run({"tp": tp}, tick)
        a = v2.observe(tp, tick)
        if a and caught is None:
            caught = a
    ok = (len(v1_fired) == 0 and caught is not None and caught["tick"] == 30)
    return ok, {
        "v1_threshold_guardian_alerts": len(v1_fired),
        "v2_anomaly_caught_at_tick": caught["tick"] if caught else None,
        "v2_anomaly_detail": caught,
        "anomaly_started_at_tick": 30,
    }


# ---------------------------------------------------------------------------
# P7 - Meta-guardian detects a dead guardian and dishonest counters
# ---------------------------------------------------------------------------
@proof("P7_meta_guardian",
       "Guardian-of-guardians flags a guardian that stopped heartbeating and "
       "one whose counters claim more heals than attempts")
def p7():
    def noop(_): return False
    healthy = ClosedLoopGuardian("outcome", noop, lambda s: None, lambda s: True)
    dead = ClosedLoopGuardian("node_cadence", noop, lambda s: None, lambda s: True)
    liar = ClosedLoopGuardian("version_drift", noop, lambda s: None, lambda s: True)
    liar.counters["remediation_verified"] = 5     # corrupted/dishonest display
    liar.counters["remediation_attempted"] = 2

    meta = MetaGuardian(dead_after=3)
    for g in (healthy, dead, liar):
        meta.register(g)

    for tick in range(1, 11):
        healthy.run({}, tick)
        liar.run({}, tick)
        if tick <= 5:
            dead.run({}, tick)     # dies after tick 5

    findings = meta.sweep(current_tick=10)
    kinds = {(f["guardian"], f["finding"]) for f in findings}
    ok = (("node_cadence", "DEAD") in kinds
          and ("version_drift", "DISHONEST_COUNTERS") in kinds
          and not any(f["guardian"] == "outcome" for f in findings))
    return ok, {"findings": findings}


# ---------------------------------------------------------------------------
# P8 - Predictive J.A.R.V.I.S. pre-throttles before the ceiling; V1 breaches
# ---------------------------------------------------------------------------
@proof("P8_predictive_jarvis",
       "With ops trending toward the 620k ceiling, V2 forecasts the breach "
       "and pre-throttles with ticks to spare; reactive V1 only acts after "
       "the ceiling is already breached")
def p8():
    v1 = ReactiveJarvisV1()
    v2 = JarvisV2()
    v2_action_tick = v1_action_tick = None
    v2_action = None
    v2_breached = False

    # Two identical worlds; each agent's throttle only affects its own world.
    ops_v1, ops_v2 = 400_000.0, 400_000.0
    GROWTH = 25_000
    for tick in range(30):
        a1 = v1.on_tick(ops_v1)
        a2 = v2.on_tick(ops_v2)
        if a1 == "reactive_throttle" and v1_action_tick is None:
            v1_action_tick = tick
        if a2 in ("pre_throttle", "reactive_throttle") and v2_action_tick is None:
            v2_action_tick = tick
            v2_action = a2
        if ops_v2 >= 620_000:
            v2_breached = True
        # growth continues; throttling flattens that agent's curve next tick
        if not v1.throttled:
            ops_v1 += GROWTH
        if not v2.throttled:
            ops_v2 += GROWTH

    margin_ticks = (v1_action_tick - v2_action_tick) if v1_action_tick is not None else None
    explanation = v2.rlog.explain(0)
    ok = (v2_action == "pre_throttle" and v2_action_tick < (v1_action_tick or 99)
          and not v2_breached and v1.breach_ticks >= 1)
    return ok, {
        "v2_action": v2_action,
        "v2_acted_at_tick": v2_action_tick,
        "v1_acted_at_tick(after_breach)": v1_action_tick,
        "lead_time_ticks": margin_ticks,
        "v2_ever_breached_ceiling": v2_breached,
        "v1_ticks_spent_in_breach": v1.breach_ticks,
        "v2_reasoning_log_entry": explanation,
    }


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 72)
    print("TuaniChat V2 Intelligence - Proof Harness (seed=%d)" % SEED)
    print("=" * 72)
    for fn in (p1, p2, p3, p4, p5, p6, p7, p8):
        fn()
        print("-" * 72)
    passed = sum(1 for r in RESULTS if r["pass"])
    print(f"RESULT: {passed}/{len(RESULTS)} proofs passed")
    out = os.environ.get("ATLAS_BRAIN_PROOF_OUT", "/tmp/atlas_brain_proof_results.json")
    with open(out, "w") as f:
        json.dump({"seed": SEED, "passed": passed, "total": len(RESULTS),
                   "proofs": RESULTS}, f, indent=2, default=str)
    sys.exit(0 if passed == len(RESULTS) else 1)
 