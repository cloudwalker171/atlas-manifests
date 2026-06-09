"""
TuaniChat V2 Enrichment Brain
=============================
Four architectural upgrades over the V1 FIFO/flat-pipeline brain.
Stdlib only. Deterministic. Zero external API calls. Zero paid AI.
Designed to wrap, not replace, the V1 pipeline: every component degrades
to V1 behavior when it has no learned state (the no-data fallback).

Components
----------
1. OutcomeFeedbackRanker  - EV-ranked queue (replaces FIFO)
2. CostAwareWaterfall     - free->cheap sequenced checks, early stop
3. CorroborationScorer    - cross-source agreement confidence
4. SelfTuningResolver     - entity-resolution threshold learned from corrections
"""

from collections import defaultdict


# ---------------------------------------------------------------------------
# 1. OUTCOME-FEEDBACK LEARNING (EV-ranked queue)
# ---------------------------------------------------------------------------
class OutcomeFeedbackRanker:
    """Learns P(contactable) and P(convert | contactable) per (source, path)
    from real reply/bounce/conversion outcomes, then ranks the enrichment
    queue by expected value instead of arrival order.

    Laplace-smoothed so it behaves like FIFO (uniform EV) with no data and
    sharpens as outcomes arrive. This is why V2 must run LIVE to get smart:
    the ranking is only as good as the outcome stream feeding it.
    """

    def __init__(self, alpha=1.0, beta=1.0):
        self.alpha, self.beta = alpha, beta
        self.stats = defaultdict(lambda: {
            "enriched": 0, "contactable": 0, "bounced": 0,
            "replied": 0, "converted": 0,
        })

    def record_outcome(self, source, path="default", *, contactable=False,
                       bounced=False, replied=False, converted=False):
        s = self.stats[(source, path)]
        s["enriched"] += 1
        if contactable: s["contactable"] += 1
        if bounced:     s["bounced"] += 1
        if replied:     s["replied"] += 1
        if converted:   s["converted"] += 1

    def p_contactable(self, source, path="default"):
        s = self.stats[(source, path)]
        return (s["contactable"] + self.alpha) / (s["enriched"] + self.alpha + self.beta)

    def p_convert(self, source, path="default"):
        s = self.stats[(source, path)]
        return (s["converted"] + self.alpha) / (s["contactable"] + self.alpha + self.beta)

    def expected_value(self, source, path="default"):
        """EV of enriching one more record from this (source, path)."""
        return self.p_contactable(source, path) * self.p_convert(source, path)

    def rank_queue(self, records):
        """records: iterable of dicts with 'source', optional 'path', 'queued_at'.
        Returns EV-descending; FIFO preserved as tie-break (and as the
        no-data fallback, since untrained EVs are all equal)."""
        return sorted(
            records,
            key=lambda r: (-self.expected_value(r["source"], r.get("path", "default")),
                           r.get("queued_at", 0)),
        )

    def source_report(self):
        return {
            f"{src}/{path}": {
                "ev": round(self.expected_value(src, path), 4),
                "p_contactable": round(self.p_contactable(src, path), 4),
                "p_convert": round(self.p_convert(src, path), 4),
                "n": self.stats[(src, path)]["enriched"],
            }
            for (src, path) in self.stats
        }


# ---------------------------------------------------------------------------
# 2. COST-AWARE WATERFALL (free -> cheap, early stop)
# ---------------------------------------------------------------------------
class WaterfallStep:
    """name, ops_cost (abstract ops units against the ~620k/day ceiling),
    fn(record) -> (confidence_gain: float, evidence: dict|None)."""
    def __init__(self, name, ops_cost, fn):
        self.name, self.ops_cost, self.fn = name, ops_cost, fn


class CostAwareWaterfall:
    """Runs steps cheapest-first, accumulates confidence, stops as soon as
    sufficient_confidence is reached. V1 runs every step on every record;
    V2 spends ops only until the record is 'good enough'.

    Steps are auto-ordered by (ops_cost / historical_yield) so the waterfall
    also LEARNS which cheap steps actually resolve records and promotes them.
    """

    def __init__(self, steps, sufficient_confidence=0.85):
        self.steps = list(steps)
        self.sufficient_confidence = sufficient_confidence
        self.yield_stats = defaultdict(lambda: {"runs": 0, "gain": 0.0})

    def _ordered_steps(self):
        def efficiency(step):
            ys = self.yield_stats[step.name]
            avg_gain = (ys["gain"] / ys["runs"]) if ys["runs"] else 0.5  # optimistic prior
            return step.ops_cost / max(avg_gain, 1e-6)
        return sorted(self.steps, key=efficiency)

    def enrich(self, record):
        confidence, ops_spent, trail = 0.0, 0, []
        for step in self._ordered_steps():
            if confidence >= self.sufficient_confidence:
                break
            gain, evidence = step.fn(record)
            ops_spent += step.ops_cost
            ys = self.yield_stats[step.name]
            ys["runs"] += 1
            ys["gain"] += gain
            confidence = min(1.0, confidence + gain * (1.0 - confidence))
            trail.append({"step": step.name, "ops": step.ops_cost,
                          "gain": round(gain, 3), "conf": round(confidence, 3)})
            if evidence:
                record.setdefault("evidence", {})[step.name] = evidence
        return {"confidence": round(confidence, 4), "ops_spent": ops_spent,
                "trail": trail, "resolved": confidence >= self.sufficient_confidence}


# ---------------------------------------------------------------------------
# 3. CROSS-SOURCE CORROBORATION
# ---------------------------------------------------------------------------
class CorroborationScorer:
    """Same business observed consistently in CT registry + business
    registration + IRS 990 is worth far more than one scrape. Scores
    agreement across independent sources per field, with diminishing
    returns per extra source and a penalty for direct conflicts."""

    FIELD_WEIGHTS = {"name": 0.20, "address": 0.25, "phone": 0.30, "ein": 0.25}

    def score(self, observations):
        """observations: {source: {field: value}} -> (score 0..1, detail)"""
        detail, total = {}, 0.0
        for field, weight in self.FIELD_WEIGHTS.items():
            values = [obs[field] for obs in observations.values()
                      if obs.get(field) not in (None, "")]
            if not values:
                detail[field] = {"sources": 0, "agree": 0, "score": 0.0}
                continue
            norm = [self._normalize(v) for v in values]
            best = max(norm.count(v) for v in set(norm))
            conflicts = len(norm) - best
            # diminishing returns: 1 source=0.5, 2 agreeing=0.8, 3+=0.95
            agree_score = {1: 0.5, 2: 0.8}.get(best, 0.95 if best >= 3 else 0.0)
            agree_score = max(0.0, agree_score - 0.25 * conflicts)
            detail[field] = {"sources": len(values), "agree": best,
                             "conflicts": conflicts, "score": round(agree_score, 3)}
            total += weight * agree_score
        return round(total, 4), detail

    @staticmethod
    def _normalize(v):
        return "".join(str(v).lower().split())


# ---------------------------------------------------------------------------
# 4. SELF-TUNING ENTITY RESOLUTION
# ---------------------------------------------------------------------------
class SelfTuningResolver:
    """Entity-resolution match threshold that learns from corrections.
    false_merge  (we merged two distinct businesses)   -> raise threshold
    false_split  (we failed to merge the same business) -> lower threshold
    Step size decays so the threshold converges instead of oscillating.
    Fallback: with zero corrections it IS the fixed V1 threshold."""

    def __init__(self, initial_threshold=0.70, lo=0.50, hi=0.98,
                 step0=0.04, decay=0.96):
        self.threshold = initial_threshold
        self.lo, self.hi = lo, hi
        self.step = step0
        self.decay = decay
        self.corrections = {"false_merge": 0, "false_split": 0}
        self.history = [initial_threshold]

    def is_match(self, similarity):
        return similarity >= self.threshold

    def record_correction(self, kind):
        """kind: 'false_merge' (merged distinct entities -> raise threshold)
              or 'false_split' (missed a true merge -> lower threshold)."""
        assert kind in self.corrections
        self.corrections[kind] += 1
        if kind == "false_merge":
            self.threshold = min(self.hi, self.threshold + self.step)
        else:
            self.threshold = max(self.lo, self.threshold - self.step)
        self.step *= self.decay
        self.history.append(round(self.threshold, 4))


# ---------------------------------------------------------------------------
# Facade: the V2 brain, wired for evolve-not-replace
# ---------------------------------------------------------------------------
class EnrichmentBrainV2:
    """Composition of the four upgrades. Each is independently feature-flagged
    so cutover can promote one capability at a time (see MIGRATION plan).
    With all flags off, behavior is exactly V1 (FIFO + full pipeline)."""

    def __init__(self, waterfall_steps, flags=None):
        self.flags = {"ev_ranking": True, "waterfall": True,
                      "corroboration": True, "self_tuning_er": True,
                      **(flags or {})}
        self.ranker = OutcomeFeedbackRanker()
        self.waterfall = CostAwareWaterfall(waterfall_steps)
        self.corroborator = CorroborationScorer()
        self.resolver = SelfTuningResolver()

    def next_batch(self, queue, batch_size):
        ordered = (self.ranker.rank_queue(queue) if self.flags["ev_ranking"]
                   else sorted(queue, key=lambda r: r.get("queued_at", 0)))
        return ordered[:batch_size]
