"""
V1 baseline behaviors (faithful simulations of current production logic).
These exist so the proof harness can compare V2 against EXACTLY the
behaviors we are evolving away from - and so V2 keeps them as fallback.
"""


class FIFOEnricher:
    """V1: process the queue in arrival order, no learning."""
    @staticmethod
    def next_batch(queue, batch_size):
        return sorted(queue, key=lambda r: r.get("queued_at", 0))[:batch_size]


class FullPipelineEnricher:
    """V1: every record runs EVERY pipeline step, costs are fixed."""
    def __init__(self, steps):
        self.steps = steps

    def enrich(self, record):
        ops = 0
        confidence = 0.0
        for step in self.steps:
            gain, _ = step.fn(record)
            ops += step.ops_cost
            confidence = min(1.0, confidence + gain * (1.0 - confidence))
        return {"confidence": round(confidence, 4), "ops_spent": ops}


class FlatConfidenceScorer:
    """V1: single-source and triple-corroborated records get the same flat
    confidence - no cross-source signal."""
    @staticmethod
    def score(observations):
        return 0.7, {}  # flat


class FixedThresholdResolver:
    """V1: entity-resolution threshold is fixed forever."""
    def __init__(self, threshold=0.70):
        self.threshold = threshold

    def is_match(self, similarity):
        return similarity >= self.threshold


class ThresholdGuardianV1:
    """V1 guardian: static threshold, logs remediation, NEVER verifies.
    Counts every attempt as a self-heal (the display-dishonesty bug)."""

    def __init__(self, name, detect_fn, remediate_fn):
        self.name = name
        self.detect_fn, self.remediate_fn = detect_fn, remediate_fn
        self.counters = {"detected": 0, "self_healed_CLAIMED": 0}
        self.log = []

    def run(self, state, tick=0):
        if not self.detect_fn(state):
            return {"status": "healthy"}
        self.counters["detected"] += 1
        self.remediate_fn(state)                     # may silently fail
        self.counters["self_healed_CLAIMED"] += 1    # <-- counted blindly
        self.log.append({"tick": tick, "msg": "remediated (unverified)"})
        return {"status": "self_healed"}             # <-- possibly a lie


class ReactiveJarvisV1:
    """V1: throttles only AFTER the ops ceiling is already breached."""
    def __init__(self, ops_ceiling=620_000):
        self.ops_ceiling = ops_ceiling
        self.throttled = False
        self.breach_ticks = 0

    def on_tick(self, ops_today_projected):
        if ops_today_projected >= self.ops_ceiling:
            self.breach_ticks += 1
            if not self.throttled:
                self.throttled = True
                return "reactive_throttle"
        return "normal"
