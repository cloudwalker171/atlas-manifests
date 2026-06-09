"""
TuaniChat Guardians V2
======================
Fixes the two bench-test findings:
  (1) log-but-don't-remediate / fake-resolve: V1 guardians log a remediation
      and count it as healed WITHOUT verifying the metric recovered.
  (2) display dishonesty: self-heal counters count ATTEMPTS as SUCCESSES.

V2 guardians CLOSE THE LOOP: detect -> remediate -> VERIFY -> escalate if
not actually fixed. Counters separate attempted / verified / escalated.
Adds anomaly detection (catches in-band weirdness thresholds miss) and a
META-GUARDIAN that watches the guardians themselves.
Stdlib only, deterministic.
"""

import math
from collections import deque


# ---------------------------------------------------------------------------
# 1. CLOSED-LOOP GUARDIAN (detect -> remediate -> verify -> escalate)
# ---------------------------------------------------------------------------
class ClosedLoopGuardian:
    """detect_fn(state) -> True if unhealthy
    remediate_fn(state) -> None (attempts a fix; may silently fail!)
    verify_fn(state)    -> True if metric actually recovered

    HONEST COUNTERS:
      detected             - issues seen
      remediation_attempted- fixes tried
      remediation_verified - fixes PROVEN to work (the only 'self-healed')
      escalated            - verified-failed fixes raised to operator/J.A.R.V.I.S.
    """

    def __init__(self, name, detect_fn, remediate_fn, verify_fn,
                 max_retries=1, escalate_fn=None):
        self.name = name
        self.detect_fn, self.remediate_fn, self.verify_fn = \
            detect_fn, remediate_fn, verify_fn
        self.max_retries = max_retries
        self.escalate_fn = escalate_fn or (lambda issue: None)
        self.counters = {"detected": 0, "remediation_attempted": 0,
                         "remediation_verified": 0, "escalated": 0}
        self.events = []
        self.last_heartbeat = 0

    def run(self, state, tick=0):
        self.last_heartbeat = tick
        if not self.detect_fn(state):
            return {"status": "healthy"}
        self.counters["detected"] += 1
        for attempt in range(1, self.max_retries + 2):
            self.remediate_fn(state)
            self.counters["remediation_attempted"] += 1
            if self.verify_fn(state):                       # <-- THE LOOP CLOSES
                self.counters["remediation_verified"] += 1
                self.events.append({"tick": tick, "result": "verified_healed",
                                    "attempts": attempt})
                return {"status": "verified_healed", "attempts": attempt}
        # remediation did NOT actually work -> escalate, never claim healed
        self.counters["escalated"] += 1
        issue = {"guardian": self.name, "tick": tick,
                 "result": "remediation_failed_escalated"}
        self.events.append(issue)
        self.escalate_fn(issue)
        return {"status": "escalated"}

    def honest_report(self):
        c = self.counters
        return {
            **c,
            "true_self_heal_rate": round(
                c["remediation_verified"] / c["detected"], 3) if c["detected"] else None,
            "NOTE": "self-healed == verified only; attempts are not successes",
        }


# ---------------------------------------------------------------------------
# 2. ANOMALY GUARDIAN (z-score; catches in-band anomalies thresholds miss)
# ---------------------------------------------------------------------------
class AnomalyGuardian:
    """Rolling mean/std per metric; flags |z| >= z_limit even when the value
    is comfortably inside the static alert band. Example: throughput sags
    100 -> 70 while the static alert only fires below 50 - V1 stays silent,
    V2 flags it. Warm-up of `min_points` before judging (no cold-start noise)."""

    def __init__(self, name, window=30, z_limit=3.5, min_points=10):
        self.name = name
        self.window, self.z_limit, self.min_points = window, z_limit, min_points
        self.values = deque(maxlen=window)
        self.anomalies = []
        self.last_heartbeat = 0

    def observe(self, value, tick=0):
        self.last_heartbeat = tick
        if len(self.values) >= self.min_points:
            mean = sum(self.values) / len(self.values)
            var = sum((v - mean) ** 2 for v in self.values) / len(self.values)
            std = math.sqrt(var) or 1e-9
            z = (value - mean) / std
            if abs(z) >= self.z_limit:
                a = {"tick": tick, "value": value, "z": round(z, 2),
                     "baseline_mean": round(mean, 2)}
                self.anomalies.append(a)
                self.values.append(value)
                return a
        self.values.append(value)
        return None


# ---------------------------------------------------------------------------
# 3. META-GUARDIAN (guardian of guardians)
# ---------------------------------------------------------------------------
class MetaGuardian:
    """Watches the watchers. Flags:
      - DEAD     : guardian missed `dead_after` heartbeat ticks
      - INEFFECTIVE: verified-heal rate below floor over enough detections
      - DISHONEST: counters claim more verified heals than attempts (a
                   display-dishonesty regression tripwire)
    """

    def __init__(self, dead_after=3, efficacy_floor=0.5, min_detections=4):
        self.registry = {}
        self.dead_after = dead_after
        self.efficacy_floor = efficacy_floor
        self.min_detections = min_detections
        self.findings = []

    def register(self, guardian):
        self.registry[guardian.name] = guardian

    def sweep(self, current_tick):
        findings = []
        for name, g in self.registry.items():
            if current_tick - g.last_heartbeat >= self.dead_after:
                findings.append({"guardian": name, "finding": "DEAD",
                                 "last_heartbeat": g.last_heartbeat})
            counters = getattr(g, "counters", None)
            if counters:
                if counters["remediation_verified"] > counters["remediation_attempted"]:
                    findings.append({"guardian": name, "finding": "DISHONEST_COUNTERS"})
                if (counters["detected"] >= self.min_detections and
                        counters["remediation_verified"] / counters["detected"]
                        < self.efficacy_floor):
                    findings.append({"guardian": name, "finding": "INEFFECTIVE",
                                     "verified_rate": round(
                                         counters["remediation_verified"]
                                         / counters["detected"], 3)})
        self.findings.extend(findings)
        return findings
