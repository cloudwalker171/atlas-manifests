"""
TuaniChat J.A.R.V.I.S. V2 - Predictive Intelligence Module
==========================================================
Upgrades the proven reactive V1 intel module (31/31 tests, tnc-jarvis.service)
from "respond when it breaks" to "see it coming and act first".
Stdlib only. Deterministic math (least-squares + EWMA) - the local council AI
may ANNOTATE decisions but every decision here works with zero AI calls.

Components
----------
1. BreachForecaster   - linear-trend + EWMA forecast of ops/queue metrics;
                        predicts ceiling breaches N ticks ahead
2. MetaLearner        - tunes its own alert thresholds from guardian history
                        (false-alarm vs missed-incident balance)
3. ReasoningLog       - structured, replayable WHY for every decision
4. GoalPacer          - goal-driven pacing against the ~620k ops/day ceiling
"""

import math
from collections import deque


# ---------------------------------------------------------------------------
# 1. PREDICTIVE: forecast breaches before they happen
# ---------------------------------------------------------------------------
class BreachForecaster:
    """Keeps a rolling window per metric; fits a least-squares trend line and
    an EWMA level; forecasts value h ticks ahead and reports the first tick
    at which a limit would be breached. Pre-allocation hooks fire BEFORE the
    breach instead of after (V1 reacts only once the limit is already hit)."""

    def __init__(self, window=24, ewma_alpha=0.3):
        self.window = window
        self.alpha = ewma_alpha
        self.series = {}

    def observe(self, metric, value):
        s = self.series.setdefault(metric, deque(maxlen=self.window))
        s.append(float(value))

    def _trend(self, metric):
        s = self.series.get(metric)
        if not s or len(s) < 3:
            return None, None
        n = len(s)
        xs = range(n)
        mx, my = (n - 1) / 2.0, sum(s) / n
        sxx = sum((x - mx) ** 2 for x in xs)
        sxy = sum((x - mx) * (y - my) for x, y in zip(xs, s))
        slope = sxy / sxx if sxx else 0.0
        return slope, my + slope * (n - 1 - mx)  # slope, current level

    def forecast(self, metric, horizon):
        slope, level = self._trend(metric)
        if slope is None:
            return None
        return level + slope * horizon

    def ticks_until_breach(self, metric, limit, max_horizon=12):
        """First future tick (1..max_horizon) where forecast >= limit, or None."""
        slope, level = self._trend(metric)
        if slope is None or slope <= 0:
            return None
        for h in range(1, max_horizon + 1):
            if level + slope * h >= limit:
                return h
        return None


# ---------------------------------------------------------------------------
# 2. META-LEARNING: tune own thresholds from guardian history
# ---------------------------------------------------------------------------
class MetaLearner:
    """Consumes the guardian event history (alert fired -> was it a real
    incident?) and nudges alert thresholds: too many false alarms -> relax;
    any missed incident -> tighten (missed incidents weighted 3x, because a
    quiet pager is cheaper than a dead queue). Converges via decaying step."""

    def __init__(self, thresholds, step0=0.05, decay=0.95,
                 false_alarm_tolerance=0.2):
        self.thresholds = dict(thresholds)   # {alert_name: threshold}
        self.step = {k: step0 for k in thresholds}
        self.decay = decay
        self.tolerance = false_alarm_tolerance
        self.history = {k: [] for k in thresholds}  # (fired, was_real)

    def record(self, alert_name, fired, was_real_incident):
        self.history[alert_name].append((fired, was_real_incident))

    def retune(self, alert_name, direction_hint=+1):
        """direction_hint: +1 means raising the threshold makes the alert
        LESS sensitive (e.g. error-rate alerts); -1 for inverted metrics."""
        h = self.history[alert_name][-50:]
        if len(h) < 5:
            return self.thresholds[alert_name]  # not enough evidence: no-op
        false_alarms = sum(1 for fired, real in h if fired and not real)
        missed = sum(1 for fired, real in h if not fired and real)
        fired_total = max(1, sum(1 for fired, _ in h if fired))
        fa_rate = false_alarms / fired_total
        t, s = self.thresholds[alert_name], self.step[alert_name]
        if missed > 0:
            t -= direction_hint * s * 3 * missed      # tighten hard
        elif fa_rate > self.tolerance:
            t += direction_hint * s                   # relax gently
        self.thresholds[alert_name] = t
        self.step[alert_name] = s * self.decay
        return t


# ---------------------------------------------------------------------------
# 3. EXPLAINABLE: structured reasoning log
# ---------------------------------------------------------------------------
class ReasoningLog:
    """Every decision gets a machine-replayable record: inputs seen, rule
    applied, alternatives rejected, confidence, and whether the deterministic
    fallback (vs local council AI annotation) produced it. 'Genuinely
    explainable' = you can recompute the decision from the entry alone."""

    def __init__(self):
        self.entries = []

    def log(self, decision, *, inputs, rule, because, alternatives=None,
            confidence=1.0, engine="deterministic"):
        entry = {
            "seq": len(self.entries),
            "decision": decision,
            "inputs": inputs,
            "rule": rule,
            "because": because,
            "alternatives_rejected": alternatives or [],
            "confidence": confidence,
            "engine": engine,  # 'deterministic' | 'council_ai'
        }
        self.entries.append(entry)
        return entry

    def explain(self, seq):
        e = self.entries[seq]
        alts = ("; rejected: " + ", ".join(e["alternatives_rejected"])
                if e["alternatives_rejected"] else "")
        return (f"[{e['seq']}] {e['decision']} <- rule '{e['rule']}' on "
                f"{e['inputs']} because {e['because']}{alts} "
                f"(conf={e['confidence']}, engine={e['engine']})")


# ---------------------------------------------------------------------------
# 4. GOAL-DRIVEN PACING
# ---------------------------------------------------------------------------
class GoalPacer:
    """Given a daily goal (e.g. 'enrich 5k records') and the ops ceiling,
    computes a target rate per tick and adjusts when ahead/behind, never
    exceeding the share of the ceiling allotted to this workload."""

    def __init__(self, daily_goal_units, ops_per_unit, ops_ceiling_per_day,
                 ceiling_share=0.6, ticks_per_day=96):
        self.goal = daily_goal_units
        self.ops_per_unit = ops_per_unit
        self.max_units_day = (ops_ceiling_per_day * ceiling_share) / ops_per_unit
        self.ticks = ticks_per_day
        self.done = 0
        self.tick = 0

    def units_this_tick(self):
        self.tick += 1
        remaining_ticks = max(1, self.ticks - self.tick + 1)
        remaining_goal = max(0, self.goal - self.done)
        target = remaining_goal / remaining_ticks
        cap = self.max_units_day / self.ticks * 1.5  # burst cap 150% of even pace
        return min(target * 1.1, cap)  # 10% catch-up headroom

    def record_done(self, units):
        self.done += units


# ---------------------------------------------------------------------------
# Facade
# ---------------------------------------------------------------------------
class JarvisV2:
    """Wraps V1 behavior: if the forecaster has <3 points or forecasts no
    breach, JarvisV2 does exactly what reactive V1 does (act on breach).
    Prediction is an ADDITIVE layer, never a replacement - V1's reactive
    check remains the last line and the fallback."""

    def __init__(self, ops_ceiling=620_000):
        self.forecaster = BreachForecaster()
        self.meta = MetaLearner({"ops_pressure": 0.85})
        self.rlog = ReasoningLog()
        self.ops_ceiling = ops_ceiling
        self.throttled = False

    def on_tick(self, ops_today_projected):
        self.forecaster.observe("ops", ops_today_projected)
        breach_in = self.forecaster.ticks_until_breach("ops", self.ops_ceiling)
        if breach_in is not None and breach_in <= 6 and not self.throttled:
            self.throttled = True
            self.rlog.log(
                "PRE_THROTTLE", inputs={"ops": ops_today_projected,
                                        "breach_in_ticks": breach_in},
                rule="forecast_breach<=6_ticks",
                because=f"trend hits {self.ops_ceiling} ceiling in {breach_in} ticks",
                alternatives=["wait_for_breach(V1 reactive)"],
                confidence=0.9)
            return "pre_throttle"
        # V1 reactive fallback - unchanged proven behavior
        if ops_today_projected >= self.ops_ceiling and not self.throttled:
            self.throttled = True
            self.rlog.log("REACTIVE_THROTTLE",
                          inputs={"ops": ops_today_projected},
                          rule="v1_reactive_ceiling",
                          because="ceiling already breached (fallback path)")
            return "reactive_throttle"
        return "normal"
