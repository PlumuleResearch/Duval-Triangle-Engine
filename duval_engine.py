"""
Duval Triangle Engine
=====================
Core fault diagnosis engine based on the Duval Triangle Method (IEC 60599).
Translates three key DGA gases — CH4, C2H4, C2H2 — into fault zone coordinates.

Zones:
  PD  — Partial Discharge
  D1  — Discharge of Low Energy
  D2  — Discharge of High Energy
  DT  — Thermal + Electrical Fault
  T1  — Thermal Fault < 300 °C
  T2  — Thermal Fault 300–700 °C
  T3  — Thermal Fault > 700 °C
"""

import numpy as np
from dataclasses import dataclass
from typing import Optional
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.path import Path


# ---------------------------------------------------------------------------
# Zone boundary definitions (triangular % coordinates → Cartesian)
# Each polygon is defined in (CH4%, C2H4%, C2H2%) triangular coordinates.
# Conversion: x = C2H4 + CH4*cos60,  y = CH4*sin60  (with 0% C2H2 at origin)
# ---------------------------------------------------------------------------

ZONE_BOUNDARIES_TRI = {
    "PD": [
        (98, 2, 0),
        (100, 0, 0),
        (98, 0, 2),
    ],
    "D1": [
        (0,   0,  100),
        (0,  23,   77),
        (64, 23,   13),
        (87,  0,   13),
    ],
    "D2": [
        (0,  23,  77),
        (0,  71,  29),
        (31, 40,  29),
        (47, 40,  13),
        (64, 23,  13),
    ],
    "DT": [
        (0,  71,  29),
        (0,  85,  15),
        (35, 50,  15),
        (46, 50,   4),
        (96,  0,   4),
        (87,  0,  13),
        (47, 40,  13),
        (31, 40,  29),
    ],
    "T1": [
        (76, 20,  4),
        (80, 20,  0),
        (98,  2,  0),
        (98,  0,  2),
        (96,  0,  4),
    ],
    "T2": [
        (46, 50,  4),
        (50, 50,  0),
        (80, 20,  0),
        (76, 20,  4),
    ],
    "T3": [
        (0,  85,  15),
        (0, 100,   0),
        (50, 50,   0),
        (35, 50,  15),
    ],
}

ZONE_COLORS = {
    "PD": "#1a1a2e",
    "D1": "#f7c59f",
    "D2": "#ef233c",
    "DT": "#8ecae6",
    "T1": "#a8dadc",
    "T2": "#e76f51",
    "T3": "#2a9d8f",
}

ZONE_DESCRIPTIONS = {
    "PD": "Partial Discharge (Corona / cold plasma in gas bubbles or voids)",
    "D1": "Discharge of Low Energy (sparking, pinholes, carbonized punctures in paper)",
    "D2": "Discharge of High Energy (power arc, extensive carbon formation, metal fusion)",
    "DT": "Thermal + Electrical Fault (mixed mode — review oil & paper insulation)",
    "T1": "Thermal Fault T < 300 °C (paper turns brownish / begins to carbonize)",
    "T2": "Thermal Fault 300 < T < 700 °C (paper carbonized, carbon particles in oil)",
    "T3": "Thermal Fault T > 700 °C (extensive carbon, metal coloration or fusion)",
}


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------

def tri_to_cartesian(ch4_pct: float, c2h4_pct: float, c2h2_pct: float):
    """
    Convert triangular % coordinates to 2-D Cartesian (x, y).
    Convention:
        • Bottom-left vertex A = pure C2H2  (100% C2H2)
        • Bottom-right vertex M = pure C2H4 (100% C2H4)
        • Top vertex L          = pure CH4  (100% CH4)
    """
    x = c2h4_pct + ch4_pct * np.cos(np.radians(60))
    y = ch4_pct * np.sin(np.radians(60))
    return x, y


def build_cartesian_zone(tri_points):
    """Return list of (x, y) Cartesian coords from list of (ch4%, c2h4%, c2h2%) tuples."""
    return [tri_to_cartesian(ch4, c2h4, c2h2) for ch4, c2h4, c2h2 in tri_points]


# Pre-build Matplotlib Path objects for fast point-in-polygon tests
_ZONE_PATHS: dict[str, Path] = {}
for _zone, _pts in ZONE_BOUNDARIES_TRI.items():
    _cart = build_cartesian_zone(_pts)
    _arr = np.array(_cart + [_cart[0]])        # close polygon
    _ZONE_PATHS[_zone] = Path(_arr)


# ---------------------------------------------------------------------------
# Core data structures
# ---------------------------------------------------------------------------

@dataclass
class DGASample:
    """A single DGA reading from a transformer."""
    transformer_id: str
    timestamp: str
    ch4_ppm: float
    c2h4_ppm: float
    c2h2_ppm: float
    source: str = "database"

    @property
    def total(self) -> float:
        return self.ch4_ppm + self.c2h4_ppm + self.c2h2_ppm

    @property
    def ch4_pct(self) -> float:
        return 100.0 * self.ch4_ppm / self.total if self.total else 0.0

    @property
    def c2h4_pct(self) -> float:
        return 100.0 * self.c2h4_ppm / self.total if self.total else 0.0

    @property
    def c2h2_pct(self) -> float:
        return 100.0 * self.c2h2_ppm / self.total if self.total else 0.0

    @property
    def cartesian(self) -> tuple[float, float]:
        return tri_to_cartesian(self.ch4_pct, self.c2h4_pct, self.c2h2_pct)


@dataclass
class DiagnosisResult:
    sample: DGASample
    fault_zone: str
    description: str
    x: float
    y: float
    severity: str           # LOW / MEDIUM / HIGH / CRITICAL


# ---------------------------------------------------------------------------
# Diagnosis engine
# ---------------------------------------------------------------------------

_SEVERITY_MAP = {
    "PD": "MEDIUM",
    "D1": "MEDIUM",
    "D2": "CRITICAL",
    "DT": "HIGH",
    "T1": "LOW",
    "T2": "MEDIUM",
    "T3": "HIGH",
}


def diagnose(sample: DGASample) -> DiagnosisResult:
    """
    Classify a DGA sample into a Duval Triangle fault zone.

    Returns a DiagnosisResult with zone label, description, severity,
    and the 2-D Cartesian coordinates of the fault point.
    """
    if sample.total == 0:
        raise ValueError(f"Sample {sample.transformer_id} @ {sample.timestamp}: "
                         "all three gas concentrations are zero — cannot diagnose.")

    x, y = sample.cartesian
    point = np.array([[x, y]])

    for zone, path in _ZONE_PATHS.items():
        if path.contains_point((x, y), radius=0.1):
            return DiagnosisResult(
                sample=sample,
                fault_zone=zone,
                description=ZONE_DESCRIPTIONS[zone],
                x=x,
                y=y,
                severity=_SEVERITY_MAP[zone],
            )

    # Fallback: assign nearest zone by centroid distance
    min_dist = float("inf")
    nearest = "DT"
    for zone, path in _ZONE_PATHS.items():
        verts = path.vertices[:-1]
        cx, cy = verts[:, 0].mean(), verts[:, 1].mean()
        d = (x - cx) ** 2 + (y - cy) ** 2
        if d < min_dist:
            min_dist, nearest = d, zone

    return DiagnosisResult(
        sample=sample,
        fault_zone=nearest + "*",
        description=ZONE_DESCRIPTIONS[nearest] + " [boundary — nearest zone]",
        x=x,
        y=y,
        severity=_SEVERITY_MAP[nearest],
    )


def batch_diagnose(samples: list[DGASample]) -> list[DiagnosisResult]:
    """Diagnose a list of DGA samples."""
    results = []
    for s in samples:
        try:
            results.append(diagnose(s))
        except ValueError as e:
            print(f"[WARN] Skipping sample: {e}")
    return results
