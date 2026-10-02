"""ตรวจว่าพิกัดอยู่ในเขตกรุงเทพมหานครจริงหรือไม่ (point-in-polygon กับขอบเขตจังหวัด).

ขอบเขตจาก https://github.com/chingchai/OpenGISData-Thailand (provinces.geojson, pro_code 10)
"""
import json
from functools import lru_cache
from pathlib import Path

BOUNDARY_FILE = Path(__file__).with_name("bangkok_boundary.geojson")


@lru_cache(maxsize=1)
def _polygons():
    geom = json.loads(BOUNDARY_FILE.read_text(encoding="utf-8"))["features"][0]["geometry"]
    polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
    out = []
    for poly in polys:
        rings = [[(x, y) for x, y, *_ in ring] for ring in poly]
        xs = [p[0] for p in rings[0]]
        ys = [p[1] for p in rings[0]]
        out.append(((min(xs), min(ys), max(xs), max(ys)), rings))
    return out


def _in_ring(x, y, ring):
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def in_bangkok(lat, lon) -> bool:
    for (x0, y0, x1, y1), rings in _polygons():
        if x0 <= lon <= x1 and y0 <= lat <= y1 and _in_ring(lon, lat, rings[0]) \
                and not any(_in_ring(lon, lat, hole) for hole in rings[1:]):
            return True
    return False
