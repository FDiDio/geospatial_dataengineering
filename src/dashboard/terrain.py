"""Altitude of the H3 cells, from the Copernicus 90 m elevation model served by Open-Meteo."""

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from statistics import fmean

import h3

from dashboard.places import USER_AGENT

ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"
# A cell is sampled at the centers of its children two resolutions finer: 49 points, about 2.5 km apart
# in a resolution 5 cell, within the 100 points of a request. The H3 hierarchy is approximate: a few
# of them lie just across the edge of the cell.
SAMPLE_STEP = 2


@dataclass(frozen=True)
class Altitude:
    """Altitude of a cell, in meters above sea level, the sea counting as 0."""

    mean: float
    min: float
    max: float
    points: int


def cell_altitude(cell: str) -> Altitude:
    points = [h3.cell_to_latlng(child) for child in h3.cell_to_children(cell, h3.get_resolution(cell) + SAMPLE_STEP)]
    params = urllib.parse.urlencode(
        {
            "latitude": ",".join(f"{latitude:.5f}" for latitude, _ in points),
            "longitude": ",".join(f"{longitude:.5f}" for _, longitude in points),
        }
    )
    request = urllib.request.Request(f"{ELEVATION_URL}?{params}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=10) as response:
        heights = json.load(response)["elevation"]
    return Altitude(fmean(heights), min(heights), max(heights), len(heights))
