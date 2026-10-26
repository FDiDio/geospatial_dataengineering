"""
Places of the dashboard: names of cities to coordinates with the OpenStreetMap Nominatim geocoder,
typed coordinates, and the points clicked on the map.
"""

import json
import re
import urllib.parse
import urllib.request
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any

import h3

NOMINATIM_URL = "https://nominatim.openstreetmap.org"
# The Nominatim usage policy asks for an identifying user agent and at most one request per second:
# the dashboard sends one request per searched name or clicked point and caches the answers
USER_AGENT = "era5-geospatial-pipeline/0.2 (ERA5 weather map)"

# Two numbers, e.g. "45.46, 9.19", "45.46 9.19" or, with decimal commas, "45,46 9,19"
NUMBER = r"[-+]?\d+(?:[.,]\d+)?"
COORDINATES = re.compile(rf"({NUMBER})\s*[,;]?\s+({NUMBER})|({NUMBER})\s*[,;]\s*({NUMBER})")


@dataclass(frozen=True)
class Place:
    name: str  # full name, e.g. "Milano, Lombardia, Italia", or the coordinates
    latitude: float
    longitude: float  # -180 to 180
    kind: str = ""  # e.g. city, state, county, village, or coordinates

    @property
    def short_name(self) -> str:
        return self.name if self.kind == "coordinates" else self.name.split(",")[0]

    @property
    def label(self) -> str:
        """
        The name with its kind and coordinates: different places can share a name ("New York, United States"
        is a city and a state), and a selectbox needs distinct labels to keep its choice across reruns.
        """
        details = ", ".join(filter(None, [self.kind, f"{self.latitude:.2f}° {self.longitude:.2f}°"]))
        return f"{self.name} ({details})"


def nominatim(endpoint: str, params: dict[str, Any], language: str) -> Any:
    query = urllib.parse.urlencode({**params, "format": "jsonv2"})
    request = urllib.request.Request(
        f"{NOMINATIM_URL}/{endpoint}?{query}", headers={"User-Agent": USER_AGENT, "Accept-Language": language}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def search(query: str, language: str = "en", limit: int = 5) -> list[Place]:
    """Places matching a name, most relevant first, named in the given language when known."""
    results = nominatim("search", {"q": query, "limit": limit}, language)
    return [
        Place(result["display_name"], float(result["lat"]), float(result["lon"]), result.get("addresstype", ""))
        for result in results
    ]


def reverse(latitude: float, longitude: float, language: str = "en") -> str | None:
    """The name of the town or area at some coordinates, or None where there is none, e.g. at sea."""
    result = nominatim("reverse", {"lat": latitude, "lon": longitude, "zoom": 10}, language)
    return result.get("display_name")


def parse_coordinates(text: str) -> tuple[float, float] | None:
    """
    Latitude and longitude typed in degrees, the longitude brought to -180..180 (0..360 is accepted),
    or None when the text is not a pair of coordinates.
    """
    match = COORDINATES.fullmatch(text.strip())
    if match is None:
        return None
    latitude, longitude = (float(number.replace(",", ".")) for number in match.groups() if number is not None)
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 360):
        return None
    return latitude, (longitude + 180) % 360 - 180


def format_coordinates(latitude: float, longitude: float) -> str:
    return f"{latitude:.4f}, {longitude:.4f}"


def format_dms(latitude: float, longitude: float) -> str:
    """Coordinates in degrees, minutes and seconds, e.g. 45°27′51″N 9°11′23″E."""

    def angle(value: float, positive: str, negative: str) -> str:
        minutes, seconds = divmod(round(abs(value) * 3600), 60)
        degrees, minutes = divmod(minutes, 60)
        return f"{degrees}°{minutes:02d}′{seconds:02d}″{positive if value >= 0 else negative}"

    return f"{angle(latitude, 'N', 'S')} {angle(longitude, 'E', 'W')}"


def at(latitude: float, longitude: float) -> Place:
    """The place of some coordinates."""
    return Place(format_coordinates(latitude, longitude), latitude, longitude, "coordinates")


def clicked_point(state: Mapping[str, Any] | None, layers: Collection[str]) -> tuple[float, float] | None:
    """
    The center of the hexagon clicked on one of these layers of the map, from the selection state of
    st.pydeck_chart, or None. deck.gl reports the clicked object, not the clicked coordinates.
    """
    objects = ((state or {}).get("selection") or {}).get("objects") or {}
    for layer in layers:
        picked = objects.get(layer)
        if picked and picked[0].get("h3"):
            return h3.cell_to_latlng(picked[0]["h3"])
    return None
