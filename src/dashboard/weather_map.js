// The map of the dashboard, a Streamlit component (st.components.v2, maps.py): MapLibre draws the CARTO basemap and
// deck.gl the hexagons over it. The browser covers the whole view with hexagons at each move, smaller on the world and
// larger as the map zooms in, and lights the one under the pointer. A click sends its point and the resolution of its
// hexagon to Python. The map lives as long as the page: each run of the script gives it new data, which changes its
// layers, and it moves only when Python gives it a new view.

// h3-js before deck.gl, whose bundle takes the h3 of the page for its H3 layers
const LIBRARIES = [
  "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl.js",
  "https://cdn.jsdelivr.net/npm/h3-js@4.4.0/dist/h3-js.umd.js",
  "https://cdn.jsdelivr.net/npm/deck.gl@9.3.11/dist.min.js",
];
const STYLESHEET = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl.css";
// MapLibre draws the equator 512 pixels wide at zoom 0
const EQUATOR_M = 40075016.686;
const TILE_PX = 512;
// At most this many hexagons, else coarser ones
const MAX_CELLS = 12000;

// The hexagons are the finest at least this many pixels a side: about 6 on the world, 16 on a city
const minEdge = (zoom) => Math.min(16, 3 + 1.2 * zoom);

// A file of the page, loaded once however many maps and runs ask for it
function load(url, tag) {
  window.__weatherMapFiles ??= {};
  window.__weatherMapFiles[url] ??= new Promise((resolve, reject) => {
    const element = document.createElement(tag);
    if (tag === "link") Object.assign(element, { rel: "stylesheet", href: url });
    else element.src = url;
    element.onload = resolve;
    element.onerror = () => reject(new Error(`The map cannot load ${url}`));
    document.head.appendChild(element);
  });
  return window.__weatherMapFiles[url];
}

// The finest resolution whose hexagons are at least minEdge pixels a side at the center of the map, minEdge of the
// zoom showing its scale at the equator: the same at every latitude
function resolutionOf(map) {
  const metersPerPixel =
    (EQUATOR_M * Math.cos((map.getCenter().lat * Math.PI) / 180)) / (TILE_PX * 2 ** map.getZoom());
  const edge = minEdge(Math.log2(EQUATOR_M / (TILE_PX * metersPerPixel)));
  let resolution = 0;
  while (resolution < 15 && h3.getHexagonEdgeLengthAvg(resolution + 1, "m") / metersPerPixel >= edge) resolution++;
  return resolution;
}

// The hexagons over the view, in strips 60° wide: H3 reads a polygon wider than 180° the short way round
function cover(map, resolution) {
  const bounds = map.getBounds();
  const south = Math.max(bounds.getSouth(), -85);
  const north = Math.min(bounds.getNorth(), 85);
  const west = Math.max(bounds.getWest(), -180);
  const east = Math.min(bounds.getEast(), 180);
  const cells = new Set();
  for (let start = west; start < east; start += 60) {
    const end = Math.min(start + 60, east);
    for (const cell of h3.polygonToCells([[south, start], [south, end], [north, end], [north, start]], resolution)) {
      cells.add(cell);
    }
    if (cells.size > MAX_CELLS) return null;
  }
  return [...cells];
}

function layers(state) {
  const { data, cells, hover } = state;
  const accent = data.accent;
  const hexagon = { getHexagon: (cell) => cell, extruded: false, lineWidthUnits: "pixels" };
  const drawn = [];
  if (data.values.length) {
    drawn.push(
      new deck.H3HexagonLayer({
        id: "values",
        data: data.values,
        getHexagon: (row) => row.h3,
        getFillColor: (row) => row.color,
        extruded: false,
        stroked: false,
        pickable: true,
      }),
    );
  }
  drawn.push(
    new deck.H3HexagonLayer({
      ...hexagon,
      id: "grid",
      data: cells,
      filled: false,
      getLineColor: data.grid,
      getLineWidth: 1,
      updateTriggers: { getLineColor: data.grid },
    }),
  );
  if (hover) {
    drawn.push(
      new deck.H3HexagonLayer({
        ...hexagon,
        id: "hover",
        data: [hover],
        getFillColor: [...accent, 40],
        getLineColor: [...accent, 200],
        getLineWidth: 1.5,
      }),
    );
  }
  if (data.place) {
    drawn.push(
      new deck.H3HexagonLayer({
        ...hexagon,
        id: "place-hexagon",
        data: [data.place.cell],
        getFillColor: [...accent, 60],
        getLineColor: [...accent, 255],
        getLineWidth: 2.5,
      }),
      // A white dot with a dark ring on the point: visible on both ends of every color scale
      new deck.ScatterplotLayer({
        id: "place",
        data: [data.place],
        getPosition: (place) => [place.lng, place.lat],
        radiusUnits: "pixels",
        getRadius: 6,
        getFillColor: [255, 255, 255],
        stroked: true,
        getLineColor: [20, 20, 20],
        lineWidthUnits: "pixels",
        getLineWidth: 2,
      }),
    );
  }
  return drawn;
}

async function create(parentElement, data, setTriggerValue) {
  await load(STYLESHEET, "link");
  for (const url of LIBRARIES) await load(url, "script");
  const container = document.createElement("div");
  container.className = "weather-map";
  container.style.height = `${data.height}px`;
  parentElement.appendChild(container);
  const map = new maplibregl.Map({
    container,
    style: data.style,
    center: [data.view.lng, data.view.lat],
    zoom: data.view.zoom,
    renderWorldCopies: false,
    // On a phone one finger scrolls the page over the map and two move it, as a hint on the map says
    cooperativeGestures: data.phone,
    dragRotate: false,
    touchPitch: false,
    attributionControl: { compact: true },
  });
  map.touchZoomRotate.disableRotation();
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");
  const overlay = new deck.MapboxOverlay({
    interleaved: false,
    layers: [],
    getTooltip: ({ object, layer }) => (layer?.id === "values" && object ? { text: object.text } : null),
  });
  map.addControl(overlay);
  const state = { map, overlay, container, data, cells: [], hover: null, resolution: 0, view: data.view.id };
  state.draw = () => overlay.setProps({ layers: layers(state) });
  const coverView = () => {
    let resolution = resolutionOf(map);
    let cells = cover(map, resolution);
    while (cells === null && resolution > 0) cells = cover(map, --resolution);
    Object.assign(state, { resolution, cells: cells ?? [] });
    state.draw();
  };
  map.on("moveend", coverView);
  map.on("mousemove", (event) => {
    const cell = h3.latLngToCell(event.lngLat.lat, event.lngLat.lng, state.resolution);
    if (cell !== state.hover) {
      state.hover = cell;
      state.draw();
    }
  });
  map.on("mouseout", () => {
    state.hover = null;
    state.draw();
  });
  map.on("click", (event) => {
    const point = event.lngLat.wrap();
    setTriggerValue("click", { lat: point.lat, lng: point.lng, resolution: state.resolution });
  });
  coverView();
  return state;
}

export default async function (component) {
  const { data, parentElement, setTriggerValue } = component;
  // One map for the element, however many runs reach it before it is made
  parentElement.__weatherMap ??= create(parentElement, data, setTriggerValue);
  const state = await parentElement.__weatherMap;
  if (data.style !== state.data.style) state.map.setStyle(data.style);
  if (state.container.style.height !== `${data.height}px`) {
    state.container.style.height = `${data.height}px`;
    state.map.resize();
  }
  state.data = data;
  if (data.view.id !== state.view) {
    state.view = data.view.id;
    state.map.flyTo({ center: [data.view.lng, data.view.lat], zoom: data.view.zoom, duration: 800 });
  }
  state.draw();
  return () => {
    state.overlay.finalize();
    state.map.remove();
    delete parentElement.__weatherMap;
  };
}
