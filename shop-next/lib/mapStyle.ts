// Своя схема карты для доставки в стиле Magic Vibes (MapLibre, данные OpenStreetMap через OpenFreeMap, без ключей).
// Рисуем то, по чему человек узнаёт своё место: дома с номерами, все улицы и проезды (в том числе в частном
// секторе), метро и заметные ориентиры (ТЦ, школы, больницы, вокзалы), вода, парки, районы и города.
// Не рисуем: аэродромы, военные и промышленные зоны, границы, горы — они только мешают найти дом.
// Номеров домов нет там, где их нет в OpenStreetMap (частный сектор, сёла): для курьера номер можно ввести вручную.
import type { StyleSpecification } from "maplibre-gl";

const C = {
  bg: "#f5f2ec", residential: "#efeae0", water: "#bfdcec", waterLine: "#a9cfe3", park: "#dcebc6", wood: "#d3e4bd",
  building: "#e4ddd0", buildingLine: "#cfc6b5", road: "#ffffff", roadCase: "#ddd5c6",
  major: "#ffffff", majorCase: "#cbc1ad", motorway: "#eefbb4", motorwayCase: "#c6d98a", track: "#d6ccb9",
  text: "#2b2825", textMuted: "#6f685e", halo: "#ffffff",
  metro: "#ff3d7f", poi: "#8a7fd6",
};
const FONT = ["Noto Sans Regular"];
const FONT_B = ["Noto Sans Bold"];
const name = ["coalesce", ["get", "name:ru"], ["get", "name"]] as unknown as string;
const w = (stops: [number, number][]) => ["interpolate", ["exponential", 1.6], ["zoom"], ...stops.flat()] as unknown as number;
const cls = (list: string[]) => ["in", ["get", "class"], ["literal", list]] as unknown as boolean;

const MINOR = ["minor", "service"];
const MAJOR = ["primary", "secondary", "tertiary", "trunk"];

export const DELIVERY_MAP_STYLE: StyleSpecification = {
  version: 8,
  // through our own domain: Russian ISPs throttle tiles.openfreemap.org (nginx /map/ proxies and caches it).
  // /map/, not /ofm/: browsers cached /ofm/ answers with a doubled CORS header for a week
  glyphs: "https://magicvibes.ru/map/fonts/{fontstack}/{range}.pbf",
  sources: {
    omt: {
      type: "vector", url: "https://magicvibes.ru/map/planet",
      // лицензия ODbL требует указать источник данных
      attribution: '<a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">© OpenStreetMap</a> · <a href="https://openfreemap.org" target="_blank" rel="noopener">OpenFreeMap</a>',
    },
  },
  layers: [
    { id: "bg", type: "background", paint: { "background-color": C.bg } },
    // жилые кварталы чуть темнее фона — видно, где дома
    { id: "residential", type: "fill", source: "omt", "source-layer": "landuse", minzoom: 10, filter: cls(["residential", "suburb", "neighbourhood"]), paint: { "fill-color": C.residential } },
    { id: "wood", type: "fill", source: "omt", "source-layer": "landcover", filter: cls(["wood"]), paint: { "fill-color": C.wood, "fill-opacity": 0.75 } },
    { id: "park", type: "fill", source: "omt", "source-layer": "park", paint: { "fill-color": C.park } },
    { id: "water", type: "fill", source: "omt", "source-layer": "water", paint: { "fill-color": C.water } },
    { id: "waterway", type: "line", source: "omt", "source-layer": "waterway", minzoom: 10, paint: { "line-color": C.waterLine, "line-width": w([[10, 0.6], [16, 3]]) } },
    // здания
    { id: "building", type: "fill", source: "omt", "source-layer": "building", minzoom: 13, paint: { "fill-color": C.building, "fill-outline-color": C.buildingLine } },
    // дороги: проезды частного сектора и грунтовки, затем обводка, затем заливка
    { id: "road-track", type: "line", source: "omt", "source-layer": "transportation", minzoom: 14, filter: cls(["track"]),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.track, "line-width": w([[14, 1], [18, 6]]) } },
    { id: "road-path", type: "line", source: "omt", "source-layer": "transportation", minzoom: 16, filter: cls(["path"]),
      layout: { "line-cap": "round" }, paint: { "line-color": C.track, "line-width": w([[16, 0.8], [18, 2]]), "line-dasharray": [2, 1.5] } },
    { id: "road-minor-case", type: "line", source: "omt", "source-layer": "transportation", minzoom: 12, filter: cls(MINOR),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.roadCase, "line-width": w([[12, 1], [18, 15]]) } },
    { id: "road-major-case", type: "line", source: "omt", "source-layer": "transportation", minzoom: 8, filter: cls(MAJOR),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.majorCase, "line-width": w([[8, 1], [18, 24]]) } },
    { id: "road-motorway-case", type: "line", source: "omt", "source-layer": "transportation", minzoom: 6, filter: cls(["motorway"]),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.motorwayCase, "line-width": w([[6, 1.2], [18, 28]]) } },
    { id: "road-minor", type: "line", source: "omt", "source-layer": "transportation", minzoom: 12, filter: cls(MINOR),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.road, "line-width": w([[12, 0.5], [18, 12]]) } },
    { id: "road-major", type: "line", source: "omt", "source-layer": "transportation", minzoom: 8, filter: cls(MAJOR),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.major, "line-width": w([[8, 0.6], [18, 20]]) } },
    { id: "road-motorway", type: "line", source: "omt", "source-layer": "transportation", minzoom: 6, filter: cls(["motorway"]),
      layout: { "line-cap": "round", "line-join": "round" }, paint: { "line-color": C.motorway, "line-width": w([[6, 0.8], [18, 24]]) } },
    // номера всех домов, что есть в OpenStreetMap
    { id: "housenumber", type: "symbol", source: "omt", "source-layer": "housenumber", minzoom: 15,
      layout: { "text-field": ["get", "housenumber"], "text-font": FONT, "text-size": ["interpolate", ["linear"], ["zoom"], 15, 10, 18, 13], "text-padding": 1, "text-allow-overlap": false },
      paint: { "text-color": C.textMuted, "text-halo-color": C.halo, "text-halo-width": 1.2 } },
    // названия улиц, проездов и переулков
    { id: "road-label", type: "symbol", source: "omt", "source-layer": "transportation_name", minzoom: 13,
      filter: cls(["motorway", "trunk", "primary", "secondary", "tertiary", "minor", "service", "track"]),
      layout: { "symbol-placement": "line", "text-field": name, "text-font": FONT, "text-size": ["interpolate", ["linear"], ["zoom"], 13, 10, 18, 14], "text-max-angle": 30 },
      paint: { "text-color": C.text, "text-halo-color": C.halo, "text-halo-width": 1.6 } },
    // метро: розовая точка и название станции
    { id: "metro-dot", type: "circle", source: "omt", "source-layer": "poi", minzoom: 11,
      filter: ["all", ["==", ["get", "class"], "railway"], ["in", ["get", "subclass"], ["literal", ["subway", "station"]]]] as unknown as boolean,
      paint: { "circle-color": C.metro, "circle-radius": ["interpolate", ["linear"], ["zoom"], 11, 3, 16, 6], "circle-stroke-color": "#ffffff", "circle-stroke-width": 1.5 } },
    { id: "metro-label", type: "symbol", source: "omt", "source-layer": "poi", minzoom: 13,
      filter: ["all", ["==", ["get", "class"], "railway"], ["in", ["get", "subclass"], ["literal", ["subway", "station"]]]] as unknown as boolean,
      layout: { "text-field": name, "text-font": FONT_B, "text-size": 12, "text-offset": [0, 1.1], "text-anchor": "top", "text-max-width": 8 },
      paint: { "text-color": "#c4225a", "text-halo-color": C.halo, "text-halo-width": 1.6 } },
    // ориентиры: ТЦ, школы, больницы, почта, администрация, храмы — без вывесок отдельных магазинов
    { id: "poi-label", type: "symbol", source: "omt", "source-layer": "poi", minzoom: 15,
      filter: ["any",
        cls(["school", "college", "hospital", "post", "town_hall", "place_of_worship"]),
        ["all", ["==", ["get", "class"], "shop"], ["in", ["get", "subclass"], ["literal", ["mall", "department_store"]]]],
      ] as unknown as boolean,
      layout: { "text-field": name, "text-font": FONT, "text-size": 11, "text-max-width": 9, "text-padding": 6, "text-optional": true },
      paint: { "text-color": C.poi, "text-halo-color": C.halo, "text-halo-width": 1.4 } },
    // районы и кварталы
    { id: "place-suburb", type: "symbol", source: "omt", "source-layer": "place", minzoom: 11, maxzoom: 16, filter: cls(["suburb", "quarter", "neighbourhood"]),
      layout: { "text-field": name, "text-font": FONT, "text-size": 12, "text-transform": "uppercase", "text-letter-spacing": 0.06, "text-max-width": 8 },
      paint: { "text-color": C.textMuted, "text-halo-color": C.halo, "text-halo-width": 1.5 } },
    // сёла, посёлки, деревни
    { id: "place-village", type: "symbol", source: "omt", "source-layer": "place", minzoom: 9, filter: cls(["village", "hamlet", "isolated_dwelling"]),
      layout: { "text-field": name, "text-font": FONT_B, "text-size": ["interpolate", ["linear"], ["zoom"], 9, 11, 15, 15] },
      paint: { "text-color": C.text, "text-halo-color": C.halo, "text-halo-width": 1.8 } },
    // города
    { id: "place-city", type: "symbol", source: "omt", "source-layer": "place", maxzoom: 15, filter: cls(["city", "town"]),
      layout: { "text-field": name, "text-font": FONT_B, "text-size": ["interpolate", ["linear"], ["zoom"], 4, 11, 12, 18] },
      paint: { "text-color": C.text, "text-halo-color": C.halo, "text-halo-width": 2 } },
  ],
};
