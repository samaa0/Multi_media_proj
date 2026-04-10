import copy
import json
from pathlib import Path


OUT_FILE = Path("Smart_Mobility_Dynamic_Parking_Flow.json")
DOCKER_FLOW_FILE = Path("docker/node-red/data/flows.json")


def node(node_id, node_type, z="", **kwargs):
    item = {"id": node_id, "type": node_type, "z": z}
    item.update(kwargs)
    return item


setup_tab = "tab_setup"
ingest_tab = "tab_ingest"
analytics_tab = "tab_analytics"
dashboard_tab = "tab_dashboard"

mongo_cfg = "cfg_mongo"
ui_base = "cfg_ui_base"
ui_tab = "cfg_ui_tab"
group_controls = "grp_controls"
group_alerts = "grp_alerts"
group_meter = "grp_meter"
group_private = "grp_private"
group_history = "grp_history"


load_profiles_func = r"""
const defaultProfile = {
    id: "ifc_central",
    name: "IFC Mall",
    query: "IFC Mall, Central",
    areaName: "Central",
    district: "Central and Western",
    address: "8 Finance Street, Central, Hong Kong",
    latitude: 22.2840,
    longitude: 114.1588,
    radiusMeters: 1200,
    trafficKeywords: ["central", "admiralty", "harcourt", "ifc", "finance street", "connaught road"]
};

global.set("defaultDestinationProfile", defaultProfile);
if (!global.get("selectedDestinationQuery")) {
    global.set("selectedDestinationQuery", defaultProfile.query);
}
if (!global.get("selectedDestinationProfile")) {
    global.set("selectedDestinationProfile", defaultProfile);
}
if (!global.get("parkingPreference")) {
    global.set("parkingPreference", "budget");
}

msg.payload = {
    seedType: "defaultDestinationProfile",
    version: "v1",
    seededAt: new Date().toISOString(),
    profile: defaultProfile
};
return msg;
"""


prime_dashboard_state_func = r"""
const defaultProfile = global.get("defaultDestinationProfile") || {
    query: "IFC Mall, Central"
};
global.set("selectedDestinationQuery", defaultProfile.query);
global.set("selectedDestinationProfile", defaultProfile);
global.set("parkingPreference", "budget");
return [
    { payload: defaultProfile.query, topic: "destination" },
    { payload: "", options: [] },
    { payload: "budget", topic: "preference" },
    { payload: "refresh", topic: "refresh" }
];
"""


store_destination_func = r"""
const defaultProfile = global.get("defaultDestinationProfile") || { query: "IFC Mall, Central" };
const value = (msg.payload || defaultProfile.query).toString().trim();
global.set("selectedDestinationQuery", value || defaultProfile.query);
msg.payload = "refresh";
msg.topic = "refresh";
return msg;
"""


store_preference_func = r"""
const value = (msg.payload || "budget").toString();
global.set("parkingPreference", value);
msg.payload = "refresh";
msg.topic = "refresh";
return msg;
"""


build_refresh_message_func = r"""
msg.payload = "refresh";
msg.topic = "refresh";
return msg;
"""


build_geocode_request_func = r"""
const fallback = global.get("defaultDestinationProfile") || { query: "IFC Mall, Central" };
const query = (global.get("selectedDestinationQuery") || fallback.query || "IFC Mall, Central").toString().trim();
msg.url = `https://geodata.gov.hk/gs/api/v1.0.0/locationSearch?q=${encodeURIComponent(query)}&lang=en`;
msg.headers = {
    "User-Agent": "Mozilla/5.0"
};
msg.destinationQuery = query;
return msg;
"""


parse_geocode_result_func = r"""
function slugify(text) {
    return String(text || "")
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, "_")
        .replace(/^_+|_+$/g, "");
}

function buildKeywords(...parts) {
    const text = parts.filter(Boolean).join(" ").toLowerCase();
    return Array.from(new Set(text.split(/[^a-z0-9]+/).filter((item) => item && item.length > 2))).slice(0, 10);
}

function normalizeText(text) {
    return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function detectRegion(result) {
    const district = String(result && result.districtEN || "");
    const address = String(result && result.addressEN || "");
    const haystack = `${district} ${address}`.toLowerCase();
    if (/(central and western|wan chai|eastern|southern|hong kong)/.test(haystack)) {
        return "Hong Kong Island";
    }
    if (/(yau tsim mong|sham shui po|kowloon city|wong tai sin|kwun tong|kowloon)/.test(haystack)) {
        return "Kowloon";
    }
    if (/(tsuen wan|tuen mun|yuen long|north district|tai po|sha tin|sai kung|islands|new territories|tseung kwan o|ma on shan|tin shui wai)/.test(haystack)) {
        return "New Territories";
    }
    return "All Hong Kong";
}

function scoreResult(result, query) {
    const haystack = normalizeText([result.nameEN, result.addressEN, result.districtEN].filter(Boolean).join(" "));
    const normalizedQuery = normalizeText(query);
    let score = 0;
    if (!normalizedQuery) {
        return score;
    }
    if (haystack.includes(normalizedQuery)) {
        score += 80;
    }
    const tokens = normalizedQuery.split(/\s+/).filter((item) => item && item.length > 0);
    for (const token of tokens) {
        if (haystack.includes(token)) {
            score += token.length <= 3 ? 14 : 10;
        }
        const words = haystack.split(/\s+/);
        if (words.some((word) => word.startsWith(token))) {
            score += 8;
        }
        if (String(result.nameEN || "").toLowerCase().includes(token)) {
            score += 12;
        }
        if (String(result.addressEN || "").toLowerCase().includes(token)) {
            score += 5;
        }
        if (String(result.districtEN || "").toLowerCase().includes(token)) {
            score += 4;
        }
    }
    return score;
}

const fallback = global.get("defaultDestinationProfile") || {
    id: "ifc_central",
    name: "IFC Mall",
    query: "IFC Mall, Central",
    areaName: "Central",
    district: "Central and Western",
    address: "8 Finance Street, Central, Hong Kong",
    latitude: 22.2840,
    longitude: 114.1588,
    radiusMeters: 1200,
    trafficKeywords: ["central", "admiralty", "harcourt", "ifc", "finance street", "connaught road"]
};

const query = (msg.destinationQuery || global.get("selectedDestinationQuery") || fallback.query).toString().trim();
const results = Array.isArray(msg.payload) ? msg.payload : [];
const proj4 = global.get("proj4");

let selectedProfile = global.get("selectedDestinationProfile") || fallback;

const profiles = [];
if (results.length && proj4) {
    proj4.defs("EPSG:2326", "+proj=tmerc +lat_0=22.31213333333334 +lon_0=114.1785555555556 +k=1 +x_0=836694.05 +y_0=819069.8 +ellps=intl +towgs84=-162.619,-276.959,-161.764,-1.719,0.067,1.092,1.27 +units=m +no_defs +type=crs");
    const ranked = results
        .slice()
        .sort((a, b) => scoreResult(b, query) - scoreResult(a, query))
        .slice(0, 12);
    const seenIds = new Set();
    for (const item of ranked) {
        const converted = proj4("EPSG:2326", "EPSG:4326", [Number(item.x), Number(item.y)]);
        const longitude = Array.isArray(converted) ? Number(converted[0]) : fallback.longitude;
        const latitude = Array.isArray(converted) ? Number(converted[1]) : fallback.latitude;
        const profile = {
            id: slugify(`${item.nameEN || query}_${item.addressEN || item.districtEN || ""}`) || "custom_destination",
            name: item.nameEN || query,
            query,
            areaName: (item.districtEN || fallback.areaName || "Hong Kong").replace(/ District$/i, ""),
            district: item.districtEN || fallback.district,
            region: detectRegion(item),
            address: item.addressEN || query,
            latitude: Number.isFinite(latitude) ? latitude : fallback.latitude,
            longitude: Number.isFinite(longitude) ? longitude : fallback.longitude,
            radiusMeters: 1200,
            trafficKeywords: buildKeywords(query, item.nameEN, item.addressEN, item.districtEN)
        };
        if (seenIds.has(profile.id)) {
            continue;
        }
        seenIds.add(profile.id);
        profiles.push(profile);
    }
}

if (!profiles.length) {
    profiles.push({
        ...selectedProfile,
        region: selectedProfile.region || detectRegion(selectedProfile)
    });
}

selectedProfile = profiles[0];
global.set("selectedDestinationQuery", query);
global.set("selectedDestinationProfile", selectedProfile);
global.set("searchResultProfiles", profiles);

const dropdownOptions = profiles.map((item) => ({
    [`${item.name} · ${item.region || "All Hong Kong"} · ${item.district || item.areaName}`]: item.id
}));

return [
    {
        options: dropdownOptions,
        payload: selectedProfile.id
    },
    {
        payload: selectedProfile.id,
        topic: "selectedResultId"
    }
];
"""


apply_selected_result_func = r"""
const fallback = global.get("defaultDestinationProfile") || {};
const profiles = global.get("searchResultProfiles") || [];
const selectedId = (msg.payload || "").toString();
const selected = profiles.find((item) => item.id === selectedId) || global.get("selectedDestinationProfile") || fallback;
global.set("selectedDestinationProfile", selected);
global.set("selectedDestinationQuery", selected.query || selected.name || fallback.query || "IFC Mall, Central");
msg.payload = "refresh";
msg.topic = "refresh";
return msg;
"""


normalize_meter_snapshot_func = r"""
function normalizeKey(text) {
    return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function directLookup(obj, candidate) {
    if (Object.prototype.hasOwnProperty.call(obj, candidate)) {
        return obj[candidate];
    }
    const target = normalizeKey(candidate);
    const key = Object.keys(obj).find((item) => normalizeKey(item) === target);
    return key ? obj[key] : undefined;
}

function lookupByTokens(obj, tokens) {
    for (const token of tokens) {
        const direct = directLookup(obj, token);
        if (direct !== undefined && direct !== null && direct !== "") {
            return direct;
        }
    }
    const key = Object.keys(obj).find((item) => {
        const normalized = normalizeKey(item);
        return tokens.some((token) => normalized.includes(normalizeKey(token)));
    });
    return key ? obj[key] : undefined;
}

function toNumber(value) {
    if (value === undefined || value === null || value === "") {
        return null;
    }
    const parsed = Number(String(value).replace(/[^0-9.\-]+/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
}

function classifyVacancy(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (!raw) {
        return null;
    }
    if (["vacant", "available", "free", "yes", "y", "v"].includes(raw)) {
        return 1;
    }
    if (["occupied", "full", "unavailable", "no", "n", "o"].includes(raw)) {
        return 0;
    }
    return null;
}

const infoRows = Array.isArray(msg.payload.meterInfo) ? msg.payload.meterInfo : [];
const occupancyRows = Array.isArray(msg.payload.meterOccupancy) ? msg.payload.meterOccupancy : [];

const metaBySpace = {};
for (const row of infoRows) {
    const spaceId = lookupByTokens(row, ["spaceid", "parkingspaceid", "parking_space_id", "meterid", "id"]);
    if (!spaceId) {
        continue;
    }
    metaBySpace[String(spaceId)] = {
        facilityName: lookupByTokens(row, ["street", "roadname", "road", "streetname", "name", "location"]) || `Meter Cluster ${spaceId}`,
        district: lookupByTokens(row, ["district", "districtname", "area"]) || "Unknown",
        latitude: toNumber(lookupByTokens(row, ["latitude", "lat", "y"])),
        longitude: toNumber(lookupByTokens(row, ["longitude", "lng", "lon", "x"]))
    };
}

const groups = {};
for (const row of occupancyRows) {
    const spaceId = lookupByTokens(row, ["spaceid", "parkingspaceid", "parking_space_id", "meterid", "id"]);
    const meta = metaBySpace[String(spaceId)] || {};
    const facilityName = meta.facilityName || lookupByTokens(row, ["street", "roadname", "road", "location", "name"]) || `Meter Cluster ${spaceId || "Unknown"}`;
    const district = meta.district || lookupByTokens(row, ["district", "districtname", "area"]) || "Unknown";
    const groupId = `${district}::${facilityName}`;
    if (!groups[groupId]) {
        groups[groupId] = {
            sourceType: "meter",
            facilityId: groupId,
            facilityName,
            district,
            latitudeSum: 0,
            longitudeSum: 0,
            coordinateCount: 0,
            availableSpaces: 0,
            totalSpaces: 0,
            isCovered: false
        };
    }
    const item = groups[groupId];
    const lat = meta.latitude !== null ? meta.latitude : toNumber(lookupByTokens(row, ["latitude", "lat", "y"]));
    const lon = meta.longitude !== null ? meta.longitude : toNumber(lookupByTokens(row, ["longitude", "lng", "lon", "x"]));
    if (lat !== null && lon !== null) {
        item.latitudeSum += lat;
        item.longitudeSum += lon;
        item.coordinateCount += 1;
    }

    const rowAvailable = toNumber(lookupByTokens(row, ["available", "vacant", "availablespaces"]));
    const rowTotal = toNumber(lookupByTokens(row, ["totalspaces", "totalspaces", "spaces", "capacity"]));
    if (rowAvailable !== null || rowTotal !== null) {
        item.availableSpaces += rowAvailable !== null ? rowAvailable : 0;
        item.totalSpaces += rowTotal !== null ? rowTotal : Math.max(rowAvailable || 0, 0);
        continue;
    }

    const vacancyFlag = classifyVacancy(lookupByTokens(row, ["OccupancyStatus", "occupancystatus", "vacancystatus", "vacancy_status"]));
    if (vacancyFlag !== null) {
        item.availableSpaces += vacancyFlag;
        item.totalSpaces += 1;
    }
}

const snapshotTime = new Date().toISOString();
const records = Object.values(groups).map((item) => ({
    sourceType: item.sourceType,
    facilityId: item.facilityId,
    facilityName: item.facilityName,
    district: item.district,
    latitude: item.coordinateCount ? item.latitudeSum / item.coordinateCount : null,
    longitude: item.coordinateCount ? item.longitudeSum / item.coordinateCount : null,
    availableSpaces: item.availableSpaces,
    totalSpaces: item.totalSpaces || null,
    isCovered: false,
    snapshotTime
}));

msg.payload = {
    sourceType: "meter",
    dataset: "td_meter_occupancy_and_distribution",
    snapshotTime,
    recordCount: records.length,
    records
};
return msg;
"""


sanitize_meter_info_csv_func = r"""
const raw = String(msg.payload || "").replace(/^\uFEFF/, "");
const lines = raw.split(/\r?\n/);
const headerIndex = lines.findIndex((line) => line.startsWith("PoleId,ParkingSpaceId,"));
msg.payload = headerIndex >= 0 ? lines.slice(headerIndex).join("\n") : raw;
return msg;
"""


normalize_private_snapshot_func = r"""
function normalizeKey(text) {
    return String(text || "").toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function toNumber(value) {
    if (value === undefined || value === null || value === "") {
        return null;
    }
    const parsed = Number(String(value).replace(/[^0-9.\-]+/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
}

function getValue(obj, candidates) {
    for (const candidate of candidates) {
        if (Object.prototype.hasOwnProperty.call(obj, candidate) && obj[candidate] !== undefined && obj[candidate] !== null && obj[candidate] !== "") {
            return obj[candidate];
        }
    }
    const normalizedMap = {};
    for (const key of Object.keys(obj || {})) {
        normalizedMap[normalizeKey(key)] = obj[key];
    }
    for (const candidate of candidates) {
        const normalized = normalizeKey(candidate);
        if (normalizedMap[normalized] !== undefined && normalizedMap[normalized] !== null && normalizedMap[normalized] !== "") {
            return normalizedMap[normalized];
        }
    }
    const fuzzyKey = Object.keys(obj || {}).find((key) => {
        const normalized = normalizeKey(key);
        return candidates.some((candidate) => normalized.includes(normalizeKey(candidate)));
    });
    return fuzzyKey ? obj[fuzzyKey] : undefined;
}

function collectObjects(node, bucket) {
    if (Array.isArray(node)) {
        node.forEach((item) => collectObjects(item, bucket));
        return;
    }
    if (!node || typeof node !== "object") {
        return;
    }
    const keys = Object.keys(node);
    const normalizedKeys = keys.map((key) => normalizeKey(key));
    const looksLikeCarpark = normalizedKeys.some((key) => ["parkid", "carparkid", "id", "name", "displayname", "latitude", "vacancy"].includes(key));
    if (looksLikeCarpark && keys.length > 1) {
        bucket.push(node);
    }
    keys.forEach((key) => collectObjects(node[key], bucket));
}

function extractPrivateVehicleValue(value) {
    if (Array.isArray(value)) {
        for (const item of value) {
            const extracted = extractPrivateVehicleValue(item);
            if (extracted !== null) {
                return extracted;
            }
        }
        return null;
    }
    if (value && typeof value === "object") {
        const vehicleType = String(getValue(value, ["vehicleType", "vehicletype", "type"]) || "").toLowerCase();
        if (!vehicleType || vehicleType.includes("private")) {
            return toNumber(getValue(value, ["vacancy", "available", "vacant", "value", "space", "spaces"]));
        }
        return null;
    }
    return toNumber(value);
}

const infoObjects = [];
const vacancyObjects = [];
collectObjects(msg.payload.privateInfo, infoObjects);
collectObjects(msg.payload.privateVacancy, vacancyObjects);

const infoById = {};
for (const item of infoObjects) {
    const parkId = getValue(item, ["park_Id", "parkId", "carparkId", "id"]);
    if (!parkId) {
        continue;
    }
    infoById[String(parkId)] = item;
}

const vacancyById = {};
for (const item of vacancyObjects) {
    const parkId = getValue(item, ["park_Id", "parkId", "carparkId", "id"]);
    if (!parkId) {
        continue;
    }
    vacancyById[String(parkId)] = item;
}

const snapshotTime = new Date().toISOString();
const ids = Array.from(new Set(Object.keys(infoById).concat(Object.keys(vacancyById))));
const records = ids.map((parkId) => {
    const info = infoById[parkId] || {};
    const vacancy = vacancyById[parkId] || {};
    const facilityName = getValue(info, ["name", "displayName", "carparkName", "parkName"]) ||
        getValue(vacancy, ["name", "displayName", "carparkName", "parkName"]) ||
        `Private Car Park ${parkId}`;
    const district = getValue(info, ["district", "districtName", "district_en"]) ||
        getValue(vacancy, ["district", "districtName", "district_en"]) ||
        "Unknown";
    const coveredDescriptor = String(getValue(info, ["carParkType", "type", "remarks", "address", "name"]) || "").toLowerCase();
    const isCovered = !coveredDescriptor.includes("open air");
    const availableSpaces = extractPrivateVehicleValue(getValue(vacancy, ["privateCar", "vacancy", "vacancies", "parkingSpace"]));
    const totalSpaces = toNumber(getValue(info, ["totalCapacity", "capacity", "spaces", "spaceCount"]));
    return {
        sourceType: "private",
        facilityId: parkId,
        facilityName,
        district,
        latitude: toNumber(getValue(info, ["latitude", "lat", "y"])),
        longitude: toNumber(getValue(info, ["longitude", "lng", "lon", "x"])),
        availableSpaces: availableSpaces !== null ? availableSpaces : 0,
        totalSpaces,
        isCovered,
        snapshotTime
    };
});

msg.payload = {
    sourceType: "private",
    dataset: "dpo_one_stop_carpark_info_vacancy",
    snapshotTime,
    recordCount: records.length,
    records
};
return msg;
"""


normalize_traffic_snapshot_func = r"""
function flatten(obj, prefix, bag) {
    if (Array.isArray(obj)) {
        obj.forEach((item, index) => flatten(item, `${prefix}${index}.`, bag));
        return;
    }
    if (!obj || typeof obj !== "object") {
        bag[prefix.slice(0, -1)] = obj;
        return;
    }
    Object.keys(obj).forEach((key) => flatten(obj[key], `${prefix}${key}.`, bag));
}

function numericFromFlat(flat, tokens) {
    for (const key of Object.keys(flat)) {
        const normalized = key.toLowerCase();
        if (tokens.some((token) => normalized.includes(token))) {
            const raw = flat[key];
            const parsed = Number(String(raw).replace(/[^0-9.\-]+/g, ""));
            if (Number.isFinite(parsed)) {
                return parsed;
            }
        }
    }
    return null;
}

function textFromFlat(flat, tokens) {
    for (const key of Object.keys(flat)) {
        const normalized = key.toLowerCase();
        if (tokens.some((token) => normalized.includes(token))) {
            const raw = flat[key];
            if (raw !== undefined && raw !== null && String(raw).trim()) {
                return String(raw).trim();
            }
        }
    }
    return "";
}

function traverse(node, bucket) {
    if (Array.isArray(node)) {
        node.forEach((item) => traverse(item, bucket));
        return;
    }
    if (!node || typeof node !== "object") {
        return;
    }
    const flat = {};
    flatten(node, "", flat);
    const currentMinutes = numericFromFlat(flat, ["journeytime", "traveltime", "timevalue", "currenttime", "journey"]);
    const referenceMinutes = numericFromFlat(flat, ["normal", "reference", "freeflow", "basetime"]);
    const labelParts = [
        textFromFlat(flat, ["locationid", "jtiid", "id"]),
        textFromFlat(flat, ["from", "origin", "start"]),
        textFromFlat(flat, ["to", "destination", "dest"]),
        textFromFlat(flat, ["road", "route", "corridor", "description"])
    ].filter(Boolean);
    if (currentMinutes !== null && labelParts.length) {
        const label = labelParts.join(" | ");
        const congestionFactor = referenceMinutes && referenceMinutes > 0 ? currentMinutes / referenceMinutes : 1;
        bucket.push({
            label,
            currentMinutes,
            referenceMinutes,
            congestionFactor
        });
    }
    Object.keys(node).forEach((key) => traverse(node[key], bucket));
}

const records = [];
traverse(msg.payload, records);
const deduped = [];
const seen = new Set();
for (const item of records) {
    if (seen.has(item.label)) {
        continue;
    }
    seen.add(item.label);
    deduped.push(item);
}

const snapshotTime = new Date().toISOString();
msg.payload = {
    sourceType: "traffic",
    dataset: "td_journey_time_indicators_v2",
    snapshotTime,
    recordCount: deduped.length,
    records: deduped
};
return msg;
"""


normalize_weather_snapshot_func = r"""
function toNumber(value) {
    if (value === undefined || value === null || value === "") {
        return null;
    }
    const parsed = Number(String(value).replace(/[^0-9.\-]+/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
}

function getCurrentTemperature(payload) {
    const block = payload && payload.temperature && Array.isArray(payload.temperature.data) ? payload.temperature.data : [];
    const hkoRecord = block.find((item) => String(item.place || "").toLowerCase() === "hong kong observatory");
    if (hkoRecord) {
        return toNumber(hkoRecord.value);
    }
    return block.length ? toNumber(block[0].value) : null;
}

function getWeatherIconNames(payload) {
    const names = payload && Array.isArray(payload.icon) ? payload.icon.map((item) => String(item)) : [];
    return names;
}

function collectRainfall(payload) {
    const rows = payload && Array.isArray(payload.hourlyRainfall) ? payload.hourlyRainfall : [];
    const mapped = rows.map((item) => ({
        automaticWeatherStation: item.automaticWeatherStation || item.automaticWeatherStation_en || item.name || "Unknown",
        rainfall: toNumber(item.value)
    }));
    const maxRainfall = mapped.reduce((max, item) => Math.max(max, item.rainfall || 0), 0);
    return { mapped, maxRainfall };
}

const currentWeather = msg.payload.currentWeather || {};
const hourlyRain = msg.payload.hourlyRain || {};
const rainfall = collectRainfall(hourlyRain);
const warningMessages = Array.isArray(currentWeather.warningMessage) ? currentWeather.warningMessage : [];
const isRaining = rainfall.maxRainfall >= 0.5 || warningMessages.some((item) => String(item).toLowerCase().includes("rain"));
const snapshotTime = new Date().toISOString();

msg.payload = {
    sourceType: "weather",
    dataset: "hko_current_weather_and_hourly_rainfall",
    snapshotTime,
    temperatureCelsius: getCurrentTemperature(currentWeather),
    humidity: toNumber(currentWeather.humidity && currentWeather.humidity.data && currentWeather.humidity.data[0] && currentWeather.humidity.data[0].value),
    weatherIcons: getWeatherIconNames(currentWeather),
    warningMessages,
    rainfallStations: rainfall.mapped,
    maxRainfall: rainfall.maxRainfall,
    isRaining
};
return msg;
"""


build_range_query_func = r"""
msg.payload = {};
return msg;
"""


compute_analytics_package_func = r"""
function getProfiles() {
    return global.get("defaultDestinationProfile") || {
        id: "ifc_central",
        name: "IFC Mall",
        query: "IFC Mall, Central",
        areaName: "Central",
        district: "Central and Western",
        address: "8 Finance Street, Central, Hong Kong",
        latitude: 22.2840,
        longitude: 114.1588,
        radiusMeters: 1200,
        trafficKeywords: ["central", "admiralty", "harcourt", "ifc", "finance street", "connaught road"]
    };
}

function parseIso(value) {
    const time = Date.parse(value);
    return Number.isFinite(time) ? time : 0;
}

function sortDocs(docs) {
    let list;
    if (Array.isArray(docs)) {
        list = docs.slice();
    } else if (docs && typeof docs === "object" && Object.prototype.hasOwnProperty.call(docs, "snapshotTime")) {
        list = [docs];
    } else {
        list = Object.values(docs || {});
    }
    return list.sort((a, b) => parseIso(a.snapshotTime) - parseIso(b.snapshotTime));
}

function latestDoc(docs) {
    const sorted = sortDocs(docs);
    return sorted.length ? sorted[sorted.length - 1] : null;
}

function docBefore(docs, targetTs) {
    const sorted = sortDocs(docs);
    let candidate = null;
    for (const item of sorted) {
        if (parseIso(item.snapshotTime) <= targetTs) {
            candidate = item;
        }
    }
    return candidate || (sorted.length ? sorted[0] : null);
}

function toNumber(value) {
    if (value === undefined || value === null || value === "") {
        return null;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function haversine(lat1, lon1, lat2, lon2) {
    if ([lat1, lon1, lat2, lon2].some((value) => value === null || value === undefined)) {
        return null;
    }
    const toRad = (deg) => deg * Math.PI / 180;
    const R = 6371000;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
}

function candidateFilter(records, profile, sourceType) {
    const valid = (records || []).map((record) => {
        const distance = haversine(profile.latitude, profile.longitude, record.latitude, record.longitude);
        return Object.assign({}, record, { distanceMeters: distance });
    }).filter((record) => {
        if (record.distanceMeters !== null && record.distanceMeters <= profile.radiusMeters) {
            return true;
        }
        return String(record.district || "").toLowerCase().includes(String(profile.district || "").toLowerCase());
    });

    valid.sort((a, b) => {
        const aDist = a.distanceMeters === null ? Number.MAX_SAFE_INTEGER : a.distanceMeters;
        const bDist = b.distanceMeters === null ? Number.MAX_SAFE_INTEGER : b.distanceMeters;
        return aDist - bDist;
    });

    const filtered = sourceType === "private" ? valid.filter((item) => item.sourceType === "private" || item.isCovered) : valid;
    return filtered.slice(0, 6);
}

function buildRecordMap(doc) {
    const map = {};
    const rows = doc && Array.isArray(doc.records) ? doc.records : [];
    rows.forEach((item) => {
        map[item.facilityId] = item;
    });
    return map;
}

function deriveTraffic(profile, trafficDoc) {
    const records = trafficDoc && Array.isArray(trafficDoc.records) ? trafficDoc.records : [];
    if (!records.length) {
        return { travelEstimateMinutes: 12, congestionLabel: "Moderate", supportingLabel: "No live traffic record matched the selected destination." };
    }
    let best = null;
    let bestScore = -1;
    for (const record of records) {
        const text = String(record.label || "").toLowerCase();
        const keywordScore = profile.trafficKeywords.reduce((score, keyword) => score + (text.includes(keyword) ? 2 : 0), 0);
        const congestionScore = record.congestionFactor ? Math.min(record.congestionFactor * 2, 4) : 0;
        const score = keywordScore + congestionScore;
        if (score > bestScore) {
            bestScore = score;
            best = record;
        }
    }
    const minutes = best && best.currentMinutes ? best.currentMinutes : 12;
    const factor = best && best.congestionFactor ? best.congestionFactor : 1;
    const congestionLabel = factor >= 1.8 ? "Heavy" : factor >= 1.3 ? "Moderate" : "Light";
    return {
        travelEstimateMinutes: minutes,
        congestionLabel,
        supportingLabel: best ? best.label : "Fallback travel estimate"
    };
}

function deriveWeather(weatherDoc) {
    if (!weatherDoc) {
        return {
            isRaining: false,
            weatherSummary: "Weather data unavailable",
            rainfallSummary: "No rainfall snapshot available",
            temperatureText: "N/A"
        };
    }
    const temperatureText = weatherDoc.temperatureCelsius !== null && weatherDoc.temperatureCelsius !== undefined ? `${weatherDoc.temperatureCelsius} C` : "N/A";
    const rainfallSummary = weatherDoc.isRaining ?
        `Rain detected, peak hourly rainfall ${weatherDoc.maxRainfall || 0} mm.` :
        "No significant rainfall detected.";
    const weatherSummary = weatherDoc.isRaining ? "Wet conditions" : "Dry conditions";
    return {
        isRaining: !!weatherDoc.isRaining,
        weatherSummary,
        rainfallSummary,
        temperatureText
    };
}

function safeRatio(available, total) {
    if (available === null || available === undefined) {
        return 0;
    }
    if (!total || total <= 0) {
        return Math.min(available / 20, 1);
    }
    return Math.max(0, Math.min(available / total, 1));
}

function depletionInfo(current, baseline) {
    const currentAvailable = toNumber(current && current.availableSpaces) || 0;
    const baselineAvailable = toNumber(baseline && baseline.availableSpaces);
    if (baselineAvailable === null || baselineAvailable === undefined) {
        return {
            deltaSpaces: null,
            ratePerMinute: 0,
            minutesToFull: null,
            label: "Insufficient history"
        };
    }
    const consumed = baselineAvailable - currentAvailable;
    const ratePerMinute = consumed / 15;
    if (ratePerMinute > 0.05 && currentAvailable > 0) {
        const minutesToFull = currentAvailable / ratePerMinute;
        const label = minutesToFull <= 10 ? "Critical" : minutesToFull <= 20 ? "Warning" : "Watch";
        return {
            deltaSpaces: consumed,
            ratePerMinute,
            minutesToFull,
            label
        };
    }
    return {
        deltaSpaces: consumed,
        ratePerMinute: Math.max(ratePerMinute, 0),
        minutesToFull: null,
        label: consumed <= 0 ? "Stable or improving" : "Low depletion risk"
    };
}

function scoreCandidate(candidate, baseline, context) {
    const ratio = safeRatio(candidate.availableSpaces, candidate.totalSpaces);
    const depletion = depletionInfo(candidate, baseline);
    let score = ratio * 50;
    score += Math.min(toNumber(candidate.availableSpaces) || 0, 25);
    if (candidate.distanceMeters !== null) {
        score += Math.max(0, 15 - (candidate.distanceMeters / 100));
    }
    if (depletion.minutesToFull !== null) {
        score -= Math.max(0, 25 - Math.min(depletion.minutesToFull, 25));
        if (context.travelEstimateMinutes > depletion.minutesToFull) {
            score -= 35;
        }
    }
    if (context.isRaining) {
        score += candidate.isCovered ? 14 : -20;
    } else if (candidate.sourceType === "meter") {
        score += 6;
    }
    if (context.preference === "sheltered") {
        score += candidate.isCovered ? 10 : -8;
    } else if (context.preference === "budget") {
        score += candidate.sourceType === "meter" ? 8 : 0;
    }
    const arrivalUnsafe = depletion.minutesToFull !== null && context.travelEstimateMinutes > depletion.minutesToFull;
    const riskText = arrivalUnsafe ? "Likely full before arrival" : depletion.label;
    return {
        candidate,
        depletion,
        score,
        arrivalUnsafe,
        ratio,
        riskText
    };
}

function formatMinutes(value) {
    if (value === null || value === undefined || !Number.isFinite(value)) {
        return "N/A";
    }
    return `${Math.max(1, Math.round(value))} min`;
}

function aggregateNearbyAvailability(doc, profile) {
    const rows = candidateFilter(doc && doc.records, profile, "meter");
    return rows.reduce((sum, item) => sum + (toNumber(item.availableSpaces) || 0), 0);
}

function nearestAvailableByDistance(records, profile, sourceType) {
    const rows = candidateFilter(records, profile, sourceType);
    return rows.find((item) => (toNumber(item.availableSpaces) || 0) > 0) || rows[0] || null;
}

function seriesForDay(docs, profile, dateOffsetDays) {
    const now = new Date();
    const start = new Date(now.getFullYear(), now.getMonth(), now.getDate() + dateOffsetDays, 0, 0, 0, 0);
    const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
    return sortDocs(docs)
        .filter((doc) => {
            const ts = parseIso(doc.snapshotTime);
            return ts >= start.getTime() && ts < end.getTime();
        })
        .map((doc) => {
            const rows = candidateFilter(doc.records || [], profile, "meter");
            const total = rows.reduce((sum, item) => sum + (toNumber(item.availableSpaces) || 0), 0);
            return { x: new Date(parseIso(doc.snapshotTime)), y: total };
        });
}

function recentWindowSeries(docs, profile, hours, offsetHours) {
    const end = Date.now() - ((offsetHours || 0) * 60 * 60 * 1000);
    const start = end - (hours * 60 * 60 * 1000);
    return sortDocs(docs)
        .filter((doc) => {
            const ts = parseIso(doc.snapshotTime);
            return ts >= start && ts <= end;
        })
        .map((doc) => ({
            x: new Date(parseIso(doc.snapshotTime)),
            y: aggregateNearbyAvailability(doc, profile)
        }));
}

function ensureRenderableSeries(series, fallbackValue, minutesSpan) {
    if (series.length >= 2) {
        return series;
    }
    if (series.length === 1) {
        const point = series[0];
        const anchor = new Date(point.x);
        return [
            { x: new Date(anchor.getTime() - ((minutesSpan || 30) * 60 * 1000)), y: point.y },
            point
        ];
    }
    const end = new Date();
    const start = new Date(end.getTime() - ((minutesSpan || 30) * 60 * 1000));
    return [
        { x: start, y: fallbackValue || 0 },
        { x: end, y: fallbackValue || 0 }
    ];
}

function weeklyAverageSeries(docs, profile) {
    const buckets = {};
    sortDocs(docs).forEach((doc) => {
        const rows = candidateFilter(doc.records || [], profile, "meter");
        const total = rows.reduce((sum, item) => sum + (toNumber(item.availableSpaces) || 0), 0);
        const date = new Date(parseIso(doc.snapshotTime));
        const hour = date.getHours();
        if (!buckets[hour]) {
            buckets[hour] = { sum: 0, count: 0 };
        }
        buckets[hour].sum += total;
        buckets[hour].count += 1;
    });
    const baseDate = new Date();
    const series = [];
    for (let hour = 0; hour < 24; hour += 1) {
        const bucket = buckets[hour] || { sum: 0, count: 0 };
        const date = new Date(baseDate.getFullYear(), baseDate.getMonth(), baseDate.getDate(), hour, 0, 0, 0);
        series.push({
            x: date,
            y: bucket.count ? Number((bucket.sum / bucket.count).toFixed(2)) : 0
        });
    }
    return series;
}

function hotspotTable(docs, profile, historyDescriptor) {
    const buckets = {};
    sortDocs(docs).forEach((doc) => {
        const rows = candidateFilter(doc.records || [], profile, "meter");
        const totalAvailable = rows.reduce((sum, item) => sum + (toNumber(item.availableSpaces) || 0), 0);
        const totalCapacity = rows.reduce((sum, item) => sum + (toNumber(item.totalSpaces) || 0), 0);
        const date = new Date(parseIso(doc.snapshotTime));
        const hour = date.getHours();
        if (!buckets[hour]) {
            buckets[hour] = { ratioSum: 0, count: 0 };
        }
        buckets[hour].ratioSum += totalCapacity > 0 ? totalAvailable / totalCapacity : totalAvailable / 20;
        buckets[hour].count += 1;
    });
    const rows = Object.keys(buckets).map((hour) => {
        const bucket = buckets[hour];
        const ratio = bucket.count ? bucket.ratioSum / bucket.count : 0;
        return {
            hour: Number(hour),
            ratio
        };
    }).sort((a, b) => a.ratio - b.ratio).slice(0, 3);

    const htmlRows = rows.map((row) => {
        const color = row.ratio < 0.2 ? "#b91c1c" : row.ratio < 0.4 ? "#c2410c" : "#0f766e";
        const label = `${String(row.hour).padStart(2, "0")}:00 - ${String((row.hour + 1) % 24).padStart(2, "0")}:00`;
        const badge = row.ratio < 0.2 ? "Severe" : row.ratio < 0.4 ? "Busy" : "Manageable";
        return `<tr><td>${label}</td><td><span class="sm-badge" style="background:${color};">${badge}</span></td><td style="color:${color};font-weight:700;">${Math.round(row.ratio * 100)}%</td></tr>`;
    }).join("");

    return `<div class="sm-card sm-history-card"><div class="sm-card-header"><div><div class="sm-eyebrow">Historical Heatmap-Style View</div><div class="sm-title">Congestion hotspots</div></div><div class="sm-muted">${historyDescriptor}</div></div><table class="sm-table"><thead><tr><th>Time window</th><th>Severity</th><th>Average availability</th></tr></thead><tbody>${htmlRows || '<tr><td colspan="3">No historical data available yet.</td></tr>'}</tbody></table></div>`;
}

const selectedDestination = global.get("selectedDestinationQuery") || "IFC Mall, Central";
const preference = global.get("parkingPreference") || "budget";
const profile = global.get("selectedDestinationProfile") || getProfiles();

const cutoffTs = Date.now() - (7 * 24 * 60 * 60 * 1000);
const withinWindow = (doc) => parseIso(doc && doc.snapshotTime) >= cutoffTs;
const meterDocs = sortDocs(msg.payload.meterDocs).filter(withinWindow);
const privateDocs = sortDocs(msg.payload.privateDocs).filter(withinWindow);
const trafficDocs = sortDocs(msg.payload.trafficDocs).filter(withinWindow);
const weatherDocs = sortDocs(msg.payload.weatherDocs).filter(withinWindow);

const latestMeterDoc = latestDoc(meterDocs);
const latestPrivateDoc = latestDoc(privateDocs);
const latestTrafficDoc = latestDoc(trafficDocs);
const latestWeatherDoc = latestDoc(weatherDocs);
const latestTimestamp = Math.max(parseIso(latestMeterDoc && latestMeterDoc.snapshotTime), parseIso(latestPrivateDoc && latestPrivateDoc.snapshotTime), parseIso(latestTrafficDoc && latestTrafficDoc.snapshotTime), parseIso(latestWeatherDoc && latestWeatherDoc.snapshotTime));
const baselineTarget = latestTimestamp - (15 * 60 * 1000);
const baselineMeterDoc = docBefore(meterDocs, baselineTarget);
const baselinePrivateDoc = docBefore(privateDocs, baselineTarget);
const meterBaselineMap = buildRecordMap(baselineMeterDoc);
const privateBaselineMap = buildRecordMap(baselinePrivateDoc);

const traffic = deriveTraffic(profile, latestTrafficDoc);
const weather = deriveWeather(latestWeatherDoc);

const meterCandidates = candidateFilter(latestMeterDoc && latestMeterDoc.records, profile, "meter")
    .slice(0, 6)
    .map((item) => scoreCandidate(item, meterBaselineMap[item.facilityId], {
        travelEstimateMinutes: traffic.travelEstimateMinutes,
        isRaining: weather.isRaining,
        preference
    }));

const privateCandidates = candidateFilter(latestPrivateDoc && latestPrivateDoc.records, profile, "private")
    .slice(0, 6)
    .map((item) => scoreCandidate(item, privateBaselineMap[item.facilityId], {
        travelEstimateMinutes: traffic.travelEstimateMinutes,
        isRaining: weather.isRaining,
        preference
    }));

const meterCandidatesSorted = meterCandidates.slice().sort((a, b) => b.score - a.score);
const privateCandidatesSorted = privateCandidates.slice().sort((a, b) => b.score - a.score);
const nearestAvailableRoadsideRaw = nearestAvailableByDistance(latestMeterDoc && latestMeterDoc.records, profile, "meter");
const preferredRoadside = (nearestAvailableRoadsideRaw && meterCandidatesSorted.find((item) => item.candidate.facilityId === nearestAvailableRoadsideRaw.facilityId)) || meterCandidatesSorted.find((item) => (toNumber(item.candidate.availableSpaces) || 0) > 0) || meterCandidatesSorted[0] || null;
const preferredIndoor = privateCandidatesSorted.find((item) => (toNumber(item.candidate.availableSpaces) || 0) > 0) || privateCandidatesSorted[0] || null;
const allCandidates = meterCandidates.concat(privateCandidates)
    .sort((a, b) => {
        const aAvailable = toNumber(a.candidate.availableSpaces) || 0;
        const bAvailable = toNumber(b.candidate.availableSpaces) || 0;
        if ((aAvailable > 0) !== (bAvailable > 0)) {
            return aAvailable > 0 ? -1 : 1;
        }
        return b.score - a.score;
    });
const primary = allCandidates[0] || null;
const backupIndoor = preferredIndoor;
const bestRoadside = preferredRoadside;

const staleThresholdMinutes = 20;
const staleMinutes = latestTimestamp ? Math.round((Date.now() - latestTimestamp) / 60000) : 999;
const staleWarning = staleMinutes > staleThresholdMinutes ? `Cached data in use. Latest snapshot is ${staleMinutes} minutes old.` : "Live data refreshed within the expected polling window.";

const recommendationSentence = primary ?
    `Recommended option: ${primary.candidate.facilityName}. Estimated drive time is ${formatMinutes(traffic.travelEstimateMinutes)}. ${weather.isRaining ? "Rainfall is present, so covered parking is prioritised." : "Dry weather keeps roadside parking competitive."} ${primary.arrivalUnsafe ? "The current option still carries a before-arrival depletion risk." : "Predicted availability remains acceptable on arrival."}` :
    "No parking recommendation can be produced yet because the historical cache is still empty.";

const osmDirectUrl = `https://www.openstreetmap.org/?mlat=${profile.latitude}&mlon=${profile.longitude}#map=17/${profile.latitude}/${profile.longitude}`;
const mapHtml = `<div class="sm-card sm-destination-card"><div class="sm-card-header"><div><div class="sm-eyebrow">Selected destination</div><div class="sm-title">${profile.name}</div></div><a class="sm-map-link" href="${osmDirectUrl}" target="_blank" rel="noopener noreferrer">Open map</a></div><div class="sm-destination-address">${profile.address || profile.query || profile.areaName || ""}</div><div class="sm-destination-meta"><span class="sm-inline-chip">${profile.region || "All Hong Kong"}</span><span class="sm-inline-chip">${profile.areaName || "Hong Kong"}</span><span class="sm-inline-chip">${profile.district || "Unknown district"}</span><span class="sm-inline-chip">${profile.latitude.toFixed(4)}, ${profile.longitude.toFixed(4)}</span></div></div>`;

const primaryType = primary ? (primary.candidate.isCovered ? "Covered backup" : "Roadside meter") : "Pending";
const topHtml = `<div class="sm-hero">
<div class="sm-hero-grid">
<div>
<div class="sm-eyebrow">Smart Mobility & Dynamic Parking Navigation System</div>
<div class="sm-hero-title">${profile.name} parking advisory</div>
<div class="sm-hero-subtitle">${profile.address || profile.query || profile.areaName || ""}</div>
<div class="sm-hero-copy">${recommendationSentence}</div>
</div>
<div class="sm-kpi-stack">
<div class="sm-kpi"><span class="sm-kpi-label">Best option</span><span class="sm-kpi-value">${primary ? primary.candidate.facilityName : "Waiting for history"}</span><span class="sm-kpi-sub">${primaryType}</span></div>
<div class="sm-kpi"><span class="sm-kpi-label">Travel time</span><span class="sm-kpi-value">${formatMinutes(traffic.travelEstimateMinutes)}</span><span class="sm-kpi-sub">${traffic.congestionLabel} traffic</span></div>
</div>
</div>
<div class="sm-chip-row">
<div class="sm-chip"><strong>Weather</strong><span>${weather.weatherSummary} (${weather.temperatureText})</span></div>
<div class="sm-chip"><strong>Rainfall</strong><span>${weather.rainfallSummary}</span></div>
<div class="sm-chip"><strong>System status</strong><span>${staleWarning}</span></div>
</div>
</div>`;

const roadsideFacilityName = bestRoadside ? bestRoadside.candidate.facilityName : "No roadside cluster matched";
const roadsideAvailable = bestRoadside ? (toNumber(bestRoadside.candidate.availableSpaces) || 0) : 0;
const roadsideTotal = bestRoadside ? (toNumber(bestRoadside.candidate.totalSpaces) || 0) : 0;
const roadsideAggregateAvailable = meterCandidates.reduce((sum, item) => sum + (toNumber(item.candidate.availableSpaces) || 0), 0);
const roadsideAggregateTotal = meterCandidates.reduce((sum, item) => sum + (toNumber(item.candidate.totalSpaces) || 0), 0);
const gaugePercent = Math.round(safeRatio(roadsideAggregateAvailable, roadsideAggregateTotal) * 100);
const meterSummary = `<div class="sm-card"><div class="sm-eyebrow">Nearest HK meter with space</div><div class="sm-title">${roadsideFacilityName}</div><div class="sm-muted">${roadsideAvailable} spaces currently available${roadsideTotal ? ` out of ${roadsideTotal}` : ""}. Across the 6 nearest HK meter clusters around ${profile.name}, there are ${roadsideAggregateAvailable} spaces in total.</div></div>`;
const depletionText = `<div class="sm-card"><div class="sm-eyebrow">Depletion countdown</div><div class="sm-title">${bestRoadside && bestRoadside.depletion.minutesToFull !== null ? formatMinutes(bestRoadside.depletion.minutesToFull) : (bestRoadside ? bestRoadside.depletion.label : "Unavailable")}</div><div class="sm-muted">${bestRoadside && bestRoadside.depletion.minutesToFull !== null ? `Predicted time to full occupancy for ${roadsideFacilityName} using the last 15-minute depletion trend.` : `Current trend assessment for ${roadsideFacilityName}.`}</div></div>`;
const riskHtml = `<div class="sm-card" style="background:${bestRoadside && bestRoadside.arrivalUnsafe ? "linear-gradient(135deg,#7f1d1d,#b91c1c)" : "linear-gradient(135deg,#0f766e,#0f766e)"};color:#ffffff;">
<div class="sm-eyebrow" style="color:rgba(255,255,255,0.76);">Roadside risk assessment</div>
<div class="sm-title" style="color:#ffffff;margin-top:4px;">${bestRoadside ? bestRoadside.riskText : "Unavailable"}</div>
<div class="sm-metric-row">
<div class="sm-metric"><span class="sm-metric-label">Arrival</span><span class="sm-metric-value">${formatMinutes(traffic.travelEstimateMinutes)}</span></div>
<div class="sm-metric"><span class="sm-metric-label">Depletion window</span><span class="sm-metric-value">${bestRoadside && bestRoadside.depletion.minutesToFull !== null ? formatMinutes(bestRoadside.depletion.minutesToFull) : "N/A"}</span></div>
</div>
</div>`;

const privateDisplayCandidates = privateCandidatesSorted.filter((item) => (toNumber(item.candidate.availableSpaces) || 0) > 0);
const privateRowsSource = (privateDisplayCandidates.length ? privateDisplayCandidates : privateCandidatesSorted).slice(0, 5);
const backupRows = privateRowsSource.map((item, index) => `
<tr>
<td>${index + 1}</td>
<td>${item.candidate.facilityName}</td>
<td>${item.candidate.distanceMeters !== null ? `${Math.round(item.candidate.distanceMeters)} m` : "Nearby"}</td>
<td>${item.candidate.isCovered ? "Indoor / covered" : "Outdoor / open-air"}</td>
<td><span class="sm-badge" style="background:${item.arrivalUnsafe ? "#b91c1c" : "#0f766e"};">${item.arrivalUnsafe ? "Unsafe" : "Suitable"}</span></td>
</tr>`).join("");
const privateHeaderText = privateDisplayCandidates.length ?
    `${privateDisplayCandidates.length} nearby private car parks currently report available space.` :
    "No positive private counts are currently reported, so the nearest indoor backup options are listed.";
const backupTableHtml = `<div class="sm-card"><div class="sm-card-header"><div><div class="sm-eyebrow">Private Car Parks Near ${profile.name}</div><div class="sm-title">Nearby indoor backup options</div></div><div class="sm-muted">${privateHeaderText}</div></div>
<table class="sm-table">
<thead><tr><th>Rank</th><th>Facility</th><th>Distance</th><th>Type</th><th>Arrival suitability</th></tr></thead>
<tbody>${backupRows || '<tr><td colspan="5">No nearby private car parks were matched for this destination.</td></tr>'}</tbody>
</table></div>`;
const roadsideShortlistHtml = meterCandidates
    .filter((item) => (toNumber(item.candidate.availableSpaces) || 0) > 0)
    .sort((a, b) => (a.candidate.distanceMeters || 999999) - (b.candidate.distanceMeters || 999999))
    .slice(0, 4)
    .map((item, index) => `<tr><td>${index + 1}</td><td>${item.candidate.facilityName}</td><td>${toNumber(item.candidate.availableSpaces) || 0}</td><td>${item.candidate.distanceMeters !== null ? `${Math.round(item.candidate.distanceMeters)} m` : "Nearby"}</td></tr>`).join("");
const backupInsightHtml = `<div class="sm-card"><div class="sm-card-header"><div><div class="sm-eyebrow">HK meter shortlist</div><div class="sm-title">Nearest outdoor public parking options</div></div><div class="sm-muted">${profile.name} is currently anchored on ${profile.address || profile.query}. The table lists the nearest HK meters that still have space.</div></div><table class="sm-table"><thead><tr><th>Rank</th><th>HK meter cluster</th><th>Available</th><th>Distance</th></tr></thead><tbody>${roadsideShortlistHtml || '<tr><td colspan="4">No nearby HK meter with available space was found at this moment.</td></tr>'}</tbody></table></div>`;

const todaySeries = seriesForDay(meterDocs, profile, 0);
const yesterdaySeries = seriesForDay(meterDocs, profile, -1);
const recentSeries = recentWindowSeries(meterDocs, profile, 6, 0);
const previousWindowSeries = recentWindowSeries(meterDocs, profile, 6, 6);
const effectivePrimarySeries = todaySeries.length >= 2 ? todaySeries : recentSeries;
const effectiveComparisonSeries = yesterdaySeries.length >= 2 ? yesterdaySeries : previousWindowSeries;
const primarySeriesLabel = todaySeries.length >= 2 ? "Today" : "Recent 6h";
const comparisonSeriesLabel = yesterdaySeries.length >= 2 ? "Yesterday" : "Previous 6h";

const weeklySeries = weeklyAverageSeries(meterDocs, profile);
const weeklyLabel = meterDocs.length >= 24 ? "7-Day Average" : "Collected History Average";
const historyDescriptor = meterDocs.length >= 24 ?
    `Lowest average availability periods over the last 7 days for ${profile.name}.` :
    `History is still building. Showing the best available hotspot summary from ${meterDocs.length} collected snapshots for ${profile.name}.`;
const fallbackAvailability = aggregateNearbyAvailability(latestMeterDoc || {}, profile);
const renderablePrimarySeries = ensureRenderableSeries(effectivePrimarySeries, fallbackAvailability, 30);
const renderableComparisonSeries = ensureRenderableSeries(effectiveComparisonSeries, fallbackAvailability, 30);
const renderableWeeklySeries = ensureRenderableSeries(weeklySeries.filter((point) => point.y > 0), fallbackAvailability, 60);

const packagePayload = {
    generatedAt: new Date().toISOString(),
    destinationId: profile.id,
    destinationName: profile.name,
    preference,
    travelEstimateMinutes: traffic.travelEstimateMinutes,
    congestionLabel: traffic.congestionLabel,
    weatherState: weather,
    recommendation: {
        primary: primary ? {
            facilityId: primary.candidate.facilityId,
            facilityName: primary.candidate.facilityName,
            sourceType: primary.candidate.sourceType,
            score: Number(primary.score.toFixed(2)),
            rationale: recommendationSentence
        } : null,
        backupIndoor: backupIndoor ? {
            facilityId: backupIndoor.candidate.facilityId,
            facilityName: backupIndoor.candidate.facilityName,
            score: Number(backupIndoor.score.toFixed(2))
        } : null
    },
    widgets: {
        topHtml,
        mapHtml,
        gaugePercent,
        meterSummary,
        depletionText,
        riskHtml,
        backupTableHtml,
        backupInsightHtml,
        hotspotHtml: hotspotTable(meterDocs, profile, historyDescriptor),
        todayVsYesterdayChart: [{
            series: [primarySeriesLabel, comparisonSeriesLabel],
            data: [renderablePrimarySeries, renderableComparisonSeries],
            labels: [""]
        }],
        weeklyPatternChart: [{
            series: [weeklyLabel],
            data: [renderableWeeklySeries],
            labels: [""]
        }]
    }
};

return [
    { payload: packagePayload },
    {
        payload: {
            generatedAt: packagePayload.generatedAt,
            destinationId: packagePayload.destinationId,
            destinationName: packagePayload.destinationName,
            preference: packagePayload.preference,
            travelEstimateMinutes: packagePayload.travelEstimateMinutes,
            congestionLabel: packagePayload.congestionLabel,
            weatherState: packagePayload.weatherState,
            recommendation: packagePayload.recommendation
        }
    }
];
"""


prepare_widget_messages_func = r"""
const widgets = msg.payload.widgets || {};
return [
    { payload: widgets.topHtml || "" },
    { payload: widgets.mapHtml || "" },
    { payload: widgets.gaugePercent || 0 },
    { payload: widgets.meterSummary || "" },
    { payload: widgets.depletionText || "" },
    { payload: widgets.riskHtml || "" },
    { payload: widgets.backupTableHtml || "" },
    { payload: widgets.backupInsightHtml || "" },
    { payload: widgets.todayVsYesterdayChart || [] },
    { payload: widgets.weeklyPatternChart || [] },
    { payload: widgets.hotspotHtml || "" }
];
"""


nodes = [
    node(setup_tab, "tab", label="0. Setup", disabled=False, info="Initialise destination presets and dashboard defaults."),
    node(ingest_tab, "tab", label="1. Ingestion", disabled=False, info="Fetch, normalise, and store smart mobility datasets."),
    node(analytics_tab, "tab", label="2. Analytics", disabled=False, info="Read MongoDB history and compute parking recommendations."),
    node(dashboard_tab, "tab", label="3. Dashboard", disabled=False, info="Dashboard control nodes and visualisation widgets."),
    node(mongo_cfg, "mongodb3", uri="mongodb://localhost:27017", name="Smart Mobility MongoDB", options="", parallelism="-1"),
    node(
        ui_base,
        "ui_base",
        theme={
            "name": "theme-light",
            "lightTheme": {
                "default": "#0f4c81",
                "baseColor": "#0f4c81",
                "baseFont": "Georgia,Times New Roman,serif",
                "edited": True,
                "reset": False,
            },
            "darkTheme": {
                "default": "#1d3557",
                "baseColor": "#1d3557",
                "baseFont": "Georgia,Times New Roman,serif",
                "edited": True,
                "reset": False,
            },
            "customTheme": {
                "name": "Smart Mobility",
                "default": "#0f4c81",
                "baseColor": "#0f4c81",
                "baseFont": "Georgia,Times New Roman,serif",
                "reset": False,
            },
            "themeState": {
                "base-color": {"default": "#0f4c81", "value": "#0f4c81", "edited": True},
                "page-titlebar-backgroundColor": {"value": "#f3f4f6", "edited": True},
                "page-backgroundColor": {"value": "#f3f4f6", "edited": True},
                "page-sidebar-backgroundColor": {"value": "#e5e7eb", "edited": True},
                "group-textColor": {"value": "#1e3a5f", "edited": True},
                "group-borderColor": {"value": "#cbd5e1", "edited": True},
                "group-backgroundColor": {"value": "#ffffff", "edited": True},
                "widget-textColor": {"value": "#0f172a", "edited": True},
                "widget-backgroundColor": {"value": "#ffffff", "edited": True},
                "widget-borderColor": {"value": "#d1d5db", "edited": True},
                "base-font": {"value": "Georgia,Times New Roman,serif"},
            },
            "angularTheme": {"primary": "blue", "accents": "cyan", "warn": "red", "background": "grey"},
        },
        site={
            "name": "Smart Mobility & Dynamic Parking Navigation System",
            "hideToolbar": "false",
            "allowSwipe": "false",
            "lockMenu": "true",
            "allowTempTheme": "false",
            "dateFormat": "DD/MM/YYYY",
            "sizes": {"sx": 48, "sy": 48, "gx": 6, "gy": 6, "cx": 6, "cy": 6, "px": 0, "py": 0},
        },
    ),
    node(ui_tab, "ui_tab", name="Smart Mobility Dashboard", icon="dashboard", disabled=False, hidden=False),
    node(group_controls, "ui_group", name="Controls", tab=ui_tab, disp=True, width="24", collapse=False),
    node(group_alerts, "ui_group", name="Top Panel", tab=ui_tab, disp=True, width="24", collapse=False),
    node(group_meter, "ui_group", name="Roadside Prediction", tab=ui_tab, disp=True, width="8", collapse=False),
    node(group_private, "ui_group", name="Private Backup", tab=ui_tab, disp=True, width="16", collapse=False),
    node(group_history, "ui_group", name="Historical Trends", tab=ui_tab, disp=True, width="24", collapse=False),
    node("ui_theme_template", "ui_template", dashboard_tab, group=group_controls, name="Dashboard Theme CSS", order=0, width=0, height=0, format="""<style>
.nr-dashboard-theme md-content,
.nr-dashboard-theme .nr-dashboard-cardcontainer {
  background: linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%) !important;
}
.nr-dashboard-theme .md-toolbar-tools,
.nr-dashboard-theme md-toolbar {
  background: linear-gradient(90deg, #f8fafc 0%, #eef4fb 100%) !important;
  color: #143a63 !important;
}
.nr-dashboard-theme md-toolbar .md-toolbar-tools,
.nr-dashboard-theme md-toolbar .md-toolbar-tools * {
  color: #143a63 !important;
  font-weight: 700 !important;
}
.nr-dashboard-theme .md-toolbar-tools h1,
.nr-dashboard-theme .md-toolbar-tools .md-headline {
  color: #143a63 !important;
}
.sm-hero {
  padding: 18px 20px;
  border-radius: 18px;
  color: #f8fafc;
  background:
    radial-gradient(circle at top right, rgba(253, 186, 116, 0.35), transparent 32%),
    linear-gradient(135deg, #0f172a 0%, #0f4c81 55%, #0f766e 100%);
  box-shadow: 0 18px 38px rgba(15, 23, 42, 0.28);
}
.sm-hero-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.6fr) minmax(220px, 0.9fr);
  gap: 18px;
  align-items: start;
}
.sm-eyebrow {
  font-size: 12px;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: rgba(15, 23, 42, 0.54);
  font-weight: 700;
}
.sm-hero .sm-eyebrow {
  color: rgba(248, 250, 252, 0.78);
}
.sm-hero-title {
  margin-top: 8px;
  font-size: 24px;
  line-height: 1.1;
  font-weight: 700;
}
.sm-hero-copy {
  margin-top: 10px;
  max-width: 760px;
  font-size: 15px;
  line-height: 1.5;
  color: rgba(248, 250, 252, 0.95);
}
.sm-hero-subtitle {
  margin-top: 8px;
  font-size: 13px;
  color: rgba(248, 250, 252, 0.82);
}
.sm-kpi-stack {
  display: grid;
  gap: 10px;
}
.sm-kpi {
  border-radius: 14px;
  padding: 12px 14px;
  background: rgba(255, 255, 255, 0.13);
  border: 1px solid rgba(255, 255, 255, 0.14);
  backdrop-filter: blur(6px);
}
.sm-kpi-label,
.sm-kpi-sub {
  display: block;
}
.sm-kpi-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.12em;
  color: rgba(248, 250, 252, 0.74);
}
.sm-kpi-value {
  display: block;
  margin-top: 6px;
  font-size: 18px;
  font-weight: 700;
}
.sm-kpi-sub {
  margin-top: 4px;
  font-size: 12px;
  color: rgba(248, 250, 252, 0.85);
}
.sm-chip-row {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  margin-top: 14px;
}
.sm-chip {
  min-width: 180px;
  padding: 8px 10px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.16);
  border: 1px solid rgba(255, 255, 255, 0.12);
}
.sm-chip strong,
.sm-chip span {
  display: block;
}
.sm-chip strong {
  font-size: 11px;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: rgba(248, 250, 252, 0.74);
}
.sm-chip span {
  margin-top: 4px;
  font-size: 12px;
  line-height: 1.4;
}
.sm-card {
  padding: 12px 14px;
  border-radius: 16px;
  background: #ffffff;
  border: 1px solid #dbe4ee;
  box-shadow: 0 10px 24px rgba(15, 23, 42, 0.08);
}
.nr-dashboard-theme .nr-dashboard-cardtitle {
  padding: 12px 16px 6px !important;
  font-size: 13px !important;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #64748b !important;
  font-weight: 700 !important;
}
.nr-dashboard-theme md-card {
  border-radius: 18px !important;
  box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08) !important;
  border: 1px solid #dbe4ee !important;
}
.nr-dashboard-theme .nr-dashboard-template,
.nr-dashboard-theme .nr-dashboard-text,
.nr-dashboard-theme .nr-dashboard-gauge,
.nr-dashboard-theme .nr-dashboard-chart {
  background: transparent !important;
}
.nr-dashboard-theme .md-button {
  border-radius: 14px !important;
}
.nr-dashboard-theme md-input-container,
.nr-dashboard-theme md-select,
.nr-dashboard-theme md-switch {
  background: #ffffff !important;
  border-radius: 14px !important;
}
.nr-dashboard-theme .nr-dashboard-dropdown,
.nr-dashboard-theme .nr-dashboard-switch,
.nr-dashboard-theme .nr-dashboard-button {
  padding-top: 2px !important;
  background: transparent !important;
}
.nr-dashboard-theme .nr-dashboard-dropdown,
.nr-dashboard-theme .nr-dashboard-switch,
.nr-dashboard-theme .nr-dashboard-button,
.nr-dashboard-theme .nr-dashboard-textinput {
  background: transparent !important;
}
.nr-dashboard-theme .nr-dashboard-textinput md-input-container,
.nr-dashboard-theme .nr-dashboard-dropdown md-input-container,
.nr-dashboard-theme .nr-dashboard-switch div,
.nr-dashboard-theme .nr-dashboard-button button {
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08) !important;
}
.nr-dashboard-theme .nr-dashboard-textinput md-input-container,
.nr-dashboard-theme .nr-dashboard-dropdown md-input-container {
  margin: 0 !important;
  padding: 8px 12px 2px !important;
  background: #ffffff !important;
  border: 1px solid #d8e2ee !important;
  border-radius: 18px !important;
}
.nr-dashboard-theme .nr-dashboard-switch {
  border-radius: 18px !important;
  padding: 10px 14px !important;
  background: #ffffff !important;
  border: 1px solid #d8e2ee !important;
  box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08) !important;
}
.nr-dashboard-theme .nr-dashboard-button button {
  min-height: 54px !important;
}
.sm-card-header {
  display: flex;
  gap: 12px;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 10px;
}
.sm-title {
  font-size: 18px;
  color: #0f172a;
  font-weight: 700;
}
.sm-muted {
  font-size: 12px;
  line-height: 1.4;
  color: #475569;
  max-width: 280px;
}
.sm-metric-row {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-top: 12px;
}
.sm-metric {
  padding: 10px 12px;
  border-radius: 12px;
  background: rgba(255, 255, 255, 0.12);
}
.sm-metric-label,
.sm-metric-value {
  display: block;
}
.sm-metric-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.1em;
  opacity: 0.8;
}
.sm-metric-value {
  margin-top: 4px;
  font-size: 18px;
  font-weight: 700;
}
.sm-table {
  width: 100%;
  border-collapse: collapse;
}
.sm-table th,
.sm-table td {
  padding: 8px 10px;
  text-align: left;
  border-bottom: 1px solid #e2e8f0;
  font-size: 13px;
}
.sm-table th {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #64748b;
}
.sm-table tr:last-child td {
  border-bottom: none;
}
.sm-table tbody tr td {
  vertical-align: top;
}
.sm-badge {
  display: inline-block;
  padding: 5px 10px;
  border-radius: 999px;
  color: #ffffff;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.sm-history-card .sm-muted {
  text-align: right;
}
.sm-destination-card {
  padding: 14px 16px;
}
.sm-destination-address {
  font-size: 14px;
  line-height: 1.45;
  color: #334155;
}
.sm-destination-meta {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 10px;
}
.sm-inline-chip {
  display: inline-flex;
  align-items: center;
  padding: 6px 10px;
  border-radius: 999px;
  background: #eaf1f8;
  color: #0f4c81;
  font-size: 11px;
  font-weight: 700;
}
.sm-map-link {
  color: #0f4c81;
  font-weight: 700;
  text-decoration: none;
  white-space: nowrap;
}
.sm-map-link:hover {
  text-decoration: underline;
}
.nr-dashboard-theme .nr-dashboard-chart {
  min-height: 300px !important;
}
.nr-dashboard-theme .nr-dashboard-template {
  min-height: auto !important;
}
.sm-chart-note {
  margin-top: 10px;
  font-size: 13px;
  color: #64748b;
}
@media (max-width: 900px) {
  .sm-hero-grid {
    grid-template-columns: 1fr;
  }
  .sm-card-header {
    flex-direction: column;
  }
  .sm-history-card .sm-muted {
    text-align: left;
  }
}
</style>""", storeOutMessages=False, fwdInMessages=False, resendOnRefresh=True, templateScope="global", className="", x=180, y=20, wires=[[]]),
    node("inj_load_profiles", "inject", setup_tab, name="Initialise Presets", topic="", payload="", payloadType="date", repeat="", crontab="", once=True, onceDelay="0.5", x=170, y=80, wires=[["fn_load_profiles"]]),
    node("fn_load_profiles", "function", setup_tab, name="Load Destination Profiles", func=load_profiles_func, outputs=1, noerr=0, x=390, y=80, wires=[["mongo_seed_profiles"]]),
    node("mongo_seed_profiles", "mongodb3 in", setup_tab, service="_ext_", configNode=mongo_cfg, name="Seed destination_profiles", collection="destination_profiles", operation="insert", x=650, y=80, wires=[["dbg_seed_profiles"]]),
    node("dbg_seed_profiles", "debug", setup_tab, name="Seed Result", active=False, tosidebar=True, console=False, tostatus=False, complete="payload", x=900, y=80, wires=[]),
    node("inj_prime_dashboard", "inject", dashboard_tab, name="Prime Dashboard Defaults", topic="", payload="", payloadType="date", repeat="", crontab="", once=True, onceDelay="1", x=180, y=60, wires=[["fn_prime_dashboard"]]),
    node("fn_prime_dashboard", "function", dashboard_tab, name="Prime Dashboard State", func=prime_dashboard_state_func, outputs=4, noerr=0, x=400, y=60, wires=[["ui_destination"], ["ui_search_result_selector"], ["ui_preference"], ["fn_build_geocode_request"]]),
    node("ui_destination", "ui_text_input", dashboard_tab, name="Destination Input", label="Destination or building", tooltip="", group=group_controls, order=1, width=12, height=1, passthru=True, mode="text", delay="0", topic="destination", sendOnBlur=True, className="", x=170, y=140, wires=[["fn_store_destination"]]),
    node("ui_search_result_selector", "ui_dropdown", dashboard_tab, name="Matched Locations", label="Matched location", tooltip="", place="Choose the matched result", group=group_controls, order=2, width=12, height=1, passthru=True, multiple=False, options=[], payload="", topic="selectedResultId", topicType="str", className="", x=175, y=180, wires=[["fn_apply_selected_result"]]),
    node("fn_store_destination", "function", dashboard_tab, name="Store Destination Query", func=store_destination_func, outputs=1, noerr=0, x=430, y=140, wires=[["fn_build_geocode_request"]]),
    node("fn_apply_selected_result", "function", dashboard_tab, name="Apply Selected Location", func=apply_selected_result_func, outputs=1, noerr=0, x=435, y=180, wires=[["lnk_run_analytics_out"]]),
    node("ui_preference", "ui_switch", dashboard_tab, name="Parking Preference", label="Prefer sheltered indoor parking", tooltip="", group=group_controls, order=3, width=7, height=1, passthru=True, decouple="false", topic="preference", style="", onvalue="sheltered", onvalueType="str", onicon="", oncolor="", offvalue="budget", offvalueType="str", officon="", offcolor="", animate=False, className="", x=170, y=230, wires=[["fn_store_preference"]]),
    node("fn_store_preference", "function", dashboard_tab, name="Store Preference", func=store_preference_func, outputs=1, noerr=0, x=420, y=230, wires=[["lnk_run_analytics_out"]]),
    node("ui_search_button", "ui_button", dashboard_tab, name="Find Matches", group=group_controls, order=4, width=5, height=1, passthru=False, label="Find matches", tooltip="", color="", bgcolor="#0f4c81", icon="search", payload="refresh", payloadType="str", topic="refresh", x=150, y=280, wires=[["fn_build_refresh"]]),
    node("ui_refresh_button", "ui_button", dashboard_tab, name="Refresh Dashboard", group=group_controls, order=5, width=5, height=1, passthru=False, label="Refresh analysis", tooltip="", color="", bgcolor="#0f4c81", icon="refresh", payload="refresh", payloadType="str", topic="refresh", x=150, y=330, wires=[["fn_build_refresh"]]),
    node("fn_build_refresh", "function", dashboard_tab, name="Build Refresh Trigger", func=build_refresh_message_func, outputs=1, noerr=0, x=410, y=305, wires=[["fn_build_geocode_request"]]),
    node("inj_dashboard_timer", "inject", dashboard_tab, name="Periodic Dashboard Refresh", topic="", payload="", payloadType="date", repeat="300", crontab="", once=False, onceDelay="", x=170, y=380, wires=[["fn_build_refresh_timer"]]),
    node("fn_build_refresh_timer", "function", dashboard_tab, name="Timer Refresh Trigger", func=build_refresh_message_func, outputs=1, noerr=0, x=420, y=380, wires=[["fn_build_geocode_request"]]),
    node("fn_build_geocode_request", "function", dashboard_tab, name="Build Geocode Request", func=build_geocode_request_func, outputs=1, noerr=0, x=670, y=250, wires=[["http_destination_geocode"]]),
    node("http_destination_geocode", "http request", dashboard_tab, name="Geocode destination", method="GET", ret="txt", paytoqs=False, url="", tls="", persist=False, proxy="", authType="", senderr=False, x=900, y=250, wires=[["json_destination_geocode"]]),
    node("json_destination_geocode", "json", dashboard_tab, name="Parse geocode JSON", property="payload", action="", pretty=False, x=1115, y=250, wires=[["fn_parse_geocode_result"]]),
    node("fn_parse_geocode_result", "function", dashboard_tab, name="Resolve User Destination", func=parse_geocode_result_func, outputs=2, noerr=0, x=1350, y=250, wires=[["ui_search_result_selector"], ["fn_apply_selected_result"]]),
    node("lnk_run_analytics_out", "link out", dashboard_tab, name="Run Analytics", mode="link", links=["lnk_run_analytics_in"], x=655, y=220, wires=[]),
    node("lnk_dashboard_package_in", "link in", dashboard_tab, name="Dashboard Package In", links=["lnk_dashboard_package_out"], x=145, y=420, wires=[["fn_prepare_widgets"]]),
    node("fn_prepare_widgets", "function", dashboard_tab, name="Prepare Widget Messages", func=prepare_widget_messages_func, outputs=11, noerr=0, x=380, y=420, wires=[["ui_top_template"], ["ui_map_template"], ["ui_meter_gauge"], ["ui_meter_summary"], ["ui_meter_depletion"], ["ui_risk_template"], ["ui_backup_template"], ["ui_backup_insight_template"], ["ui_chart_today_vs_yesterday"], ["ui_chart_weekly_pattern"], ["ui_hotspot_template"]]),
    node("ui_map_template", "ui_template", dashboard_tab, group=group_controls, name="Destination Map Card", order=4, width=24, height=3, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=680, y=340, wires=[[]]),
    node("ui_top_template", "ui_template", dashboard_tab, group=group_alerts, name="Top Advisory Card", order=1, width=24, height=5, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=680, y=380, wires=[[]]),
    node("ui_meter_gauge", "ui_gauge", dashboard_tab, name="Roadside Availability Gauge", group=group_meter, order=1, width=8, height=3, gtype="gage", title="Nearby Roadside Availability", label="%", format="{{value}}", min=0, max="100", colors=["#b91c1c", "#d97706", "#15803d"], seg1="35", seg2="70", className="", x=700, y=420, wires=[]),
    node("ui_meter_summary", "ui_template", dashboard_tab, group=group_meter, name="Roadside Summary Card", order=2, width=8, height=3, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=690, y=460, wires=[[]]),
    node("ui_meter_depletion", "ui_template", dashboard_tab, group=group_meter, name="Depletion Countdown Card", order=3, width=8, height=3, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=695, y=500, wires=[[]]),
    node("ui_risk_template", "ui_template", dashboard_tab, group=group_meter, name="Roadside Risk Badge", order=4, width=8, height=3, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=680, y=540, wires=[[]]),
    node("ui_backup_template", "ui_template", dashboard_tab, group=group_private, name="Private Backup Table", order=1, width=16, height=6, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=685, y=580, wires=[[]]),
    node("ui_backup_insight_template", "ui_template", dashboard_tab, group=group_private, name="Backup Insight Card", order=2, width=16, height=4, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=690, y=620, wires=[[]]),
    node("ui_chart_today_vs_yesterday", "ui_chart", dashboard_tab, name="Today vs Yesterday", group=group_history, order=1, width=12, height=4, label="Today vs Yesterday Nearby Availability", chartType="line", legend="true", xformat="HH:mm", interpolate="linear", nodata="Recent history is still being collected. The chart will switch to richer comparisons as more snapshots accumulate.", dot=True, ymin="0", ymax="", removeOlder="24", removeOlderPoints="", removeOlderUnit="3600", cutout=0, useOneColor=False, colors=["#1d4ed8", "#94a3b8", "#f59e0b", "#16a34a"], useOldStyle=False, outputs=1, x=700, y=660, wires=[[]]),
    node("ui_chart_weekly_pattern", "ui_chart", dashboard_tab, name="Weekly Average Pattern", group=group_history, order=2, width=12, height=4, label="Collected History Pattern", chartType="line", legend="true", xformat="HH:mm", interpolate="linear", nodata="The history panel will strengthen automatically as the Docker flow collects more days of snapshots.", dot=True, ymin="0", ymax="", removeOlder="24", removeOlderPoints="", removeOlderUnit="3600", cutout=0, useOneColor=False, colors=["#0f766e", "#9333ea", "#dc2626", "#f97316"], useOldStyle=False, outputs=1, x=690, y=700, wires=[[]]),
    node("ui_hotspot_template", "ui_template", dashboard_tab, group=group_history, name="Historical Hotspots", order=3, width=24, height=4, format='<div ng-bind-html="msg.payload"></div>', storeOutMessages=True, fwdInMessages=False, resendOnRefresh=True, templateScope="local", className="", x=690, y=700, wires=[[]]),
    node("lnk_run_analytics_in", "link in", analytics_tab, name="Analytics Trigger In", links=["lnk_run_analytics_out"], x=140, y=80, wires=[["fn_build_meter_query", "fn_build_private_query", "fn_build_traffic_query", "fn_build_weather_query"]]),
    node("fn_build_meter_query", "function", analytics_tab, name="Build Meter Query", func=build_range_query_func, outputs=1, noerr=0, x=360, y=40, wires=[["mongo_get_meter_docs"]]),
    node("fn_build_private_query", "function", analytics_tab, name="Build Private Query", func=build_range_query_func, outputs=1, noerr=0, x=360, y=90, wires=[["mongo_get_private_docs"]]),
    node("fn_build_traffic_query", "function", analytics_tab, name="Build Traffic Query", func=build_range_query_func, outputs=1, noerr=0, x=360, y=140, wires=[["mongo_get_traffic_docs"]]),
    node("fn_build_weather_query", "function", analytics_tab, name="Build Weather Query", func=build_range_query_func, outputs=1, noerr=0, x=360, y=190, wires=[["mongo_get_weather_docs"]]),
    node("mongo_get_meter_docs", "mongodb3 in", analytics_tab, service="_ext_", configNode=mongo_cfg, name="Load parking_meter_snapshots", collection="parking_meter_snapshots", operation="find.toArray", x=650, y=40, wires=[["chg_meter_topic"]]),
    node("mongo_get_private_docs", "mongodb3 in", analytics_tab, service="_ext_", configNode=mongo_cfg, name="Load private_carpark_snapshots", collection="private_carpark_snapshots", operation="find.toArray", x=660, y=90, wires=[["chg_private_topic"]]),
    node("mongo_get_traffic_docs", "mongodb3 in", analytics_tab, service="_ext_", configNode=mongo_cfg, name="Load traffic_snapshots", collection="traffic_snapshots", operation="find.toArray", x=640, y=140, wires=[["chg_traffic_topic"]]),
    node("mongo_get_weather_docs", "mongodb3 in", analytics_tab, service="_ext_", configNode=mongo_cfg, name="Load weather_snapshots", collection="weather_snapshots", operation="find.toArray", x=645, y=190, wires=[["chg_weather_topic"]]),
    node("chg_meter_topic", "change", analytics_tab, name="Topic meterDocs", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "meterDocs", "tot": "str"}], x=900, y=40, wires=[["join_analytics_inputs"]]),
    node("chg_private_topic", "change", analytics_tab, name="Topic privateDocs", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "privateDocs", "tot": "str"}], x=900, y=90, wires=[["join_analytics_inputs"]]),
    node("chg_traffic_topic", "change", analytics_tab, name="Topic trafficDocs", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "trafficDocs", "tot": "str"}], x=900, y=140, wires=[["join_analytics_inputs"]]),
    node("chg_weather_topic", "change", analytics_tab, name="Topic weatherDocs", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "weatherDocs", "tot": "str"}], x=900, y=190, wires=[["join_analytics_inputs"]]),
    node("join_analytics_inputs", "join", analytics_tab, name="Collect analytics inputs", mode="custom", build="object", property="payload", propertyType="msg", key="topic", joiner="\\n", joinerType="str", useparts=False, accumulate=False, timeout="3", count="4", reduceRight=False, x=1180, y=120, wires=[["fn_compute_package"]]),
    node("fn_compute_package", "function", analytics_tab, name="Compute Recommendation Package", func=compute_analytics_package_func, outputs=2, noerr=0, x=1450, y=120, wires=[["lnk_dashboard_package_out"], ["mongo_recommendation_log"]]),
    node("lnk_dashboard_package_out", "link out", analytics_tab, name="Dashboard Package Out", mode="link", links=["lnk_dashboard_package_in"], x=1715, y=100, wires=[]),
    node("mongo_recommendation_log", "mongodb3 in", analytics_tab, service="_ext_", configNode=mongo_cfg, name="Store recommendation_logs", collection="recommendation_logs", operation="insert", x=1720, y=160, wires=[["dbg_recommendation_log"]]),
    node("dbg_recommendation_log", "debug", analytics_tab, name="Recommendation Log", active=False, tosidebar=True, console=False, tostatus=False, complete="payload", x=1950, y=160, wires=[]),
    node("inj_ingestion_timer", "inject", ingest_tab, name="Poll all data sources every 5 minutes", topic="", payload="", payloadType="date", repeat="300", crontab="", once=True, onceDelay="1", x=220, y=60, wires=[["http_meter_info", "http_meter_occupancy", "http_private_info", "http_private_vacancy", "http_traffic", "http_current_weather", "http_hourly_rain"]]),
    node("http_meter_info", "http request", ingest_tab, name="Meter distribution CSV", method="GET", ret="txt", paytoqs=False, url="https://resource.data.one.gov.hk/td/psiparkingspaces/spaceinfo/parkingspaces.csv", tls="", persist=False, proxy="", authType="", senderr=False, x=220, y=140, wires=[["fn_sanitize_meter_info_csv"]]),
    node("fn_sanitize_meter_info_csv", "function", ingest_tab, name="Sanitise meter info CSV", func=sanitize_meter_info_csv_func, outputs=1, noerr=0, x=490, y=140, wires=[["csv_meter_info"]]),
    node("csv_meter_info", "csv", ingest_tab, name="Parse meter distribution CSV", sep=",", hdrin=True, hdrout="none", multi="mult", ret="\\n", temp="", skip="0", strings=True, include_empty_strings="", include_null_values="", x=760, y=140, wires=[["chg_meter_info_topic"]]),
    node("chg_meter_info_topic", "change", ingest_tab, name="Topic meterInfo", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "meterInfo", "tot": "str"}], x=1010, y=140, wires=[["join_meter_sources"]]),
    node("http_meter_occupancy", "http request", ingest_tab, name="Meter occupancy CSV", method="GET", ret="txt", paytoqs=False, url="https://resource.data.one.gov.hk/td/psiparkingspaces/occupancystatus/occupancystatus.csv", tls="", persist=False, proxy="", authType="", senderr=False, x=210, y=200, wires=[["csv_meter_occupancy"]]),
    node("csv_meter_occupancy", "csv", ingest_tab, name="Parse meter occupancy CSV", sep=",", hdrin=True, hdrout="none", multi="mult", ret="\\n", temp="", skip="0", strings=True, include_empty_strings="", include_null_values="", x=760, y=200, wires=[["chg_meter_occupancy_topic"]]),
    node("chg_meter_occupancy_topic", "change", ingest_tab, name="Topic meterOccupancy", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "meterOccupancy", "tot": "str"}], x=1010, y=200, wires=[["join_meter_sources"]]),
    node("join_meter_sources", "join", ingest_tab, name="Join meter inputs", mode="custom", build="object", property="payload", propertyType="msg", key="topic", joiner="\\n", joinerType="str", useparts=False, accumulate=False, timeout="3", count="2", reduceRight=False, x=1260, y=170, wires=[["fn_normalize_meter"]]),
    node("fn_normalize_meter", "function", ingest_tab, name="Normalise meter snapshot", func=normalize_meter_snapshot_func, outputs=1, noerr=0, x=1490, y=170, wires=[["mongo_store_meter"]]),
    node("mongo_store_meter", "mongodb3 in", ingest_tab, service="_ext_", configNode=mongo_cfg, name="Store parking_meter_snapshots", collection="parking_meter_snapshots", operation="insert", x=1720, y=170, wires=[["dbg_store_meter"]]),
    node("dbg_store_meter", "debug", ingest_tab, name="Meter snapshot stored", active=False, tosidebar=True, console=False, tostatus=False, complete="payload.recordCount", x=1950, y=170, wires=[]),
    node("http_private_info", "http request", ingest_tab, name="Private car park info", method="GET", ret="txt", paytoqs=False, url="https://api.data.gov.hk/v1/carpark-info-vacancy?data=info&vehicleTypes=privateCar&lang=en", tls="", persist=False, proxy="", authType="", senderr=False, x=200, y=300, wires=[["json_private_info"]]),
    node("json_private_info", "json", ingest_tab, name="Parse private info JSON", property="payload", action="", pretty=False, x=500, y=300, wires=[["chg_private_info_topic"]]),
    node("chg_private_info_topic", "change", ingest_tab, name="Topic privateInfo", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "privateInfo", "tot": "str"}], x=760, y=300, wires=[["join_private_sources"]]),
    node("http_private_vacancy", "http request", ingest_tab, name="Private car park vacancy", method="GET", ret="txt", paytoqs=False, url="https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=en", tls="", persist=False, proxy="", authType="", senderr=False, x=210, y=360, wires=[["json_private_vacancy"]]),
    node("json_private_vacancy", "json", ingest_tab, name="Parse private vacancy JSON", property="payload", action="", pretty=False, x=510, y=360, wires=[["chg_private_vacancy_topic"]]),
    node("chg_private_vacancy_topic", "change", ingest_tab, name="Topic privateVacancy", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "privateVacancy", "tot": "str"}], x=780, y=360, wires=[["join_private_sources"]]),
    node("join_private_sources", "join", ingest_tab, name="Join private inputs", mode="custom", build="object", property="payload", propertyType="msg", key="topic", joiner="\\n", joinerType="str", useparts=False, accumulate=False, timeout="3", count="2", reduceRight=False, x=1030, y=330, wires=[["fn_normalize_private"]]),
    node("fn_normalize_private", "function", ingest_tab, name="Normalise private snapshot", func=normalize_private_snapshot_func, outputs=1, noerr=0, x=1265, y=330, wires=[["mongo_store_private"]]),
    node("mongo_store_private", "mongodb3 in", ingest_tab, service="_ext_", configNode=mongo_cfg, name="Store private_carpark_snapshots", collection="private_carpark_snapshots", operation="insert", x=1505, y=330, wires=[["dbg_store_private"]]),
    node("dbg_store_private", "debug", ingest_tab, name="Private snapshot stored", active=False, tosidebar=True, console=False, tostatus=False, complete="payload.recordCount", x=1740, y=330, wires=[]),
    node("http_traffic", "http request", ingest_tab, name="Journey time indicators XML", method="GET", ret="txt", paytoqs=False, url="https://resource.data.one.gov.hk/td/jss/Journeytimev2.xml", tls="", persist=False, proxy="", authType="", senderr=False, x=220, y=470, wires=[["xml_traffic"]]),
    node("xml_traffic", "xml", ingest_tab, name="Parse traffic XML", property="payload", attr="", chr="", x=490, y=470, wires=[["fn_normalize_traffic"]]),
    node("fn_normalize_traffic", "function", ingest_tab, name="Normalise traffic snapshot", func=normalize_traffic_snapshot_func, outputs=1, noerr=0, x=735, y=470, wires=[["mongo_store_traffic"]]),
    node("mongo_store_traffic", "mongodb3 in", ingest_tab, service="_ext_", configNode=mongo_cfg, name="Store traffic_snapshots", collection="traffic_snapshots", operation="insert", x=980, y=470, wires=[["dbg_store_traffic"]]),
    node("dbg_store_traffic", "debug", ingest_tab, name="Traffic snapshot stored", active=False, tosidebar=True, console=False, tostatus=False, complete="payload.recordCount", x=1210, y=470, wires=[]),
    node("http_current_weather", "http request", ingest_tab, name="Current weather report", method="GET", ret="txt", paytoqs=False, url="https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread&lang=en", tls="", persist=False, proxy="", authType="", senderr=False, x=215, y=580, wires=[["json_current_weather"]]),
    node("json_current_weather", "json", ingest_tab, name="Parse current weather JSON", property="payload", action="", pretty=False, x=500, y=580, wires=[["chg_current_weather_topic"]]),
    node("chg_current_weather_topic", "change", ingest_tab, name="Topic currentWeather", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "currentWeather", "tot": "str"}], x=780, y=580, wires=[["join_weather_sources"]]),
    node("http_hourly_rain", "http request", ingest_tab, name="Hourly rainfall report", method="GET", ret="txt", paytoqs=False, url="https://data.weather.gov.hk/weatherAPI/opendata/hourlyRainfall.php?lang=en", tls="", persist=False, proxy="", authType="", senderr=False, x=205, y=640, wires=[["json_hourly_rain"]]),
    node("json_hourly_rain", "json", ingest_tab, name="Parse rainfall JSON", property="payload", action="", pretty=False, x=490, y=640, wires=[["chg_hourly_rain_topic"]]),
    node("chg_hourly_rain_topic", "change", ingest_tab, name="Topic hourlyRain", rules=[{"t": "set", "p": "topic", "pt": "msg", "to": "hourlyRain", "tot": "str"}], x=765, y=640, wires=[["join_weather_sources"]]),
    node("join_weather_sources", "join", ingest_tab, name="Join weather inputs", mode="custom", build="object", property="payload", propertyType="msg", key="topic", joiner="\\n", joinerType="str", useparts=False, accumulate=False, timeout="3", count="2", reduceRight=False, x=1015, y=610, wires=[["fn_normalize_weather"]]),
    node("fn_normalize_weather", "function", ingest_tab, name="Normalise weather snapshot", func=normalize_weather_snapshot_func, outputs=1, noerr=0, x=1250, y=610, wires=[["mongo_store_weather"]]),
    node("mongo_store_weather", "mongodb3 in", ingest_tab, service="_ext_", configNode=mongo_cfg, name="Store weather_snapshots", collection="weather_snapshots", operation="insert", x=1485, y=610, wires=[["dbg_store_weather"]]),
    node("dbg_store_weather", "debug", ingest_tab, name="Weather snapshot stored", active=False, tosidebar=True, console=False, tostatus=False, complete="payload", x=1715, y=610, wires=[]),
]

flow_json = json.dumps(nodes, indent=4) + "\n"
OUT_FILE.write_text(flow_json, encoding="utf-8")

docker_nodes = copy.deepcopy(nodes)
for item in docker_nodes:
    if item.get("id") == mongo_cfg:
        item["uri"] = "mongodb://mongo:27017"
docker_flow_json = json.dumps(docker_nodes, indent=4) + "\n"
DOCKER_FLOW_FILE.parent.mkdir(parents=True, exist_ok=True)
DOCKER_FLOW_FILE.write_text(docker_flow_json, encoding="utf-8")
print(f"Wrote {OUT_FILE}")
