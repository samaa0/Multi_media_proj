# Smart Mobility & Dynamic Parking Navigation System

This repository contains a complete COMP7503C assignment package for a Node-RED smart-city application that predicts parking availability at arrival time by correlating live parking, traffic, and weather data from Hong Kong government open datasets.

## Files

- `Smart_Mobility_Dynamic_Parking_Flow.json`: exported Node-RED flow for the completed system.
- `Project_Report.md`: academic project report in English.
- `Docker_Description.md`: Docker environment description for submission.
- `docker-compose.yml`: Docker orchestration for the assignment deployment.
- `docker/node-red/Dockerfile`: custom Node-RED image with the required dashboard and MongoDB nodes preinstalled.
- `docker/node-red/data/flows.json`: Docker runtime flow file with the MongoDB host set to `mongo`.
- `tools/generate_smart_mobility_flow.py`: reproducible generator for the exported Node-RED flow.
- `HKO.Flow_a2327531ab27fad76bc15da4fb0c23ee.json`: original sample flow supplied with the assignment materials.

## Required Node-RED packages

- `node-red-dashboard`
- `node-red-contrib-mongodb3`

The flow also uses core Node-RED nodes such as `http request`, `json`, `xml`, `csv`, `function`, `join`, `change`, `link`, and dashboard widgets.

## Docker run

This is the recommended assignment path because the brief explicitly requires the Node-RED Docker environment.

1. Install Docker Desktop on macOS if Docker is not already available.
2. Open a terminal in the project folder.
3. Build and start the Docker services:

```bash
docker compose up --build
```

4. Open Node-RED in the browser:
   - `http://127.0.0.1:1880`
5. Open the dashboard:
   - `http://127.0.0.1:1880/ui/`
6. Leave the system running so that the 5-minute polling cycle can accumulate historical data.
7. Use one of the preset destinations:
   - Central
   - Causeway Bay
   - Tsim Sha Tsui
   - Mong Kok
   - Kwun Tong

The Docker deployment automatically:

- starts MongoDB as a separate service
- starts Node-RED with `node-red-dashboard` and `node-red-contrib-mongodb3`
- mounts the Docker runtime flow file from `docker/node-red/data/flows.json`
- uses `mongodb://mongo:27017` inside the container network

To stop the stack:

```bash
docker compose down
```

To stop the stack and remove persistent data:

```bash
docker compose down -v
```

## Local fallback run used for validation

If Docker is unavailable on the machine, the project can also be opened with the local Node-RED runtime prepared in `local-nodered`.

1. Open a terminal in the project folder.
2. Start the local MongoDB-compatible test server:

```bash
cd local-nodered
npm run start:mongo
```

3. In a second terminal, start Node-RED:

```bash
cd local-nodered
npm run start:nodered
```

4. Open the editor:
   - `http://127.0.0.1:1880`
5. Open the dashboard:
   - `http://127.0.0.1:1880/ui/`

The local validation setup uses:

- `local-nodered/settings.js`
- `local-nodered/start-local-mongodb.js`
- the exported flow file `Smart_Mobility_Dynamic_Parking_Flow.json`

## Implemented features

- Dynamic depletion countdown for roadside parking meters
- Traffic-adjusted arrival feasibility analysis
- Weather-aware preference for covered private parking during rainfall
- Historical trend charts and hotspot table derived from MongoDB snapshots
- English dashboard with recommendation text, gauges, backup parking ranking, and historical analytics

## Polling and storage design

- Polling frequency: every 5 minutes
- Historical window for depletion analysis: 15 minutes
- Historical window for dashboard trend analysis: 7 days
- MongoDB collections:
  - `parking_meter_snapshots`
  - `private_carpark_snapshots`
  - `traffic_snapshots`
  - `weather_snapshots`
  - `destination_profiles`
  - `recommendation_logs`

## Notes

- The flow is designed for hybrid live-plus-history operation. If an upstream API temporarily fails, the dashboard continues using cached MongoDB data and marks the state as stale once the cache age exceeds the expected polling window.
- The recommendation engine is rule-based predictive analytics rather than machine learning. This is intentional so that the methodology remains explainable and academically defensible.
- After deployment, allow enough time for data accumulation before capturing dashboard screenshots for the final PDF submission.
