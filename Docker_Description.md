# Docker Description File

## Project title

Smart Mobility & Dynamic Parking Navigation System

## Purpose

This project is implemented in the Docker-based Node-RED environment required by the COMP7503C Multimedia Technologies programming assignment. The container setup provides a browser-based Node-RED runtime for flow execution and a MongoDB instance for persistent historical storage.

## Core runtime assumptions

- Node-RED runs inside the course Docker environment on port `1880`
- MongoDB is reachable from Node-RED at `mongodb://localhost:27017`
- The exported flow file `Smart_Mobility_Dynamic_Parking_Flow.json` is imported into the containerised Node-RED editor

## Suggested runtime stack

- Base image: `nodered/node-red`
- Database: MongoDB
- Required Node-RED package:
  - `node-red-dashboard`
  - `node-red-contrib-mongodb3`

## Repository Docker files

- `docker-compose.yml`
- `docker/node-red/Dockerfile`
- `docker/node-red/data/settings.js`
- `docker/node-red/data/flows.json`

These files provide a ready-to-run assignment deployment in which Node-RED and MongoDB are started together using Docker Compose.

## Example container commands

### Node-RED container

```bash
docker run -itd \
  -p 1880:1880 \
  -v /opt/node_red:/data \
  --name nodered \
  nodered/node-red
```

### MongoDB container

```bash
docker run -itd \
  -p 27017:27017 \
  -v /opt/mongodb:/data/db \
  --name smartmobility-mongo \
  mongo
```

## Deployment procedure

1. Run `docker compose up --build` from the repository root.
2. Wait for Node-RED and MongoDB to start.
3. Open `http://127.0.0.1:1880`.
4. Open `http://127.0.0.1:1880/ui/` for the dashboard.
5. Leave the system running to accumulate historical data for the predictive and historical dashboard functions.

The Docker runtime flow at `docker/node-red/data/flows.json` is preconfigured to connect to MongoDB through the Docker service hostname `mongo`.

## Produced deliverable

The Docker-based environment hosts a smart-city dashboard that:

- pulls live open data from Hong Kong government sources
- stores normalized snapshots in MongoDB
- computes predictive parking recommendations
- presents smart mobility insights through an English Node-RED dashboard

## Submission note

This file is intended to satisfy the assignment requirement for a Docker description document in the absence of a Docker Hub ID submission.
