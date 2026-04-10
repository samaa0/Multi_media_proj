# Smart Mobility & Dynamic Parking Navigation System

## Abstract

This project presents a smart-city decision-support system that improves urban driving convenience by predicting parking availability at the expected arrival time instead of showing only static, present-moment parking status. The system integrates four categories of Hong Kong government open data: roadside parking meter occupancy, private car park vacancy, traffic journey time indicators, and weather conditions. The implementation is built in Node-RED and uses MongoDB to store historical snapshots for short-term forecasting and trend analysis. A rule-based analytics engine estimates parking depletion, correlates predicted depletion with travel time, adjusts recommendations according to rainfall conditions, and visualises historical congestion hotspots. The resulting dashboard provides a real-time recommendation, a roadside parking risk assessment, a private indoor backup ranking, and historical availability charts. The project demonstrates multimedia systems concepts through heterogeneous data ingestion, temporal correlation, predictive logic, and dashboard-based information delivery for a practical smart mobility use case.

**Keywords:** smart city, Node-RED, MongoDB, parking prediction, open data, traffic analytics, weather-aware routing, dashboard visualisation

## 1. Introduction

Parking search is a common source of urban congestion, driver frustration, and wasted travel time. Many existing parking applications present a static snapshot of current availability, but this is insufficient for practical decision-making because parking spaces may be occupied before the driver arrives. This limitation becomes more serious in dense urban areas such as Hong Kong, where roadside meter turnover is rapid, traffic conditions change quickly, and weather can influence user preference for open-air or covered parking.

The aim of this project is to construct a predictive smart mobility system that goes beyond static display. Instead of only answering the question, "How many spaces are available now?", the system addresses a more useful question: "Which parking option is still likely to be available when I arrive?" To answer this question, the system correlates live parking vacancy data with traffic travel time and weather conditions, while preserving historical observations in MongoDB to support short-term depletion forecasting and historical trend visualisation.

This design directly matches the assignment requirement to acquire data streams from `data.gov.hk`, perform data analysis and correlation, and present insights through a dashboard. The application is implemented in the required Node-RED environment and produces an exported flow file, a Docker description file, and a professional English project report.

## 2. Selected Smart City Use Case

The selected use case is **smart mobility and dynamic parking navigation**. The target user is a driver travelling to a busy Hong Kong destination who must choose between roadside parking meters and nearby private car parks. A conventional static app may show several roadside spaces still available, yet those spaces may disappear before the user reaches the destination. The proposed system reduces this uncertainty by using short-term historical depletion and current traffic conditions to estimate whether a parking option remains feasible at arrival.

The dashboard is organised around five preset destination profiles:

- Central
- Causeway Bay
- Tsim Sha Tsui
- Mong Kok
- Kwun Tong

This citywide-lite design achieves a useful balance between broad data coverage and a polished demonstration. The ingestion layer remains citywide, while the dashboard focuses on preset destinations to ensure clarity, repeatability, and report-friendly evaluation.

## 3. Data Sources

The system uses official Hong Kong government open data and related official APIs. The implementation accesses the following sources:

| Data category | Source | Purpose |
|---|---|---|
| Roadside parking meters | Transport Department metered parking spaces and occupancy status datasets | Estimate roadside parking availability and short-term depletion |
| Private car parks | `api.data.gov.hk` car park information and vacancy API | Provide indoor/private backup parking options |
| Traffic | Transport Department Journey Time Indication System data | Estimate travel delay and arrival time |
| Weather and rainfall | Hong Kong Observatory open data API | Penalise open-air roadside parking during rainfall |

The live endpoints used by the implemented flow are:

- `https://resource.data.one.gov.hk/td/psiparkingspaces/spaceinfo/parkingspaces.csv`
- `https://resource.data.one.gov.hk/td/psiparkingspaces/occupancystatus/occupancystatus.csv`
- `https://api.data.gov.hk/v1/carpark-info-vacancy?data=info&vehicleTypes=privateCar&lang=en`
- `https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=en`
- `https://resource.data.one.gov.hk/td/jss/Journeytimev2.xml`
- `https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread&lang=en`
- `https://data.weather.gov.hk/weatherAPI/opendata/hourlyRainfall.php?lang=en`

All sources are accessed in English, and all dashboard labels and report content are also written in English.

## 4. System Architecture

The system is implemented as a modular Node-RED application with four logical flows: setup, ingestion, analytics, and dashboard.

```mermaid
flowchart LR
    A[Transport Department meter data] --> B[Node-RED ingestion]
    C[Private car park APIs] --> B
    D[Journey time XML] --> B
    E[HKO weather and rainfall] --> B
    B --> F[Normalization functions]
    F --> G[(MongoDB historical snapshots)]
    G --> H[Analytics engine]
    H --> I[Recommendation output]
    H --> J[Historical trend charts]
    I --> K[Node-RED Dashboard]
    J --> K
```

### 4.1 Ingestion flow

The ingestion layer polls all external datasets every five minutes. Each source is parsed into a uniform structure before storage:

- CSV nodes parse roadside parking datasets
- JSON nodes parse private car park and weather APIs
- an XML node parses the traffic dataset
- function nodes normalise records and attach a common `snapshotTime`

This structure is intentionally modular so that each source can be replaced or updated independently.

### 4.2 Storage flow

MongoDB is used to preserve time-stamped historical observations. The following collections are used:

| Collection | Stored content |
|---|---|
| `parking_meter_snapshots` | Normalized roadside meter snapshot documents |
| `private_carpark_snapshots` | Normalized private car park snapshot documents |
| `traffic_snapshots` | Normalized traffic summary documents |
| `weather_snapshots` | Weather and rainfall snapshots |
| `destination_profiles` | Preset destination metadata |
| `recommendation_logs` | Persisted recommendation outputs for debugging and report evidence |

Each parking snapshot stores an array of normalized parking records with the following fields:

- `sourceType`
- `facilityId`
- `facilityName`
- `district`
- `latitude`
- `longitude`
- `availableSpaces`
- `totalSpaces`
- `isCovered`
- `snapshotTime`

This schema supports both real-time recommendation and historical comparison without needing multiple incompatible data models.

## 5. Analytics and Correlation Methods

The system uses a deterministic, explainable predictive analytics engine. It does not claim to be a machine-learning system. This design choice is appropriate for an academic assignment because the logic is transparent, reproducible, and directly linked to the ingested datasets.

### 5.1 Feature A: dynamic depletion countdown

For each candidate parking location, the latest availability is compared with the availability approximately fifteen minutes earlier. If a reduction is observed, the system estimates the consumption rate:

\[
\text{ratePerMinute} = \frac{\text{available}_{t-15} - \text{available}_{t}}{15}
\]

If the rate is positive, the predicted time to full occupancy is:

\[
\text{minutesToFull} = \frac{\text{available}_{t}}{\text{ratePerMinute}}
\]

If the rate is zero or negative, the system reports a stable or improving state instead of generating a false warning.

### 5.2 Feature B: traffic-adjusted availability

The system extracts the latest traffic records and matches them to the selected destination using destination-specific keyword profiles. The estimated drive time is then compared with the predicted `minutesToFull` value. When the expected arrival time is longer than the remaining parking window, the candidate is marked as unsafe and downgraded in the final ranking. This is the core correlation between traffic conditions and parking availability.

### 5.3 Feature C: weather-aware routing

Weather data from the Hong Kong Observatory is used to determine whether rainfall is currently affecting the city. During rain, open-air roadside parking receives a penalty and covered private car parks receive a bonus. Under dry conditions, roadside parking remains competitive because it is typically lower cost and closer to the street-level destination.

### 5.4 Feature D: historical hotspot analysis

Historical roadside availability around the selected destination is aggregated over the previous seven days. The dashboard presents:

- a line chart comparing today and yesterday
- a seven-day average hourly availability profile
- a hotspot table that highlights the lowest average availability periods

This component satisfies the assignment requirement for historical analysis and demonstrates that the system is not limited to instantaneous display.

### 5.5 Recommendation scoring

The recommendation engine combines multiple signals:

- current availability ratio
- absolute available spaces
- depletion risk
- arrival feasibility based on traffic time
- distance to the destination
- rainfall suitability
- user preference for either budget roadside parking or sheltered indoor parking

The final output consists of one primary recommendation, one indoor backup option, and a human-readable advisory sentence in English.

## 6. Dashboard Design

The Node-RED Dashboard is arranged into four zones so that the user can quickly interpret the recommendation.

```mermaid
flowchart TB
    A[Controls: destination selector, preference switch, refresh] --> B[Top advisory panel]
    B --> C[Roadside prediction panel]
    B --> D[Private backup panel]
    C --> E[Historical trend panel]
    D --> E
```

### 6.1 Top panel

The top panel summarises the current destination, recommendation text, traffic condition, rainfall condition, temperature, and cache freshness. This acts as the system's most important decision-support region.

### 6.2 Left panel: roadside prediction

The left section contains:

- a gauge showing roadside availability percentage
- a text summary of current spaces
- a depletion countdown or stable-state message
- a risk badge indicating whether spaces are likely to disappear before arrival

### 6.3 Right panel: private backup parking

The right panel lists ranked backup facilities with their available spaces, covered status, and arrival suitability. This supports immediate re-routing when roadside meters are considered too risky.

### 6.4 Bottom panel: historical trends

The bottom panel presents historical availability patterns through two charts and one hotspot table. This allows the user to anticipate difficult parking periods, not merely react to the current moment.

## 7. Implementation Details in Node-RED

The exported flow file `Smart_Mobility_Dynamic_Parking_Flow.json` contains four tabs:

1. `0. Setup`
2. `1. Ingestion`
3. `2. Analytics`
4. `3. Dashboard`

The setup flow seeds destination profiles and primes default dashboard state. The ingestion flow collects and stores all raw snapshots. The analytics flow retrieves seven days of historical data from MongoDB and computes dashboard-ready outputs. The dashboard flow handles user interaction and visual rendering.

The design uses the following Node-RED capabilities:

- `http request` nodes for open-data access
- `csv`, `json`, and `xml` nodes for heterogeneous parsing
- `function` nodes for data cleaning, normalization, and recommendation logic
- `mongodb3` nodes for snapshot persistence and historical retrieval
- `join` and `change` nodes for message assembly
- dashboard widgets such as `ui_dropdown`, `ui_switch`, `ui_button`, `ui_gauge`, `ui_text`, `ui_chart`, and `ui_template`

The dashboard is intentionally written in English and uses formal labels so that it remains consistent with the report and submission expectations.

## 8. Experimental Scenarios

The following evaluation scenarios were designed for the implemented system:

### Scenario 1: normal traffic and dry weather

Under moderate traffic and dry weather, a nearby roadside meter cluster with acceptable depletion time should remain the top recommendation. The dashboard should show a non-critical risk badge and maintain roadside parking as the first choice.

### Scenario 2: heavy traffic causes pre-arrival depletion

If the traffic estimate rises above the predicted depletion window, the system should downgrade the roadside option and elevate a backup indoor car park. This demonstrates direct correlation between journey time and future parking feasibility.

### Scenario 3: rainfall-driven indoor preference

When rainfall is detected, even a comparable roadside meter cluster should lose ranking to a nearby covered private car park. This scenario demonstrates cross-domain data correlation between weather and parking recommendation.

### Scenario 4: stable availability

If the current parking count is unchanged or improves relative to the previous fifteen-minute baseline, the system should avoid exaggerated warnings and instead report a stable or improving condition.

### Scenario 5: temporary API outage

If live updates are delayed, the dashboard should continue using the latest cached MongoDB snapshot and display a stale-data notice. This ensures graceful degradation rather than total dashboard failure.

## 9. Discussion

The implemented system has several strengths. First, it integrates heterogeneous data modalities into a coherent smart-city application. Second, it performs genuine temporal and cross-domain correlation rather than displaying isolated statistics. Third, it uses explainable recommendation logic, which is suitable for academic demonstration and future extension.

However, the system also has limitations. The depletion forecast is short-term and linear, which may not fully capture abrupt traffic surges or event-driven parking demand. The reliability of the recommendation also depends on the consistency of upstream APIs and the completeness of metadata for location mapping. Furthermore, the dashboard uses preset destinations for clarity, which improves usability but limits free-form destination entry.

Future work could include:

- longer historical modelling with day-of-week effects
- map-based visualisation
- user-defined destinations
- more sophisticated spatiotemporal clustering for roadside meter groups
- evaluation against manually observed ground truth

## 10. Conclusion

This project demonstrates a complete smart-city multimedia application in Node-RED that satisfies the core assignment requirements of data acquisition, analysis, correlation, historical storage, and dashboard presentation. By combining parking, traffic, and weather datasets, the system produces a practical recommendation that is more useful than a static parking display. The use of MongoDB enables both predictive depletion estimation and historical congestion analysis, while the dashboard communicates the resulting insights clearly in English. Overall, the project shows how open government data can be transformed into a proactive urban mobility service through lightweight but effective multimedia systems engineering.

## References

1. Department of Computer Science, The University of Hong Kong. *COMP7503C Multimedia Technologies Programming Assignment*. Accessed from the assignment PDF provided in the course materials.
2. Hong Kong Government Open Data Portal. *data.gov.hk*. https://data.gov.hk/
3. Transport Department, Hong Kong SAR Government. *Metered parking spaces datasets*. https://resource.data.one.gov.hk/td/psiparkingspaces/
4. Hong Kong Government API. *Car park information and vacancy API*. https://api.data.gov.hk/v1/carpark-info-vacancy
5. Transport Department, Hong Kong SAR Government. *Journey Time Indication System data*. https://resource.data.one.gov.hk/td/jss/Journeytimev2.xml
6. Hong Kong Observatory. *Current weather report API*. https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread&lang=en
7. Hong Kong Observatory. *Hourly rainfall API*. https://data.weather.gov.hk/weatherAPI/opendata/hourlyRainfall.php?lang=en
8. Node-RED. https://nodered.org/
9. MongoDB. https://www.mongodb.com/
