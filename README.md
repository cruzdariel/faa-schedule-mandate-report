# An Analysis of Mandated Flight Reductions at Orlando International Airport

A real-time dashboard tracking flight cancellations at Orlando International Airport (MCO) during a period of aviation disruption. This application monitored flight data from the Greater Orlando Aviation Authority (GOAA) API and provided comprehensive analytics on cancellation patterns during the November 2025 FAA flight reduction mandates.

## Overview

This dashboard was built to track and analyze flight cancellations at MCO during the November 2025 FAA flight reduction mandates. The data is frozen at November 17th, 2025, when the FAA order was rescinded, providing a historical snapshot of the cancellation patterns during this period.

## Features

- **Real-time Flight Tracking**: Monitors scheduled flights and their cancellation status
- **Historical Data Archive**: Stores flight data in a SQLite database with de-duplication
- **Interactive Visualizations**:
  - Pie chart showing cancellations by airline carrier
  - Stacked bar chart displaying top 10 airports affected (arrivals vs. departures)
  - Line graph tracking daily cancellation percentages over time
- **Detailed Flight Table**: Searchable table of all cancelled flights with route and timing information
- **Smart Data Preservation**: Once a flight is marked as cancelled for past dates, it remains cancelled in the database

## Technical Details

### Data Collection

- **API Source**: GOAA Flights API (api.goaa.aero)
- **Timezone Handling**: All MCO flights use America/New_York (Orlando local time), all inboudn arrivals use UTC time.
- **De-duplication**: Flights are uniquely identified by SHA-1 hash of flight number, departure airport, and local date
- **Update Frequency**: Configurable via timer (currently disabled for frozen dataset)

### Database Schema

SQLite database (`data.db`) with the following structure:
- `uid`: Unique flight identifier (SHA-1 hash)
- `iata_flight`: Flight number (e.g., "AA1234")
- `dep_airport`, `arr_airport`: Airport codes
- `scheduled_ts`: UTC timestamp of scheduled departure/arrival
- `is_arrival`: Boolean flag (1 for arrivals, 0 for departures)
- `canceled`: Boolean flag for cancellation status
- `status`: Current flight status from API
- `first_seen_utc`, `last_seen_utc`: Tracking timestamps

### Key Functions

- `upsert_flight()`: Intelligently updates flight data while preserving historical cancellations
- `carrier_cancel_counts()`: Aggregates cancellations by airline carrier code
- `airport_cancel_counts()`: Analyzes affected airports by departure/arrival
- `daily_cancellation_percentages()`: Calculates daily cancellation rates
- `_freeze_data_at_nov_17()`: Removes data after November 17th, 2025 to maintain historical accuracy

## License and Disclaimer

This dashboard was developed with assistance from Claude Code. This project is provided as-is for data analysis and historical tracking purposes.

## Notes

- The rolling 3-day ingest functionality is currently disabled (lines 527-528, 535) to maintain the frozen historical dataset
- Cancellation status is preserved for past flights - once marked cancelled, the status persists in the database
