# ClimateWeb

ClimateWeb is an open data workflow developed by the National Weather Service to make station-level climate information easy to reuse by the public. It combines recent Synoptic observations with XMACIS precipitation normals into a lightweight JSON file that can be served directly to browsers or other applications and visualized with the included static dashboard.

## Data sources

- Synoptic API for current and recent observations (temperature, hourly precipitation, and timeseries for configured stations).
- XMACIS for water-year precipitation totals and long-term normals, with optional station fallbacks when the primary site is unavailable.

## Repository layout

- `bin/` - entry points for building payloads and fetching upstream data (`build_station_payloads.py`, `fetch_synoptic_data.py`, `fetch_xmacis_precip.py`).
- `lib/` - thin API clients for Synoptic and XMACIS.
- `src/` - data shaping logic (`data_processor.py`) that computes temps and precipitation metrics.
- `config/stations.yaml` - station lists and optional XMACIS fallback mappings.
- `web/` - static dashboard and the generated `station_payloads.json`.
- `tests/` - pytest coverage for the processing pipeline and XMACIS helper.

## Requirements

- Python 3.9+
- Dependencies in `requirements.txt`
- A Synoptic API token exported as `SYNOPTIC_KEY` to call the upstream API

### Setup

```bash
python -m venv .venv
source .venv/bin/activate  # or: .\.venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt

# Required for Synoptic requests
export SYNOPTIC_KEY="your-token"  # or: $env:SYNOPTIC_KEY="your-token" in PowerShell
```

## Configuration

Station identifiers live in `config/stations.yaml`. Update the `ASOS` and `HADS` lists to control which stations are retrieved from Synoptic and used when requesting precipitation normals from XMACIS. If XMACIS returns an error for a primary station (for example, "no data available"), you can define a backup station to try instead by adding an entry under `xmacis_fallbacks`, e.g.

```yaml
xmacis_fallbacks:
  KO69: KXYZ  # Try KXYZ if KO69 has no XMACIS data
```

## Build the payload

Run the builder from the repo root:

```bash
python bin/build_station_payloads.py
```

The script fetches:

- Latest ASOS temps plus 6-hour maxima/minima from Synoptic
- Hourly ASOS precipitation from Synoptic
- HADS timeseries for temperature and precipitation since the start of the water year
- Water-year precipitation totals and normals from XMACIS (with optional fallbacks)

`src/data_processor.py` combines these inputs into simplified station dictionaries and writes them to `web/station_payloads.json` (creating the `web/` folder if it does not exist).

### Payload contents

`web/station_payloads.json` has the shape:

```json
{
  "data": [
    {
      "stid": "KSFO",
      "name": "San Francisco Intl",
      "latitude": 37.62,
      "longitude": -122.38,
      "elevationFT": 13,
      "dateTime": "2024-11-30T15:00:00Z",
      "airTempF": 58,
      "dailyMaxF": 61,
      "dailyMinF": 54,
      "dailyAccumIN": 0.12,
      "waterYearIN": 3.45,
      "waterYearNormIN": 5.67,
      "percentOfNorm": 61
    }
  ]
}
```

Temperatures are in Fahrenheit and precipitation values are in inches. A value of `9999` indicates missing upstream data.

### How data is processed

- Synoptic observations: `bin/fetch_synoptic_data.py` retrieves temperature observations, hourly precip intervals, and timeseries for configured stations. Responses are parsed and validated in `lib/synoptic_client.py`.
- XMACIS precipitation normals: `bin/fetch_xmacis_precip.py` requests water-year precipitation totals and normals for each station, optionally retrying with a fallback ID.
- Payload shaping: `src/data_processor.py` converts the raw inputs into simplified dictionaries, computing daily temperature extremes (since local midnight), daily precipitation, water-year accumulation, and percent-of-normal metrics.

Each `station_payloads.json` entry contains the station ID, name, location, elevation, and computed climate fields tailored for public consumption.

## View the dashboard

Serve the `web/` directory with any static file server so the browser can fetch `station_payloads.json`:

```bash
cd web
python -m http.server 8000
# then open http://localhost:8000/ in your browser
```

The dashboard reads the generated payload, supports search and sorting, and can print a text report. Regenerate `web/station_payloads.json` to refresh the data.

## Testing

Run the test suite with:

```bash
pytest
```

The tests cover the data processing helpers, precipitation normalization, and the XMACIS fallback handling.
