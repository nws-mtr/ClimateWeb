# ClimateWeb

ClimateWeb is an open data workflow developed by the National Weather Service to make station-level climate information easy to reuse by the public. The repository packages recent observations from Synoptic with precipitation normals from XMACIS into a single, lightweight JSON file that can be embedded in web experiences or downstream applications.

## Project layout

- `bin/` — command-line helpers for fetching station data and writing payloads.
- `lib/` — minimal clients for the external Synoptic and XMACIS APIs.
- `src/` — utilities that transform raw responses into compact station dictionaries.
- `config/stations.yaml` — list of ASOS and HADS station identifiers to query.
- `tests/` — pytest suite that exercises the data processing pipeline.

## Requirements

- Python 3.9+ (timezone backports are pulled in automatically for older runtimes).
- Dependencies listed in `requirements.txt` (`numpy`, `pytest`, and timezone backports where needed).
- A Synoptic API token available in the environment as `SYNOPTIC_KEY` when running the fetch scripts.

## Configuration

Station identifiers live in `config/stations.yaml`. Update the `ASOS` and `HADS` lists to control which stations are retrieved from Synoptic and used when requesting precipitation normals from XMACIS. If XMACIS returns an error for a primary station (for example, "no data available"), you can define a backup station to try instead by adding an entry under `xmacis_fallbacks`, e.g.

```yaml
xmacis_fallbacks:
  KO69: KXYZ  # Try KXYZ if KO69 has no XMACIS data
```

## Usage

1. Install dependencies (ideally inside a virtual environment):
   ```bash
   pip install -r requirements.txt
   ```
2. Export your Synoptic API token:
   ```bash
   export SYNOPTIC_KEY="<your-token>"
   ```
3. Build the combined payload:
   ```bash
   python bin/build_station_payloads.py
   ```

`bin/build_station_payloads.py` fetches ASOS and HADS data from Synoptic, computes temperature ranges and precipitation summaries in `src/data_processor.py`, and writes the merged results to `station_payloads.json` at the repository root. The resulting JSON is small enough to serve directly to browsers or to feed another public API.

### How data is processed

- **Synoptic observations** — `bin/fetch_synoptic_data.py` retrieves the latest temperature readings, hourly precipitation totals, and longer timeseries for configured stations. The responses are parsed and validated in `lib/synoptic_client.py`.
- **XMACIS precipitation normals** — `bin/fetch_xmacis_precip.py` requests water-year precipitation totals and normals from XMACIS for each station.
- **Payload shaping** — `src/data_processor.py` converts the raw inputs into simplified dictionaries, computing daily temperature extremes, daily precipitation, water-year accumulation, and percent-of-normal metrics.

Each `station_payloads.json` entry contains the station ID, name, location, elevation, and computed climate fields tailored for public consumption.

## Testing

Run the test suite with:

```bash
pytest
```

The tests ensure the formatting helpers continue to handle edge cases in observation times, precipitation baselines, and temperature extremes.
