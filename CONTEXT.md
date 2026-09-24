# VN Climate Risk Monitor

Turns Open-Meteo weather data into tested rainfall signals for Hanoi's wards and communes.

## Geography

**Ward**:
One of Hanoi's 126 administrative areas (wards and communes) that the product reports on.
_Avoid_: location, commune, district

**Grid cell**:
The weather-model cell Open-Meteo snaps a coordinate to; several wards can share one.
_Avoid_: grid, point, station

## Weather data

**Forecast run**:
One issuance of a weather model's forecast, identified by the time it was issued; it covers the next 72 hours.
_Avoid_: forecast vintage, run, batch

**Archive**:
Historical hourly weather for past days, as reanalysed by a weather model.
_Avoid_: history, backfill data

**Rain pressure**:
An explainable score for how hard forecast rainfall will press on a ward; a prioritisation signal, not a flood probability.
_Avoid_: flood risk, alert level

## Pipeline

**Bronze object**:
An immutable raw API response stored exactly as received.
_Avoid_: raw file, landing file

**Staging table**:
An append-only Silver table (`silver.stg_*`) holding every loaded row, duplicates included.
_Avoid_: bronze table, raw table

**Clean table**:
A deduplicated Silver table (`silver.clean_*`) where each key appears once and rows are updated in place.
_Avoid_: curated table, intermediate model

**Asset**:
A table the pipeline builds incrementally, named by its schema-qualified table name.
_Avoid_: dataset, model, flow

**Watermark**:
The start time of an asset's last successful processing; the next run reads only source rows newer than it.
_Avoid_: checkpoint, cursor, high-water mark

**Job run**:
One attempt to process one asset, recorded whether it succeeds or fails.
_Avoid_: processing run, execution

**Publication**:
The point at which a Gold build that passed its checks becomes what the dashboard reads.
_Avoid_: release, published snapshot
