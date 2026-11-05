# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### Pending Release

### v3.17.1

- :bug: Add lib file

### v3.17.0

- :tada: Render forecast remarks as Markdown - issue & expiry times in Mountain time, a danger rating table per forecast day, the avalanche problems with likelihood, size & aspect/elevation, and the summary converted from CAIC HTML to Markdown
- :tada: Name each forecast area after the Regional Discussion covering most of its polygons (ie: `Southern Mountains - Moderate`) as CAIC no longer publishes zone names - a product `title`/`publicName` or area `name` is still preferred when present
- :tada: Add a Links section to the remarks that opens the forecast area on the CAIC map alongside a `url` Output field
- :rocket: Add `region`, `rating` & `ratingDateTime` to the Output schema
- :white_check_mark: Add `lib/forecast.ts` unit tests
- :arrow_up: Update `@tak-ps/etl` to v11

### v3.16.0

- :arrow_up: Update Core Dependencies

### v3.15.0

- :bug: Fetch forecasts from the new `/avid` API base after the CAIC site rebuild removed `/api-proxy/avid`
- :bug: Handle `earlySeason`, `offSeason`, and `noForecast` danger ratings
- :rocket: Make `publicName`, `forecaster`, and `isTranslated` optional as they are no longer returned

### v3.14.0

- :arrow_up: Update Core Dependencies

### v3.13.0

- :rocket: Add Capabilities document
- :arrow_up: Update GH Actions

### v3.12.0

- :arrow_up: Update Core Dependencies

### v3.11.0

- :arrow_up: Update Core Dependencies

### v3.10.0

- :arrow_up: Update Core Dependencies

### v3.9.0

- :arrow_up: Update Core Dependencies

### v3.8.0

- :arrow_up: Update Core Dependencies

### v3.7.0

- :arrow_up: Update Core Dependencies
- :arrow_up: Remove `moment` dependency
- :rocket: Strongly Type API Responses

### v3.6.0

- :arrow_up: Update Core Dependencies

### v3.5.0

- :arrow_up: Update Core Dependencies

### v3.4.0

- :arrow_up: Update Core Deps

### v3.3.0

- :arrow_up: Update Core Deps

### v3.2.0

- :arrow_up: Update Core Deps

### v3.1.0

- :tada: Include severity in Callsign

### v3.0.0

- :tada: Update to `CloudTAK@v6`

### v2.8.1

- :bug: Remove webhook type

### v2.8.0

- :tada: Implement Capabilities

### v2.7.0

- :tada: Full with highest danger level

### v2.6.0

- :rocket: Remove geometries with no forecast

### v2.5.0

- :rocket: New opacity values

### v2.4.0

- :rocket: Update to latest ESLint Config

### v2.4.0

- :rocket: Update to latest ESLint Config

### v2.3.2

- :bug: Remove static from schema

### v2.3.1

- :arrow_up: Update ETL Base

### v2.3.0

- :rocket: Update metadata props

### v2.2.0

- :rocket: Output Colour data based on Above Treeline Rating

### v2.1.0

- :rocket: Improved remarks & poly support

### v2.0.0

- :rocket: Update to latest token strategy

### v1.0.0

- :tada: Initial Commit
