import { Feature } from '@tak-ps/node-cot'
import { Static, TSchema, Type } from '@sinclair/typebox';
import { Feature as GeoJSONFeature, Polygon } from 'geojson';
import { fetch } from '@tak-ps/etl';

import ETL, { Event, SchemaType, handler as internal, local, InvocationType, DataFlowType } from '@tak-ps/etl';

const FeatureCollectionSchema = Type.Object({
    type: Type.Literal('FeatureCollection'),
    features: Type.Array(Type.Object({
        type: Type.Literal('Feature'),
        id: Type.Union([Type.String(), Type.Number()]),
        properties: Type.Any(),
        geometry: Type.Any()
    }))
});

const AvalancheForecastSchema = Type.Object({
    type: Type.Literal('avalancheforecast'),
    id: Type.String(),
    title: Type.Optional(Type.String()),
    publicName: Type.Optional(Type.String()),
    polygons: Type.Array(Type.String()),
    areaId: Type.String(),
    forecaster: Type.Optional(Type.String()),
    issueDateTime: Type.String(),
    expiryDateTime: Type.String(),
    isTranslated: Type.Optional(Type.Boolean()),
    weatherSummary: Type.Optional(Type.Unknown()),
    avalancheSummary: Type.Optional(Type.Object({
        days: Type.Array(Type.Object({
            date: Type.String(),
            content: Type.String()
        }))
    })),
    dangerRatings: Type.Optional(Type.Object({
        days: Type.Array(Type.Object({
            alp: Type.String(),
            tln: Type.String(),
            btl: Type.String()
        }))
    }))
});

const ProductSchema = Type.Array(
    Type.Union([
        AvalancheForecastSchema,
        Type.Object({
            type: Type.String(),
        }, { additionalProperties: true })
    ])
);

export default class Task extends ETL {
    static name = 'etl-caic';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Type.Object({
                    'DEBUG': Type.Boolean({ description: 'Print results in logs', default: false, })
                })
            } else {
                return Type.Object({
                    forecaster: Type.Optional(Type.String()),
                    issueDateTime: Type.String({ format: 'date-time' }),
                    expiryDateTime: Type.String(),
                    isTranslated: Type.Optional(Type.Boolean()),
                    rating: Type.String(),
                    ratingAbove: Type.String(),
                    ratingNear: Type.String(),
                    ratingBelow: Type.String()
                });
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        //const layer = await this.layer();

        const dateTime = new Date().toISOString();
        const res = await fetch(`https://avalanche.state.co.us/avid/products/all/area?productType=avalancheforecast&datetime=${encodeURIComponent(dateTime)}&includeExpired=false`, {
            method: 'GET'
        });

        if (!res.ok) throw new Error('Error fetching Forecast Geometries');

        const areas = await res.typed(FeatureCollectionSchema);

        const featMap = new Map<string, GeoJSONFeature>();
        areas.features.map((feat) => {
            featMap.set(String(feat.id), feat as GeoJSONFeature);
        });

        const res2 = await fetch(`https://avalanche.state.co.us/avid/products/all?datetime=${encodeURIComponent(dateTime)}&includeExpired=false`, {
            method: 'GET'
        });

        if (!res2.ok) throw new Error('Error fetching Forecast');
        const products = await res2.typed(ProductSchema);

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        const forecasts = products.filter((f): f is Static<typeof AvalancheForecastSchema> => f.type === 'avalancheforecast');

        const severity = [ 'extreme', 'high', 'considerable', 'moderate', 'low', 'noRating', 'earlySeason', 'offSeason', 'noForecast' ];

        const humanSeverity: Record<string, string> = {
            extreme: 'Extreme',
            high: 'High',
            considerable: 'Considerable',
            moderate: 'Moderate',
            low: 'Low',
            noRating: 'No Rating',
            earlySeason: 'Early Season',
            offSeason: 'Off Season',
            noForecast: 'No Forecast'
        }

        const fills: Record<string, string> = {
            extreme: '#221e1f',
            high: '#ee1d23',
            considerable: '#f8931d',
            moderate: '#fef102',
            low: '#4db748',
            noRating: '#cccccc',
            earlySeason: '#cccccc',
            offSeason: '#cccccc',
            noForecast: '#cccccc'
        };

        for (const f of forecasts) {
            if (!f.avalancheSummary || !f.avalancheSummary.days?.length) continue;
            if (!f.dangerRatings || !f.dangerRatings.days?.length) continue;

            const featGeometry = featMap.get(f.areaId);
            if (!featGeometry) continue;

            let severityIndex = severity.indexOf('noForecast');
            for (const rating of [f.dangerRatings.days[0].btl, f.dangerRatings.days[0].tln, f.dangerRatings.days[0].alp]) {
                const idx = severity.indexOf(rating);
                if (idx !== -1 && idx < severityIndex) severityIndex = idx;
            }

            const feature: Static<typeof Feature.InputFeature> = {
                id: `caic-${f.areaId}`,
                type: 'Feature',
                properties: {
                    callsign: humanSeverity[severity[severityIndex]],
                    fill: fills[severity[severityIndex]],
                    'fill-opacity': 0.5,
                    stroke: fills[severity[severityIndex]],
                    'stroke-opacity': 0.75,
                    remarks: f.avalancheSummary.days.length ? f.avalancheSummary.days[0].content : 'No Remarks',
                    metadata: {
                        forecaster: f.forecaster,
                        issueDateTime: f.issueDateTime,
                        expiryDateTime: f.expiryDateTime,
                        isTranslated: f.isTranslated,
                        ratingAbove: f.dangerRatings.days[0].alp,
                        ratingNear: f.dangerRatings.days[0].tln,
                        ratingBelow: f.dangerRatings.days[0].btl,
                    }
                },
                geometry: featGeometry.geometry as Polygon
            };

            if (feature.geometry.type.startsWith('Multi')) {
                feature.geometry.coordinates.forEach((coords: any, idx: number) => {
                    fc.features.push({
                        id: feature.id + '-' + idx,
                        type: 'Feature',
                        properties: feature.properties,
                        geometry: {
                            // @ts-expect-error -- Cast to ENUM
                            type: feature.geometry.type.replace('Multi', ''),
                            coordinates: coords
                        }
                    });
                });
            } else {
                fc.features.push(feature)
            }
        }

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}
