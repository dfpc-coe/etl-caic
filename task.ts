import { Feature } from '@tak-ps/node-cot'
import { Static, TSchema, Type } from '@sinclair/typebox';
import type { Feature as GeoJSONFeature, Geometry, LineString, Point, Polygon, Position } from 'geojson';
import { fetch } from '@tak-ps/etl';
import {
    AvalancheForecast,
    RegionalDiscussion,
    SEVERITY_FILL,
    SEVERITY_LABEL,
    areaName,
    forecastRemarks,
    forecastUrl,
    interiorPoint,
    worstRating
} from './lib/forecast.js';

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

const ProductSchema = Type.Array(
    Type.Union([
        AvalancheForecast,
        RegionalDiscussion,
        Type.Object({
            type: Type.String(),
        }, { additionalProperties: true })
    ])
);

function areaProperties(feat: GeoJSONFeature): { name?: string, centroid?: Position } {
    const props: Record<string, unknown> = feat.properties ?? {};
    const { name, centroid } = props;

    return {
        name: typeof name === 'string' ? name : undefined,
        centroid: Array.isArray(centroid) && centroid.every((c) => typeof c === 'number') ? centroid : undefined
    };
}

function splitGeometry(geometry: Geometry): Array<Point | LineString | Polygon> {
    if (geometry.type === 'MultiPoint') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Point', coordinates }));
    }

    if (geometry.type === 'MultiLineString') {
        return geometry.coordinates.map((coordinates) => ({ type: 'LineString', coordinates }));
    }

    if (geometry.type === 'MultiPolygon') {
        return geometry.coordinates.map((coordinates) => ({ type: 'Polygon', coordinates }));
    }

    if (geometry.type === 'GeometryCollection') return [];

    return [ geometry ];
}

export default class Task extends ETL {
    static name = 'etl-caic';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow !== DataFlowType.Incoming) return Type.Object({});

        if (type === SchemaType.Input) {
            return Type.Object({
                'DEBUG': Type.Boolean({ description: 'Print results in logs', default: false, })
            });
        }

        return Type.Object({
            region: Type.Optional(Type.String()),
            url: Type.String({ format: 'uri' }),
            forecaster: Type.Optional(Type.String()),
            issueDateTime: Type.String({ format: 'date-time' }),
            expiryDateTime: Type.String({ format: 'date-time' }),
            ratingDateTime: Type.Optional(Type.String({ format: 'date-time' })),
            isTranslated: Type.Optional(Type.Boolean()),
            rating: Type.String(),
            ratingAbove: Type.String(),
            ratingNear: Type.String(),
            ratingBelow: Type.String()
        });
    }

    async control(): Promise<void> {
        const dateTime = new Date().toISOString();
        const res = await fetch(`https://avalanche.state.co.us/avid/products/all/area?productType=avalancheforecast&datetime=${encodeURIComponent(dateTime)}&includeExpired=false`, {
            method: 'GET'
        });

        if (!res.ok) throw new Error('Error fetching Forecast Geometries');

        const areas = await res.typed(FeatureCollectionSchema);

        const featMap = new Map<string, GeoJSONFeature>();
        for (const feat of areas.features) {
            featMap.set(String(feat.id), feat);
        }

        const res2 = await fetch(`https://avalanche.state.co.us/avid/products/all?datetime=${encodeURIComponent(dateTime)}&includeExpired=false`, {
            method: 'GET'
        });

        if (!res2.ok) throw new Error('Error fetching Forecast');
        const products = await res2.typed(ProductSchema);

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        const forecasts = products.filter((f): f is Static<typeof AvalancheForecast> => f.type === 'avalancheforecast');
        const discussions = products.filter((f): f is Static<typeof RegionalDiscussion> => f.type === 'regionaldiscussion');

        for (const f of forecasts) {
            if (!f.avalancheSummary || !f.avalancheSummary.days?.length) continue;
            if (!f.dangerRatings || !f.dangerRatings.days?.length) continue;

            const featGeometry = featMap.get(f.areaId);
            if (!featGeometry) continue;

            const today = f.dangerRatings.days[0];
            const rating = worstRating(today);
            const area = areaProperties(featGeometry);
            const name = area.name || areaName(f, discussions);
            const point = interiorPoint(featGeometry.geometry, area.centroid);

            const id = `caic-${f.areaId}`;
            const properties: Static<typeof Feature.InputFeature>['properties'] = {
                callsign: name ? `${name} - ${SEVERITY_LABEL[rating]}` : SEVERITY_LABEL[rating],
                type: 'u-d-f',
                fill: SEVERITY_FILL[rating],
                'fill-opacity': 0.5,
                stroke: SEVERITY_FILL[rating],
                'stroke-opacity': 0.75,
                remarks: forecastRemarks(f, name, point),
                metadata: {
                    region: name,
                    url: forecastUrl(point),
                    forecaster: f.forecaster,
                    issueDateTime: f.issueDateTime,
                    expiryDateTime: f.expiryDateTime,
                    ratingDateTime: today.date,
                    isTranslated: f.isTranslated,
                    rating: SEVERITY_LABEL[rating],
                    ratingAbove: today.alp,
                    ratingNear: today.tln,
                    ratingBelow: today.btl,
                }
            };

            const multi = featGeometry.geometry.type.startsWith('Multi');

            splitGeometry(featGeometry.geometry).forEach((geometry, idx) => {
                fc.features.push({
                    id: multi ? `${id}-${idx}` : id,
                    type: 'Feature',
                    properties,
                    geometry
                });
            });
        }

        await this.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url), event);
}
