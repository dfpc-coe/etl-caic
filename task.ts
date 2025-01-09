import moment from 'moment';
import { Static, TSchema, Type } from '@sinclair/typebox';
import { Feature, Polygon } from 'geojson';
import ETL, { Event, SchemaType, handler as internal, local, InputFeatureCollection, InputFeature } from '@tak-ps/etl';

export default class Task extends ETL {
    static name = 'etl-caic';

    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'DEBUG': Type.Boolean({ description: 'Print results in logs', default: false, })
            })
        } else {
            return Type.Object({
                forecaster: Type.String(),
                issueDateTime: Type.String({ format: 'date-time' }),
                expiryDateTime: Type.String(),
                isTranslated: Type.Boolean(),
                rating: Type.String(),
                ratingAbove: Type.String(),
                ratingNear: Type.String(),
                ratingBelow: Type.String()
            });
        }
    }

    async control(): Promise<void> {
        //const layer = await this.layer();

        const dateTime = moment().toISOString();
        const res = await fetch(`https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=%2Fproducts%2Fall%2Farea%3FproductType%3Davalancheforecast%26datetime%3D${encodeURIComponent(dateTime)}%26includeExpired%3Dfalse`, {
            method: 'GET'
        });

        if (!res.ok) throw new Error('Error fetching Forecast Geometries');

        const featMap = new Map<string, Feature>();
        (await res.json()).features.map((feat: Feature) => {
            featMap.set(String(feat.id), feat);
        });

        const res2 = await fetch(`https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=%2Fproducts%2Fall%3Fdatetime%3D${encodeURIComponent(dateTime)}%26includeExpired%3Dfalse`, {
            method: 'GET'
        });

        if (!res2.ok) throw new Error('Error fetching Forecast');
        const products = await res2.json();

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        const forecasts: Array<{
            id: string;
            title: string;
            publicName: string;
            type: string;
            polygons: Array<string>;
            areaId: string;
            forecaster: string;
            issueDateTime: string;
            expiryDateTime: string;
            isTranslated: boolean;
            weatherSummary: unknown;
            avalancheSummary: {
                days: Array<{
                    date: string;
                    content: string;
                }>
            }
            dangerRatings: {
                days: Array<{
                    alp: string;
                    tln: string;
                    btl: string;
                }>
            }
        }> = products.filter((f: any) => { return f.type === 'avalancheforecast' });

        const fills: Record<string, string> = {
            extreme: '#221e1f',
            high: '#ee1d23',
            considerable: '#f8931d',
            moderate: '#fef102',
            low: '#4db748',
            noRating: '#ffffff'
        };

        for (const f of forecasts) {
            if (!f.avalancheSummary.days.length) continue;

            const featGeometry = featMap.get(f.areaId);
            if (!featGeometry) continue;

            const feature: Static<typeof InputFeature> = {
                id: `caic-${f.areaId}`,
                type: 'Feature',
                properties: {
                    callsign: f.title,
                    fill: fills[f.dangerRatings.days[0].alp],
                    'fill-opacity': 0.5,
                    stroke: fills[f.dangerRatings.days[0].alp],
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

await local(new Task(import.meta.url), import.meta.url);

export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}
