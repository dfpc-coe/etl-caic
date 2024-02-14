import fs from 'node:fs';
import moment from 'moment';
import { FeatureCollection, Feature } from 'geojson';
import { JSONSchema6 } from 'json-schema';
import ETL, { Event, SchemaType } from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(String(fs.readFileSync(dotfile))));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static async schema(type: SchemaType = SchemaType.Input): Promise<JSONSchema6> {
        if (type === SchemaType.Input) {
            return {
                type: 'object',
                required: [],
                properties: {
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print results in logs'
                    }
                }
            }
        } else {
            return {
                type: 'object',
                required: [],
                properties: {
                    forecaster: { type: 'string' },
                    issueDateTime: { type: 'string' },
                    expiryDateTime: { type: 'string' },
                    isTranslated: { type: 'boolean' },
                    rating: { type: 'string' },
                    ratingAbove: { type: 'string' },
                    ratingNear: { type: 'string' },
                    ratingBelow: { type: 'string' },
                }
            }
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

        const fc: FeatureCollection = {
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

        for (const f of forecasts) {
            if (!featMap.has(f.areaId)) continue;

            const feature: Feature = {
                id: `caic-${f.areaId}`,
                type: 'Feature',
                properties: {
                    callsign: f.title,
                    forecaster: f.forecaster,
                    issueDateTime: f.issueDateTime,
                    expiryDateTime: f.expiryDateTime,
                    isTranslated: f.isTranslated,
                    remarks: f.avalancheSummary.days[0].content,
                    ratingAbove: f.dangerRatings.days[0].alp,
                    ratingNear: f.dangerRatings.days[0].tln,
                    ratingBelow: f.dangerRatings.days[0].btl,
                },
                geometry: featMap.get(f.areaId).geometry
            };

            if (feature.geometry.type.startsWith('Multi')) {
                // @ts-ignore -- Geometry Collections could technically be here
                feature.geometry.coordinates.forEach((coords: any, idx: number) => {
                    fc.features.push({
                        id: feature.id + '-' + idx,
                        type: 'Feature',
                        properties: feature.properties,
                        geometry: {
                            // @ts-ignore -- Cast to ENUM
                            type: feature.geometry.type.replace('Multi', ''),
                            coordinates: coords
                        }
                    });
                });
            } else {
                fc.features.push(feature)
            }
        };

        console.error(fc.features[0].properties);

        await this.submit(fc);
    }
}

export async function handler(event: Event = {}) {
    if (event.type === 'schema:input') {
        return await Task.schema(SchemaType.Input);
    } else if (event.type === 'schema:output') {
        return await Task.schema(SchemaType.Output);
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();
