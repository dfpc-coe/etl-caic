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
                properties: {}
            }
        }
    }

    async control(): Promise<void> {
        //const layer = await this.layer();

        const dateTime = moment().toISOString();
        const res = await fetch(`https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=%2Fproducts%2Fall%2Farea%3FproductType%3Davalancheforecast%26datetime%3D${encodeURIComponent(dateTime)}%26includeExpired%3Dtrue`, {
            method: 'GET'
        });

        if (!res.ok) throw new Error('Error fetching Forecast Geometries');

        const feats: FeatureCollection = await res.json();
        const featMap = new Map<string, Feature>();
        feats.features.map((feat: Feature) => {
            featMap.set(String(feat.id), feat);
        });

        //const res2 = await fetch(`https://avalanche.state.co.us/api-proxy/avid?_api_proxy_uri=%2Fproducts%2Fall%3Fdatetime%3D${encodeURIComponent(dateTime)}%26includeExpired%3Dtrue`, {
        //    method: 'GET'
        //});
        //
        //if (!res2.ok) throw new Error('Error fetching Forecast');
        //const forecast = await res2.json();

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        };

        fc.features = Array.from(featMap.values()).map((feat: Feature) => {
            feat.id = `caic-${feat.id}`;
            delete feat.properties.centroid;
            delete feat.properties.bbox;
            return feat;
        });

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
