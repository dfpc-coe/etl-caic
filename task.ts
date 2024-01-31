import fs from 'node:fs';
import moment from 'moment';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import xml2js from 'xml2js';
import { JSONSchema6 } from 'json-schema';
import ETL, { Event, SchemaType } from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(String(fs.readFileSync(dotfile))));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export interface Share {
    ShareId: string;
    CallSign?: string;
    Password?: string;
}

export default class Task extends ETL {
    static async schema(type: SchemaType = SchemaType.Input): Promise<JSONSchema6> {
        if (type === SchemaType.Input) {
            return {
                type: 'object',
                required: ['INREACH_MAP_SHARES'],
                properties: {
                    'INREACH_MAP_SHARES': {
                        type: 'array',
                        description: 'Inreach Share IDs to pull data from',
                        // @ts-ignore
                        display: 'table',
                        items: {
                            type: 'object',
                            required: [
                                'ShareID',
                            ],
                            properties: {
                                CallSign: {
                                    type: 'string',
                                    description: 'Human Readable Name of the Operator - Used as the callsign in TAK'
                                },
                                ShareId: {
                                    type: 'string',
                                    description: 'Garmin Inreach Share ID or URL'
                                },
                                Password: {
                                    type: 'string',
                                    description: 'Optional: Garmin Inreach MapShare Password'
                                }
                            }
                        }
                    },
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print ADSBX results in logs'
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
        const layer = await this.layer();

        if (!layer.environment.INREACH_MAP_SHARES) throw new Error('No INREACH_MAP_SHARES Provided');
        if (!Array.isArray(layer.environment.INREACH_MAP_SHARES)) throw new Error('INREACH_MAP_SHARES must be an array');

        const obtains: Array<Promise<Feature[]>> = [];
        for (const share of layer.environment.INREACH_MAP_SHARES) {
            obtains.push((async (share: Share): Promise<Feature[]> => {
                try {
                    if (share.ShareId.startsWith('https://')) {
                        share.ShareId = new URL(share.ShareId).pathname.replace(/^\//, '');
                    } else if (share.ShareId.startsWith('share.garmin.com')) {
                        share.ShareId = share.ShareId.replace('share.garmin.com/', '');
                    }
                } catch (err) {
                    console.error(err);
                }

                if (!share.CallSign) share.CallSign = share.ShareId;
                console.log(`ok - requesting ${share.ShareId} ${share.CallSign ? `(${share.CallSign})` : ''}`);

                const url = new URL(`/feed/Share/${share.ShareId}`, 'https://explore.garmin.com')
                url.searchParams.append('d1', moment().subtract(30, 'minutes').utc().format());

                const kmlres = await fetch(url);
                const body = await kmlres.text();

                const featuresmap: Map<string, Feature> = new Map();
                const features: Feature[] = [];

                if (!body.trim()) return features;

                const xml = await xml2js.parseStringPromise(body);
                if (!xml.kml || !xml.kml.Document) throw new Error('XML Parse Error: Document not found');
                if (!xml.kml.Document[0] || !xml.kml.Document[0].Folder || !xml.kml.Document[0].Folder[0]) return;

                console.log(`ok - ${share.ShareId} has ${xml.kml.Document[0].Folder[0].Placemark.length} locations`);
                for (const placemark of xml.kml.Document[0].Folder[0].Placemark) {
                    if (!placemark.Point || !placemark.Point[0]) continue;

                    const coords = placemark.Point[0].coordinates[0].split(',').map((ele: string) => {
                        return parseFloat(ele);
                    });

                    const feat: Feature<Geometry, { [name: string]: any; }> = {
                        id: `inreach-${share.CallSign}`,
                        type: 'Feature',
                        properties: {
                            callsign: share.CallSign,
                            time: new Date(placemark.TimeStamp[0].when[0]),
                            start: new Date(placemark.TimeStamp[0].when[0])
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: coords
                        }
                    };

                    if (featuresmap.has(String(feat.id))) {
                        const existing = featuresmap.get(String(feat.id));

                        if (moment(feat.properties.time).isAfter(existing.properties.time)) {
                            featuresmap.set(String(feat.id), feat);
                        }
                    } else {
                        featuresmap.set(String(feat.id), feat);
                    }
                }

                features.push(...Array.from(featuresmap.values()))

                return features;
            })(share))
        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        }

        for (const res of await Promise.all(obtains)) {
            if (!res || !res.length) continue;
            fc.features.push(...res);
        }

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
