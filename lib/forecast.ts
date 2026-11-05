import { Static, Type } from '@sinclair/typebox';
import type { Geometry, Position } from 'geojson';

export const SEVERITY = [ 'extreme', 'high', 'considerable', 'moderate', 'low', 'noRating', 'earlySeason', 'offSeason', 'noForecast' ] as const;

export type Severity = typeof SEVERITY[number];

export const SEVERITY_LABEL: Record<Severity, string> = {
    extreme: 'Extreme',
    high: 'High',
    considerable: 'Considerable',
    moderate: 'Moderate',
    low: 'Low',
    noRating: 'No Rating',
    earlySeason: 'Early Season',
    offSeason: 'Off Season',
    noForecast: 'No Forecast'
};

export const SEVERITY_FILL: Record<Severity, string> = {
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

const PROBLEM_LABEL: Record<string, string> = {
    noProblem: 'No Avalanche Problem',
    persistentSlab: 'Persistent Slab',
    deepPersistentSlab: 'Deep Persistent Slab',
    stormSlab: 'Storm Slab',
    windSlab: 'Wind Slab',
    dryLoose: 'Loose Dry',
    wetLoose: 'Loose Wet',
    wetSlab: 'Wet Slab',
    cornice: 'Cornice Fall',
    glide: 'Glide'
};

const LIKELIHOOD_LABEL: Record<string, string> = {
    unlikely: 'Unlikely',
    possible: 'Possible',
    likely: 'Likely',
    veryLikely: 'Very Likely',
    almostCertain: 'Almost Certain'
};

const ELEVATION_LABEL: Record<string, string> = {
    alp: 'Above Treeline',
    tln: 'Near Treeline',
    btl: 'Below Treeline'
};

const TIMEZONE = 'America/Denver';

export const SOURCE_URL = 'https://avalanche.state.co.us/';

export const DangerDay = Type.Object({
    date: Type.Optional(Type.String()),
    alp: Type.String(),
    tln: Type.String(),
    btl: Type.String()
});

export const AvalancheProblem = Type.Object({
    type: Type.String(),
    aspectElevations: Type.Optional(Type.Array(Type.String())),
    likelihood: Type.Optional(Type.String()),
    expectedSize: Type.Optional(Type.Object({
        min: Type.Optional(Type.String()),
        max: Type.Optional(Type.String())
    }))
});

export const AvalancheForecast = Type.Object({
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
    avalancheSummary: Type.Optional(Type.Object({
        days: Type.Array(Type.Object({
            date: Type.String(),
            content: Type.String()
        }))
    })),
    dangerRatings: Type.Optional(Type.Object({
        days: Type.Array(DangerDay)
    })),
    avalancheProblems: Type.Optional(Type.Object({
        days: Type.Array(Type.Array(AvalancheProblem))
    }))
});

export const RegionalDiscussion = Type.Object({
    type: Type.Literal('regionaldiscussion'),
    id: Type.String(),
    polygons: Type.Array(Type.String()),
    areaId: Type.String(),
    publicTitle: Type.Optional(Type.String()),
    issueDateTime: Type.String(),
    expiryDateTime: Type.String()
});

const ENTITY: Record<string, string> = {
    amp: '&',
    lt: '<',
    gt: '>',
    quot: '"',
    apos: '\'',
    nbsp: ' '
};

function decodeEntities(text: string): string {
    return text.replace(/&(#x[0-9a-f]+|#[0-9]+|[a-z]+);/gi, (match, entity: string) => {
        if (entity.startsWith('#')) {
            const code = entity[1].toLowerCase() === 'x' ? parseInt(entity.slice(2), 16) : parseInt(entity.slice(1), 10);
            return Number.isNaN(code) ? match : String.fromCodePoint(code);
        }

        return ENTITY[entity.toLowerCase()] ?? match;
    });
}

/**
 * Convert the HTML fragments CAIC publishes into Markdown that also reads
 * cleanly as plain text in TAK clients without a Markdown renderer
 */
export function htmlToMarkdown(html: string): string {
    const text = html
        .replace(/\r?\n/g, ' ')
        .replace(/<br\s*\/?>/gi, '\n')
        .replace(/<(strong|b)\b[^>]*>(.*?)<\/\1>/gis, (_, __, inner: string) => inner.trim() ? `**${inner.trim()}**` : '')
        .replace(/<(em|i)\b[^>]*>(.*?)<\/\1>/gis, (_, __, inner: string) => inner.trim() ? `*${inner.trim()}*` : '')
        .replace(/<a\b[^>]*href=["']([^"']+)["'][^>]*>(.*?)<\/a>/gis, (_, href: string, inner: string) => `[${inner.trim()}](${href})`)
        .replace(/<li\b[^>]*>(.*?)<\/li>/gis, (_, inner: string) => `\n- ${inner.trim()}`)
        .replace(/<\/(p|div|ul|ol|h[1-6])>/gi, '\n\n')
        .replace(/<[^>]+>/g, '');

    return decodeEntities(text)
        .split('\n')
        .map((line) => line.replace(/[ \t\u00a0]+/g, ' ').trim())
        .join('\n')
        .replace(/\n{3,}/g, '\n\n')
        .trim();
}

export function formatDateTime(iso: string): string {
    return new Intl.DateTimeFormat('en-US', {
        weekday: 'short',
        month: 'short',
        day: 'numeric',
        year: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
        hour12: true,
        timeZone: TIMEZONE,
        timeZoneName: 'short'
    }).format(new Date(iso));
}

export function formatDay(iso: string): string {
    return new Intl.DateTimeFormat('en-US', {
        weekday: 'short',
        month: 'short',
        day: 'numeric',
        timeZone: TIMEZONE
    }).format(new Date(iso));
}

export function severityLabel(rating: string): string {
    return SEVERITY_LABEL[rating as Severity] ?? rating;
}

/**
 * The most severe rating across the elevation bands of a forecast day
 */
export function worstRating(day: Static<typeof DangerDay>): Severity {
    let worst: Severity = 'noForecast';

    for (const rating of [day.btl, day.tln, day.alp]) {
        const idx = SEVERITY.indexOf(rating as Severity);
        if (idx !== -1 && idx < SEVERITY.indexOf(worst)) worst = SEVERITY[idx];
    }

    return worst;
}

/**
 * The Regional Discussion covering most of a forecast area's polygons names
 * the region - the forecast products themselves no longer carry a name
 */
export function regionName(
    forecast: Static<typeof AvalancheForecast>,
    discussions: Array<Static<typeof RegionalDiscussion>>
): string | undefined {
    const polygons = new Set(forecast.polygons);

    let best: { name: string, overlap: number } | undefined;

    for (const discussion of discussions) {
        if (!discussion.publicTitle) continue;

        const overlap = discussion.polygons.filter((id) => polygons.has(id)).length;
        if (!overlap || (best && overlap <= best.overlap)) continue;

        best = {
            name: discussion.publicTitle.replace(/\s*Regional Discussion\s*$/i, '').trim(),
            overlap
        };
    }

    return best?.name;
}

export function areaName(
    forecast: Static<typeof AvalancheForecast>,
    discussions: Array<Static<typeof RegionalDiscussion>>
): string | undefined {
    return forecast.title || forecast.publicName || regionName(forecast, discussions);
}

function expectedSize(size?: { min?: string, max?: string }): string | null {
    const min = size?.min ? `D${Number(size.min)}` : null;
    const max = size?.max ? `D${Number(size.max)}` : null;

    if (min && max && min !== max) return `${min} to ${max}`;
    return min ?? max;
}

function aspectElevations(entries: string[] = []): string {
    const bands = new Map<string, string[]>();

    for (const entry of entries) {
        const [aspect, elevation] = entry.split('_');
        if (!aspect || !elevation) continue;

        const band = ELEVATION_LABEL[elevation] ?? elevation;
        bands.set(band, [...(bands.get(band) ?? []), aspect.toUpperCase()]);
    }

    return [...bands.entries()].map(([band, aspects]) => `${band}: ${aspects.join(', ')}`).join('; ');
}

function problemLine(problem: Static<typeof AvalancheProblem>): string {
    const details = [
        problem.likelihood ? LIKELIHOOD_LABEL[problem.likelihood] ?? problem.likelihood : null,
        expectedSize(problem.expectedSize)
    ].filter((detail) => detail !== null);

    const where = aspectElevations(problem.aspectElevations);

    return [
        `- **${PROBLEM_LABEL[problem.type] ?? problem.type}**`,
        details.length ? ` - ${details.join(', ')}` : '',
        where ? ` (${where})` : ''
    ].join('');
}

function inRing(point: Position, ring: Position[]): boolean {
    const [x, y] = point;
    let inside = false;

    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
        const [xi, yi] = ring[i];
        const [xj, yj] = ring[j];

        if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) inside = !inside;
    }

    return inside;
}

function inPolygon(point: Position, polygon: Position[][]): boolean {
    return inRing(point, polygon[0]) && !polygon.slice(1).some((hole) => inRing(point, hole));
}

function ringAverage(ring: Position[]): Position {
    const vertices = ring.slice(0, -1);
    const sum = vertices.reduce((acc, [x, y]) => [acc[0] + x, acc[1] + y], [0, 0]);
    return [sum[0] / vertices.length, sum[1] / vertices.length];
}

/**
 * A point inside the forecast area - the CAIC map resolves a forecast from
 * a lat/lng, so the published centroid is only usable when it falls inside
 * the (possibly multi-part) area
 */
export function interiorPoint(geometry: Geometry, centroid?: Position): Position | undefined {
    const polygons = geometry.type === 'Polygon'
        ? [geometry.coordinates]
        : geometry.type === 'MultiPolygon' ? geometry.coordinates : [];

    if (centroid && polygons.some((polygon) => inPolygon(centroid, polygon))) return centroid;

    for (const polygon of [...polygons].sort((a, b) => b[0].length - a[0].length)) {
        const candidate = ringAverage(polygon[0]);
        if (inPolygon(candidate, polygon)) return candidate;
    }

    return centroid;
}

export function forecastUrl(point?: Position): string {
    if (!point) return SOURCE_URL;

    const params = new URLSearchParams({
        lat: point[1].toFixed(5),
        lng: point[0].toFixed(5)
    });

    return `${SOURCE_URL}?${params}`;
}

export function forecastRemarks(
    forecast: Static<typeof AvalancheForecast>,
    name?: string,
    point?: Position
): string {
    const lines: Array<string | null> = [
        `# ${name ? `${name} ` : ''}Avalanche Forecast`,
        '',
        `**Issued:** ${formatDateTime(forecast.issueDateTime)}`,
        `**Expires:** ${formatDateTime(forecast.expiryDateTime)}`,
        forecast.forecaster ? `**Forecaster:** ${forecast.forecaster}` : null
    ];

    const days = forecast.dangerRatings?.days ?? [];
    if (days.length) {
        lines.push(
            '',
            '## Danger Rating',
            '',
            '| Day | Above Treeline | Near Treeline | Below Treeline |',
            '| --- | --- | --- | --- |',
            ...days.map((day, idx) => {
                const label = day.date ? formatDay(day.date) : `Day ${idx + 1}`;
                return `| ${label} | ${severityLabel(day.alp)} | ${severityLabel(day.tln)} | ${severityLabel(day.btl)} |`;
            })
        );
    }

    const problems = forecast.avalancheProblems?.days?.[0] ?? [];
    if (problems.length) {
        lines.push('', '## Avalanche Problems', '', ...problems.map(problemLine));
    }

    for (const day of forecast.avalancheSummary?.days ?? []) {
        const content = htmlToMarkdown(day.content);
        if (!content) continue;

        lines.push('', `## Summary - ${formatDay(day.date)}`, '', content);
    }

    lines.push(
        '',
        '## Links',
        '',
        `- [CAIC Forecast](${forecastUrl(point)})`,
        `- [Colorado Avalanche Information Center](${SOURCE_URL})`
    );

    return lines.filter((line) => line !== null).join('\n');
}
