import test from 'node:test';
import assert from 'node:assert/strict';
import {
    areaName,
    forecastRemarks,
    formatDateTime,
    formatDay,
    forecastUrl,
    htmlToMarkdown,
    interiorPoint,
    regionName,
    worstRating
} from '../lib/forecast.js';

const forecast = {
    type: 'avalancheforecast' as const,
    id: 'product-1',
    polygons: ['A', 'B', 'C'],
    areaId: 'area-1',
    issueDateTime: '2026-11-04T23:30:00Z',
    expiryDateTime: '2026-11-05T23:30:00Z',
    avalancheSummary: {
        days: [{
            date: '2026-11-05T23:30:00Z',
            content: '<p>You could trigger an avalanche in <strong>wind-drifted snow</strong>.&nbsp;</p><p>See the <a target="_blank" href="https://avalanche.state.co.us/reports/incident/1">report</a>.</p>'
        }]
    },
    dangerRatings: {
        days: [
            { position: 1, alp: 'moderate', tln: 'low', btl: 'low', date: '2026-11-05T23:30:00Z' },
            { position: 2, alp: 'low', tln: 'low', btl: 'noRating', date: '2026-11-06T23:30:00Z' }
        ]
    },
    avalancheProblems: {
        days: [[{
            type: 'windSlab',
            aspectElevations: ['nw_alp', 'n_alp', 'ne_tln'],
            likelihood: 'possible',
            expectedSize: { min: '1.0', max: '2.0' }
        }], []]
    }
};

const discussions = [{
    type: 'regionaldiscussion' as const,
    id: 'disc-south',
    polygons: ['A', 'B', 'Z'],
    areaId: 'region-south',
    publicTitle: 'Southern Mountains Regional Discussion',
    issueDateTime: '2026-11-04T23:30:00Z',
    expiryDateTime: '2026-11-05T23:30:00Z'
}, {
    type: 'regionaldiscussion' as const,
    id: 'disc-central',
    polygons: ['C'],
    areaId: 'region-central',
    publicTitle: 'Central Mountains Regional Discussion',
    issueDateTime: '2026-11-04T23:30:00Z',
    expiryDateTime: '2026-11-05T23:30:00Z'
}];

test('htmlToMarkdown converts paragraphs, links, emphasis and entities', () => {
    assert.equal(
        htmlToMarkdown('<p>Snow &amp; wind.&nbsp;</p><p>See <a href="https://example.com/a?b=1">the report</a>.</p><ul><li>one</li><li>two</li></ul>'),
        'Snow & wind.\n\nSee [the report](https://example.com/a?b=1).\n\n- one\n- two'
    );

    assert.equal(htmlToMarkdown('<p><b>Bold </b> and <em>soft</em><br>break</p>'), '**Bold** and *soft*\nbreak');
    assert.equal(htmlToMarkdown('<p></p>'), '');
});

test('dates are formatted in Mountain time', () => {
    assert.equal(formatDateTime('2026-11-04T23:30:00Z'), 'Wed, Nov 4, 2026, 4:30 PM MST');
    assert.equal(formatDateTime('2026-07-04T23:30:00Z'), 'Sat, Jul 4, 2026, 5:30 PM MDT');
    assert.equal(formatDay('2026-11-05T23:30:00Z'), 'Thu, Nov 5');
});

test('worstRating picks the most severe elevation band', () => {
    assert.equal(worstRating({ alp: 'moderate', tln: 'low', btl: 'low' }), 'moderate');
    assert.equal(worstRating({ alp: 'low', tln: 'considerable', btl: 'noRating' }), 'considerable');
    assert.equal(worstRating({ alp: 'bogus', tln: 'bogus', btl: 'bogus' }), 'noForecast');
});

test('regionName follows the discussion covering most polygons', () => {
    assert.equal(regionName(forecast, discussions), 'Southern Mountains');
    assert.equal(regionName({ ...forecast, polygons: ['Q'] }, discussions), undefined);
    assert.equal(regionName(forecast, []), undefined);
});

test('areaName prefers a product title over the region', () => {
    assert.equal(areaName(forecast, discussions), 'Southern Mountains');
    assert.equal(areaName({ ...forecast, title: 'Front Range' }, discussions), 'Front Range');
    assert.equal(areaName({ ...forecast, publicName: 'Sawatch' }, discussions), 'Sawatch');
});

test('forecastRemarks renders the forecast as Markdown', () => {
    assert.equal(forecastRemarks(forecast, 'Southern Mountains', [-107.33, 37.53]), [
        '# Southern Mountains Avalanche Forecast',
        '',
        '**Issued:** Wed, Nov 4, 2026, 4:30 PM MST',
        '**Expires:** Thu, Nov 5, 2026, 4:30 PM MST',
        '',
        '## Danger Rating',
        '',
        '| Day | Above Treeline | Near Treeline | Below Treeline |',
        '| --- | --- | --- | --- |',
        '| Thu, Nov 5 | Moderate | Low | Low |',
        '| Fri, Nov 6 | Low | Low | No Rating |',
        '',
        '## Avalanche Problems',
        '',
        '- **Wind Slab** - Possible, D1 to D2 (Above Treeline: NW, N; Near Treeline: NE)',
        '',
        '## Summary - Thu, Nov 5',
        '',
        'You could trigger an avalanche in **wind-drifted snow**.',
        '',
        'See the [report](https://avalanche.state.co.us/reports/incident/1).',
        '',
        '## Links',
        '',
        '- [CAIC Forecast](https://avalanche.state.co.us/?lat=37.53000&lng=-107.33000)',
        '- [Colorado Avalanche Information Center](https://avalanche.state.co.us/)'
    ].join('\n'));
});

test('forecastRemarks omits empty sections', () => {
    const remarks = forecastRemarks({
        ...forecast,
        forecaster: 'A. Forecaster',
        avalancheProblems: undefined,
        dangerRatings: undefined,
        avalancheSummary: { days: [{ date: '2026-11-05T23:30:00Z', content: '<p> </p>' }] }
    });

    assert.equal(remarks, [
        '# Avalanche Forecast',
        '',
        '**Issued:** Wed, Nov 4, 2026, 4:30 PM MST',
        '**Expires:** Thu, Nov 5, 2026, 4:30 PM MST',
        '**Forecaster:** A. Forecaster',
        '',
        '## Links',
        '',
        '- [CAIC Forecast](https://avalanche.state.co.us/)',
        '- [Colorado Avalanche Information Center](https://avalanche.state.co.us/)'
    ].join('\n'));
});

test('interiorPoint keeps a centroid inside the area and otherwise picks a point inside', () => {
    const square = (x: number, y: number) => [[[x, y], [x + 1, y], [x + 1, y + 1], [x, y + 1], [x, y]]];

    assert.deepEqual(interiorPoint({ type: 'Polygon', coordinates: square(0, 0) }, [0.5, 0.5]), [0.5, 0.5]);

    const twoParts = { type: 'MultiPolygon' as const, coordinates: [square(0, 0), square(4, 4)] };
    assert.deepEqual(interiorPoint(twoParts, [2.5, 2.5]), [0.5, 0.5]);
    assert.deepEqual(interiorPoint(twoParts, undefined), [0.5, 0.5]);

    const holed = { type: 'Polygon' as const, coordinates: [square(0, 0)[0], [[0.25, 0.25], [0.75, 0.25], [0.75, 0.75], [0.25, 0.75], [0.25, 0.25]]] };
    assert.deepEqual(interiorPoint(holed, [0.5, 0.5]), [0.5, 0.5]);
});

test('forecastUrl deep links the CAIC map to a point', () => {
    assert.equal(forecastUrl([-107.333333, 37.5]), 'https://avalanche.state.co.us/?lat=37.50000&lng=-107.33333');
    assert.equal(forecastUrl(undefined), 'https://avalanche.state.co.us/');
});
