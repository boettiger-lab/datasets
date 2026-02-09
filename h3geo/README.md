# h3geo: Dynamic Global H3 Layer for MapLibre

A lightweight, drop-in module for adding a global, multi-resolution H3 hexagon layer to [MapLibre GL JS](https://maplibre.org/).

## Features

*   **Global Coverage**: Renders H3 hexagons across the entire globe (Resolution 0-15+).
*   **Adaptive Resolution**: Automatically switches H3 resolution based on the map's zoom level for optimal performance and detail.
*   **Dateline Wrapping**: Seamlessly handles the International Date Line (180th meridian), preventing rendering artifacts.
*   **Performance**: Uses client-side generation with viewport culling and debouncing to maintain high frame rates.
*   **Customizable**: Configure colors, opacity, and layer IDs easily.

## Installation

This module requires **MapLibre GL JS** and **h3-js**.

### 1. Include Dependencies

Add the following scripts to your HTML `<head>`:

```html
<!-- MapLibre GL JS -->
<link href="https://unpkg.com/maplibre-gl/dist/maplibre-gl.css" rel="stylesheet">
<script src="https://unpkg.com/maplibre-gl/dist/maplibre-gl.js"></script>

<!-- h3-js (Required Global) -->
<script src="https://unpkg.com/h3-js"></script>
```

### 2. Import the Module

Download `h3geo.js` and import `H3DynamicLayer` in your script:

```javascript
import { H3DynamicLayer } from './h3geo.js';
```

## Usage

Initialize the layer after your map has loaded:

```javascript
const map = new maplibregl.Map({
    container: 'map',
    style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json', // or any other style
    center: [0, 20],
    zoom: 2
});

map.on('load', () => {
    // initialize layer
    const h3Layer = new H3DynamicLayer(map, {
        fillColor: '#007cbf',
        fillOpacity: 0.1,
        outlineColor: '#000000',
        outlineWidth: 1,
        outlineOpacity: 0.8
    });
    
    // Start rendering
    h3Layer.start();
});
```

## API Reference

### `new H3DynamicLayer(map, options)`

*   `map` (MapLibre Map): The initialized map instance.
*   `options` (Object):
    *   `fillColor` (string): Hex color for hexagon fill (default: `'#007cbf'`).
    *   `fillOpacity` (number): Opacity of fill (0-1, default: `0.1`).
    *   `outlineColor` (string): Hex color for outlines (default: `'#000000'`).
    *   `outlineWidth` (number): Width of outlines in pixels (default: `1`).
    *   `minZoom` (number): Minimum zoom to render (default: `0`).
    *   `maxZoom` (number): Maximum zoom to render (default: `24`).

### Methods

*   `start()`: Adds sources/layers and starts listening to map events.
*   `stop()`: Removes sources/layers and stops listeners.
*   `updateHexagons()`: Manually trigger a refresh of the hexagons.

## Events

The window object dispatches an `h3update` event whenever the grid is refreshed (e.g., on move or zoom).

```javascript
window.addEventListener('h3update', (e) => {
    console.log('Current Zoom:', e.detail.zoom);
    console.log('H3 Resolution:', e.detail.resolution);
    console.log('Visible Hexes:', e.detail.count);
});
```
