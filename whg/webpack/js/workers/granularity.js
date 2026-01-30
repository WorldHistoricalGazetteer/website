import * as turf from '@turf/turf';

// /**
//  * Generates an array of ImageBitmaps representing mottled fill patterns for different colours.
//  * The pattern is created on an OffscreenCanvas and consists of randomly positioned,
//  * semi-transparent circles with a radial gradient.
//  *
//  * @async
//  * @param {string[]} colours - An array of CSS colour strings to generate patterns for.
//  * @param {number} finalSize - The desired size (width and height) of the individual pattern image (before tiling).
//  * @returns {Promise<ImageBitmap[]>} A promise that resolves to an array of ImageBitmaps, one for each colour.
//  */
// async function generateFillPatterns(colours, finalSize) {
//     const drawCanvas = new OffscreenCanvas(finalSize * 2, finalSize * 2);
//     const drawCtx = drawCanvas.getContext('2d');
//
//     const maxRadius = finalSize / 3;
//     const circleCount = Math.round(3 * Math.pow(finalSize, 2) / (Math.PI * Math.pow(maxRadius, 2)));
//     const circleData = [];
//
//     for (let i = 0; i < circleCount; i++) {
//         const x = Math.random() * finalSize;
//         const y = Math.random() * finalSize;
//         const radius = maxRadius * (Math.random() * 0.5 + 0.5);
//         circleData.push({ x, y, radius });
//     }
//
//     function drawCircle(x, y, radius, colour, ctx) {
//         const gradient = ctx.createRadialGradient(x, y, 0, x, y, radius);
//         gradient.addColorStop(0, colour);
//         gradient.addColorStop(1, colour.replace(/[^,]+(?=\))/, '0')); // fade to transparent
//         ctx.beginPath();
//         ctx.arc(x, y, radius, 0, Math.PI * 2);
//         ctx.fillStyle = gradient;
//         ctx.fill();
//     }
//
//     function drawPattern(colour) {
//         drawCtx.clearRect(0, 0, finalSize * 2, finalSize * 2);
//         circleData.forEach(({ x, y, radius }) => {
//             drawCircle(x, y, radius, colour, drawCtx);
//             drawCircle(x + finalSize, y, radius, colour, drawCtx);
//             drawCircle(x, y + finalSize, radius, colour, drawCtx);
//             drawCircle(x + finalSize, y + finalSize, radius, colour, drawCtx);
//         });
//     }
//
//     const bitmaps = [];
//
//     for (const colour of colours) {
//         drawPattern(colour);
//
//         // Create a canvas to extract the central tile-sized square
//         const cropCanvas = new OffscreenCanvas(finalSize, finalSize);
//         const cropCtx = cropCanvas.getContext('2d');
//
//         cropCtx.drawImage(
//             drawCanvas,
//             finalSize / 4, finalSize / 4, // start cropping from the centre
//             finalSize, finalSize,         // width and height of the crop
//             0, 0,                         // destination top-left corner
//             finalSize, finalSize          // destination width and height
//         );
//
//         const bitmap = await createImageBitmap(await cropCanvas.convertToBlob());
//         bitmaps.push(bitmap);
//     }
//
//     return bitmaps;
// }

/**
 * Buffers features in a GeoJSON FeatureCollection based on their geometry type and properties.
 * Features with a 'granularity' property > 0 will be buffered by that amount (in kilometers).
 * Other LineString and MultiLineString geometries will be buffered by a fixed amount (0.25 kilometers).
 * GeometryCollections will have their individual geometries processed recursively.
 * Features with granularity=0 are passed through unchanged (no buffering needed).
 *
 * @async
 * @param {turf.FeatureCollection} featureCollection - The GeoJSON FeatureCollection to process.
 * @param {string[]} colours - An array of CSS colour strings (currently unused).
 * @param {number} patternSize - The desired size of the generated fill pattern images (currently unused).
 * @returns {Promise<{bufferedFeatureCollection: turf.FeatureCollection | null, patternImageBitmaps: string[] | null}>}
 * A promise that resolves to an object containing the buffered FeatureCollection (patterns are no longer generated).
 */
async function bufferFeatureCollection(featureCollection, colours, patternSize) {
    const result = {
        bufferedFeatureCollection: null,
        patternImageBitmaps: null
    };

    let anyGranular = false;

    async function bufferIfGranular(feature) {
        const { geometry } = feature;

        // Only buffer if granularity > 0
        if (feature?.properties?.granularity && feature.properties.granularity > 0) {
            anyGranular = true;
            const buffered = turf.buffer(geometry, feature.properties.granularity, { units: 'kilometers' });
            return buffered.geometry;
        }

        // Convert any remaining LineString and MultiLineString geometries to polygons with breadth of 0.5 km
        if (geometry.type === 'LineString' || geometry.type === 'MultiLineString') {
            const buffered = turf.buffer(geometry, 0.25, { units: 'kilometers' });
            return buffered.geometry;
        }

        return geometry;
    }

    /**
     * Processes a single GeoJSON Feature. If it's a GeometryCollection, it processes its geometries recursively.
     * Otherwise, it buffers the geometry using the bufferIfGranular function.
     *
     * @async
     * @param {turf.Feature<turf.Geometry>} feature - The GeoJSON Feature to process.
     * @returns {Promise<turf.Feature<turf.Geometry>>} The processed GeoJSON Feature with the (potentially) buffered geometry.
     */
    async function processFeature(feature) {
        const geometry = feature.geometry;

        if (!geometry) return feature;

        try {
            if (geometry.type === 'GeometryCollection') {
                const processedGeometries = [];

                for (let geom of geometry.geometries) {
                    const wrapped = { geometry: geom, properties: feature.properties };
                    const processed = await bufferIfGranular(wrapped);
                    processedGeometries.push(processed);
                }

                const allPolygons = processedGeometries.every(
                    g => g.type === 'Polygon' || g.type === 'MultiPolygon'
                );

                const outputGeometry = allPolygons
                    ? turf.multiPolygon(processedGeometries.map(g => g.coordinates))
                    : turf.geometryCollection(processedGeometries);

                return turf.feature(outputGeometry, feature.properties, { id: feature.id });
            } else {
                const bufferedGeometry = await bufferIfGranular(feature);
                return turf.feature(bufferedGeometry, feature.properties, { id: feature.id });
            }
        }
        catch (error) {
            console.error('Error processing feature:', error);
            return feature; // Return the original feature if an error occurs
        }
    }

    const processedFeatures = [];
    try {
        for (let feature of featureCollection.features) {
            processedFeatures.push(await processFeature(feature));
        }
    }
    catch (error) {
        console.error('Error processing feature collection:', error);
        return result; // Return early if an error occurs
    }

    result.bufferedFeatureCollection = turf.featureCollection(processedFeatures);

    // Pattern generation is disabled (commented out above)
    // if (anyGranular) {
    //     result.patternImageBitmaps = await generateFillPatterns(colours, patternSize);
    // }

    return result;
}

/**
 * Handles messages received by the Web Worker. It expects an event with data containing
 * a 'featureCollection' (GeoJSON FeatureCollection) and an array of 'colours'.
 * It calls the bufferFeatureCollection function to buffer the features and generate
 * fill patterns if necessary. The result (buffered GeoJSON and pattern image URLs)
 * is then posted back to the main thread.
 *
 * @param {MessageEvent} event - The message event received by the worker.
 * @param {object} event.data - The data sent with the message.
 * @param {turf.FeatureCollection} event.data.featureCollection - The GeoJSON FeatureCollection to process.
 * @param {string[]} event.data.colours - An array of CSS colour strings for pattern generation.
 */
self.onmessage = async function(event) {
    const { colours, featureCollection } = event.data;
    const patternSize = 64;

    try {
        const { bufferedFeatureCollection, patternImageBitmaps } =
            await bufferFeatureCollection(featureCollection, colours, patternSize);

        self.postMessage({
            patterns: patternImageBitmaps,
            bufferedGeoJSON: bufferedFeatureCollection
        });
    } catch (error) {
        console.error('Error processing data in worker:', error);
        self.postMessage({ error: 'Error processing data' });
    }
};
