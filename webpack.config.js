// /webpack.config.js

const path = require('path');
const CssMinimizerPlugin = require('css-minimizer-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const TerserPlugin = require('terser-webpack-plugin');
const Dotenv = require('dotenv-webpack');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;
const CopyWebpackPlugin = require('copy-webpack-plugin');
const { sentryWebpackPlugin } = require("@sentry/webpack-plugin");
const { execSync } = require('child_process');

// Function to get the current Git commit hash
const getGitHash = () => {
  try {
    return execSync('git rev-parse HEAD').toString().trim();
  } catch (e) {
    console.warn('Warning: Could not get git hash. Using a fallback release name.');
    return 'fallback-release';
  }
};

const isProduction = process.env.ENV_CONTEXT === 'whgazetteer-org';

module.exports = {
	mode: isProduction ? 'production' : 'development', // Use production mode for staging
    devtool: isProduction ? 'source-map' : 'eval-source-map',
	// Persistent on-disk build cache — the single biggest speed-up for COLD builds (a fresh `npm run
	// build`/`watch` start): webpack reuses previously-compiled modules from node_modules/.cache/webpack
	// instead of recompiling all ~22 entries from scratch. Does NOT change build output, only speed.
	// buildDependencies invalidates the cache when this config (or a referenced file) changes, so it
	// can never serve stale output.
	cache: {
		type: 'filesystem',
		buildDependencies: { config: [__filename] },
	},
	watch: !isProduction,
	watchOptions: {
		poll: 1000, // Check for changes every second
	},
	entry: {
		areas: './whg/webpack/js/areas.js',
		base: './whg/webpack/js/base.js',
		'builders-collection-dataset': './whg/webpack/js/builders-collection-dataset.js',
		'builders-collection-place': './whg/webpack/js/builders-collection-place.js',
		'builders-dataset': './whg/webpack/js/builders-dataset.js',
		'builders-dataset-status': './whg/webpack/js/builders-dataset-status.js',
		docs: './whg/webpack/js/docs.js',
		ds_browse: './whg/webpack/js/ds_browse.js',
		gallery: './whg/webpack/js/gallery.js',
		home: './whg/webpack/js/home.js',
		mapAndTable: './whg/webpack/js/mapAndTable.js',
		places: './whg/webpack/js/places.js',
		portal: './whg/webpack/js/portal.js',
		profile: './whg/webpack/js/profile.js',
		reconciliation: './whg/webpack/js/reconciliation.js',
		review: './whg/webpack/js/review.js',
		'wb-place-collection': './whg/webpack/js/wb-place-collection.js',
		'wb-itinerary': './whg/webpack/js/wb-itinerary.js',
		'wb-gazetteer-group': './whg/webpack/js/wb-gazetteer-group.js',
		'wb-place-record': './whg/webpack/js/wb-place-record.js',
		'wb-dataset': './whg/webpack/js/wb-dataset.js',
		'wb-suggest': './whg/webpack/js/wb-suggest.js',
		search: './whg/webpack/js/search.js',
		search_functions: './whg/webpack/js/search_resources.js',
		tasks: './whg/webpack/js/tasks.js',
		whg_maplibre: './whg/webpack/js/whg_maplibre.js',
		workbench: './whg/webpack/js/workbench.js',
	},
	output: {
		filename: '[name].bundle.js', // entry bundles keep stable names (referenced by templates); busted via ?v=
		// Async (lazy-import) chunks are content-hashed so a new deploy serves a new URL — browsers can't
		// serve a stale chunk (this is what previously broke the workbench's lazy recon-* modules after a deploy).
		chunkFilename: '[name].[contenthash].js',
		path: path.resolve(__dirname, 'static/webpack'),
		// Purge superseded hashed chunks each build so committed output doesn't accumulate old hashes.
		// KEEP the hand-committed, non-webpack assets that also live here (demo data + the Symphonym model).
		clean: { keep: (asset) => asset.startsWith('samples/') || asset.startsWith('symphonym/') },
	},
	module: {
		rules: [
			{
				test: /\.css$/,
				use: [MiniCssExtractPlugin.loader, 'css-loader'],
			},
			{
				test: /\.scss$/,
				use: [MiniCssExtractPlugin.loader, 'css-loader', 'sass-loader'],
			},
		    {
		        test: /\.csl$/,
		        use: 'raw-loader', // Use raw-loader for .csl files
		    },
		    {
		        // pdfjs-dist (and other ESM packages) ship .mjs with extensionless internal imports;
		        // relax webpack's strict ESM resolution so the lazy recon-pdfjs chunk builds cleanly.
		        test: /\.m?js$/,
		        resolve: { fullySpecified: false },
		    }
		],
	},
	plugins: [
		new Dotenv({
			path: './.env/.env',
		}),
		new MiniCssExtractPlugin({
			filename: '[name].bundle.css', // entry CSS keeps stable names (referenced by templates); busted via ?v=
			chunkFilename: '[name].[contenthash].css', // async CSS chunks are content-hashed like their JS chunks
		}),
		// Bundle report is opt-in: generating the static HTML report + full stats.json for the whole
		// (~20 MiB) graph on every build added several seconds. Run `npm run analyze` (ANALYZE=1) when
		// you actually want it; normal `npm run build` / `npm run watch` now skip it.
		...(process.env.ANALYZE ? [new BundleAnalyzerPlugin({
			analyzerMode: 'static', // `server` option is very slow
			reportFilename: 'webpackReport.html',
			openAnalyzer: false, // still writes the report, but don't auto-open it (was stealing browser focus)
			generateStatsFile: true,
			statsFilename: 'stats.json',
		})] : []),
		new CopyWebpackPlugin({
	      	patterns: [
		        {
		          from: 'node_modules/jquery/dist/jquery.min.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/bootstrap/dist/js/bootstrap.bundle.min.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/bootstrap/dist/css/bootstrap.min.css',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/jquery-ui/dist/jquery-ui.min.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/jquery-ui/dist/themes/base/jquery-ui.min.css',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/clipboard/dist/clipboard.min.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@fortawesome/fontawesome-free/css/all.min.css',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@turf/turf/turf.min.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/d3/build/d3.min.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/select2/dist/js/select2.full.js',
		          to: 'CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/select2/dist/css/select2.css',
		          to: 'CDNfallbacks/',
		        },
	      	],
	    }),...(isProduction ? [
           sentryWebpackPlugin({
               url: 'https://errors.whgazetteer.org',  // Using GlitchTip rather than Sentry
               authToken: process.env.GLITCHTIP_AUTH_TOKEN,  // Needs 'project:releases' and 'org:read' scopes
               include: path.resolve(__dirname, 'static/webpack'),  // Location of final bundled files and source maps
               thirdPartyErrorFilter: true,  // Crucial for ignoring browser extension errors
               silent: true, // Suppress verbose logs
               org: process.env.GLITCHTIP_ORG || "whgazetteer",
               project: process.env.GLITCHTIP_PROJECT || "website",
               release: getGitHash(),
           }),
       ] : []),
	],
	resolve: {
    	extensions: ['.js', '.xml', '.csl'],
		modules: [
			path.resolve(__dirname, 'static/admin/js/vendor'),
			path.resolve(__dirname, 'node_modules'),
		],
	},
	externals: {
		"jquery": "jQuery",
	},
	optimization: {
		splitChunks: {
			chunks: 'async',
			minSize: 20000,
			minRemainingSize: 0,
			minChunks: 1,
			maxAsyncRequests: 30,
			maxInitialRequests: 30,
			enforceSizeThreshold: 50000,
			cacheGroups: {
				default: {
					minChunks: 2,
					priority: -20,
					reuseExistingChunk: true,
				},
			},
		},
		minimizer: [
			new TerserPlugin({
				terserOptions: {
					format: {
						comments: false,
					},
				},
				extractComments: false,
			}),
			new CssMinimizerPlugin(),
		],
	},
};