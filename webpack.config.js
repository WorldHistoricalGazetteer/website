// /webpack.config.js

const path = require('path');
const CssMinimizerPlugin = require('css-minimizer-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const TerserPlugin = require('terser-webpack-plugin');
const Dotenv = require('dotenv-webpack');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer').BundleAnalyzerPlugin;
const CopyWebpackPlugin = require('copy-webpack-plugin');

const isProduction = process.env.ENV_CONTEXT === 'whgazetteer-org';

module.exports = {
	mode: isProduction ? 'production' : 'development', // Use production mode for staging
	watch: true,
	watchOptions: {
		poll: 1000, // Check for changes every second
	},
	entry: {
		areas: '/app/whg/webpack/js/areas.js',
		base: '/app/whg/webpack/js/base.js',
		'builders-collection-dataset': '/app/whg/webpack/js/builders-collection-dataset.js',
		'builders-collection-place': '/app/whg/webpack/js/builders-collection-place.js',
		'builders-dataset': '/app/whg/webpack/js/builders-dataset.js',
		'builders-dataset-status': '/app/whg/webpack/js/builders-dataset-status.js',
		docs: '/app/whg/webpack/js/docs.js',
		ds_browse: '/app/whg/webpack/js/ds_browse.js',
		gallery: '/app/whg/webpack/js/gallery.js',
		home: '/app/whg/webpack/js/home.js',
		mapAndTable: '/app/whg/webpack/js/mapAndTable.js',
		places: '/app/whg/webpack/js/places.js',
		portal: '/app/whg/webpack/js/portal.js',
		profile: '/app/whg/webpack/js/profile.js',
		review: '/app/whg/webpack/js/review.js',
		search: '/app/whg/webpack/js/search.js',
		search_functions: '/app/whg/webpack/js/search_resources.js',
		tasks: '/app/whg/webpack/js/tasks.js',
		whg_maplibre: '/app/whg/webpack/js/whg_maplibre.js',
		workbench: '/app/whg/webpack/js/workbench.js',
	},
	output: {
		filename: '[name].bundle.js',
		path: path.resolve(__dirname, 'static/webpack'),
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
		    }
		],
	},
	plugins: [
		new Dotenv({
			path: '/app/.env/.env',
		}),
		new MiniCssExtractPlugin({
			filename: '[name].bundle.css',
		}),
		new BundleAnalyzerPlugin({
			analyzerMode: 'static', // `server` option is very slow
			reportFilename: 'webpackReport.html',
			generateStatsFile: true,
			statsFilename: 'stats.json',
		}),
		new CopyWebpackPlugin({
	      	patterns: [
		        {
		          from: 'node_modules/jquery/dist/jquery.min.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/bootstrap/dist/js/bootstrap.bundle.min.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/bootstrap/dist/css/bootstrap.min.css',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/jquery-ui/dist/jquery-ui.min.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/jquery-ui/dist/themes/base/jquery-ui.min.css',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/clipboard/dist/clipboard.min.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@fortawesome/fontawesome-free/css/all.min.css',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/twitter-typeahead-components-bundle/main.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/@turf/turf/turf.min.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/d3/build/d3.min.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/select2/dist/js/select2.full.js',
		          to: '/app/CDNfallbacks/',
		        },
		        {
		          from: 'node_modules/select2/dist/css/select2.css',
		          to: '/app/CDNfallbacks/',
		        },
	      	],
	    }),
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