# whg/settings.py

import base64
import os
import sys

from django.contrib.messages import constants as messages

try:
    from .local_settings_autocontext import *
except ImportError:
    print('Error importing from .local_settings_autocontext')
    pass

try:
    from .local_settings import *
except ImportError:
    print('Error importing from .local_settings')
    pass

ENV_CONTEXT = os.environ.get('ENV_CONTEXT', 'dev-whgazetteer-org')  # Default if ENV_CONTEXT is not set

DATABASES['default']['CONN_MAX_AGE'] = 600  # 10 minutes

if 'test' in sys.argv:
    CELERY_ALWAYS_EAGER = True
else:
    CELERY_ALWAYS_EAGER = False

SITE_ID = 1
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.gis',
    'django.contrib.humanize',
    'django.contrib.messages',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.sitemaps',
    'django.contrib.staticfiles',

    # 3rd party
    'bootstrap_modal_forms',
    'celery_progress',
    # uncomment for debug toolbar
    # 'debug_toolbar',
    'django_celery_beat',
    'django_celery_results',
    'django_extensions',
    'django_filters',
    'django_resized',
    'django_tables2',
    'django_user_agents',
    'djgeojson',
    'encrypted_model_fields',
    'guardian',
    'leaflet',
    'mathfilters',
    "mozilla_django_oidc",
    'multiselectfield',
    'rest_framework',
    'rest_framework.authtoken',
    'rest_framework_datatables',
    'rest_framework_gis',
    'drf_spectacular',
    'tinymce',

    # apps
    'accounts.apps.AccountsConfig',
    'api.apps.ApiConfig',
    'areas.apps.AreasConfig',
    'collection.apps.CollectionConfig',  # "collections" (plural) is reserved in python
    'datasets.apps.DatasetsConfig',
    'elastic.apps.ElasticConfig',
    'ingestion.apps.IngestionConfig',
    'main.apps.MainConfig',
    'periods.apps.PeriodsConfig',
    'persons.apps.PersonsConfig',
    'places.apps.PlacesConfig',
    'regions.apps.RegionsConfig',
    'resources.apps.ResourcesConfig',  # for teaching
    'search.apps.SearchConfig',
    'sitemap.apps.SitemapConfig',
    'traces.apps.TracesConfig',
    'users.apps.UsersConfig',
]

AUTH_USER_MODEL = 'users.User'

BLOCKED_USER_AGENT_SUBSTRINGS = [
    # Messaging / social preview bots
    "Slackbot-LinkExpanding",       # Slack link unfurling
    "meta-externalagent",           # Facebook crawler
    "Twitterbot",                   # Twitter card preview
    "Discordbot",                   # Discord link preview
    "WhatsApp",                     # WhatsApp preview
    "TelegramBot",                  # Telegram preview
    "SkypeUriPreview",              # Skype link preview

    # AI / LLM crawlers
    "GPTBot",                       # OpenAI web crawler
    "ChatGPT",                      # General ChatGPT user-agents
    "ClaudeBot",                    # Anthropic's Claude
    "Bard",                         # Google Bard
    "CCBot",                        # Common Crawl
    "Amazonbot",                    # Amazon AI crawler
    "ai-crawler",                   # General AI crawlers

    # SEO and scraping bots
    "AhrefsBot",                    # SEO crawler
    "SemrushBot",                   # SEO tool
    "MJ12bot",                      # Majestic-12 crawler
    "DotBot",                       # Dotbot (SEO)
    "BLEXBot",                      # SEO / performance bot
    "Bytespider",                   # TikTok/ByteDance crawler
    "YandexBot",                    # Russian search engine
    "Baidu",                        # Chinese search engine
    "PetalBot",                     # Huawei crawler
    "DuckDuckBot",                  # DuckDuckGo

    # Misc / suspicious
    "python-requests",             # Basic scraping lib
    "curl",                        # Command-line HTTP tool
    "Go-http-client",              # GoLang scrapers
    "libwww-perl",                 # Old Perl web tools
    "Wget",                        # Another CLI downloader
    "Scrapy",                      # Python scraping framework
    "node-fetch",                  # JS server-side fetch
]

MIDDLEWARE = [
    'django.middleware.gzip.GZipMiddleware',
    # uncomment for debug toolbar
    # 'debug_toolbar.middleware.DebugToolbarMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'api.middleware.PlausibleAPIMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django_user_agents.middleware.UserAgentMiddleware',
    'main.block_user_agents.BlockUserAgentsMiddleware',
]

ROOT_URLCONF = 'whg.urls'

PUBLIC_GROUP_ID = 'review'

TIME_ZONE = 'America/New_York'

MESSAGE_TAGS = {
    messages.DEBUG: 'alert-secondary',
    messages.INFO: 'alert-info',
    messages.SUCCESS: 'alert-success',
    messages.WARNING: 'alert-warning',
    messages.ERROR: 'alert-danger',
}

CELERY_RESULT_BACKEND = 'django-db'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = TIME_ZONE
# trying to throw error if no worker available
CELERY_TASK_EAGER_PROPAGATES = True
# required per https://github.com/celery/django-celery-results/issues/334
CELERY_RESULT_EXTENDED = True
CELERY_RESULT_EXPIRES = None
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

DJANGORESIZED_DEFAULT_SIZE = [1000, 800]
DJANGORESIZED_DEFAULT_QUALITY = 75
DJANGORESIZED_DEFAULT_KEEP_META = True
DJANGORESIZED_DEFAULT_FORCE_FORMAT = 'JPEG'
DJANGORESIZED_DEFAULT_FORMAT_EXTENSIONS = {'JPEG': ".jpg"}
DJANGORESIZED_DEFAULT_NORMALIZE_ROTATION = True

# replacement section from drf-datatables
# https://django-rest-framework-datatables.readthedocs.io/en/latest/
REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
        # Any additional renderers will appear as unwanted options in the Swagger UI
        # 'rest_framework_datatables.renderers.DatatablesRenderer',
    ),
    'DEFAULT_FILTER_BACKENDS': (
        'rest_framework_datatables.filters.DatatablesFilterBackend',
        # 'django_filters.rest_framework.DjangoFilterBackend'
    ),
    'DEFAULT_PAGINATION_CLASS': 'rest_framework_datatables.pagination.DatatablesPageNumberPagination',
    'PAGE_SIZE': 15000,
    'PAGE_SIZE': 20,
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
}

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'main/templates'),
            os.path.join(BASE_DIR, 'whgmail/templates'),
            os.path.join(BASE_DIR, 'templates'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'debug': True,
            'context_processors': [
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'django.template.context_processors.debug',
                'django.template.context_processors.media',
                'django.template.context_processors.request',
                'whg.context_processors.environment',
            ],
            'builtins': [
                'whg.builtins',
            ]
        },
    },
]

WSGI_APPLICATION = 'whg.wsgi.application'

LOGGING_LEVELS = {
    'dev-whgazetteer-org': 'DEBUG',
    'whgazetteer-org': 'DEBUG',
}
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'django_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/django.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 5,  # Number of backup files to keep
            'formatter': 'verbose',
        },
        'celery_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/celery.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 5,  # Number of backup files to keep
            'formatter': 'verbose',
        },
        'root_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/root.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 5,  # Number of backup files to keep
            'formatter': 'verbose',
        },
        'messaging_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/messaging.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 5,  # Number of backup files to keep
            'formatter': 'verbose',
        },
        'authentication_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/authentication.log'),
            'maxBytes': 10485760,  # 10 MB
            'backupCount': 5,  # Number of backup files to keep
            'formatter': 'verbose',
        },
        'mapdata_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/mapdata.log'),
            'maxBytes': 10485760,
            'backupCount': 5,
            'formatter': 'verbose',
        },
        'validation_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/validation.log'),
            'maxBytes': 10485760,
            'backupCount': 5,
            'formatter': 'verbose',
        },
        'reconciliation_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/reconciliation.log'),
            'maxBytes': 10485760,
            'backupCount': 5,
            'formatter': 'verbose',
        },
        'accession_file': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'whg/logs/accession.log'),
            'maxBytes': 10485760,
            'backupCount': 5,
            'formatter': 'verbose',
        },
        "email_access_file": {
            "level": "INFO",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(BASE_DIR, 'whg/logs/email_access.log'),
            "maxBytes": 10485760,
            "backupCount": 5,
            "formatter": "verbose",
        },
        'console': {
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
    },
    'loggers': {
        'django': {
            'handlers': ['django_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,  # Ensure logs do not propagate to root logger
        },
        'celery': {
            'handlers': ['celery_file', 'console'],
            'level': 'INFO',
            'propagate': False,
        },
        'celery.task': {
            'handlers': ['celery_file', 'console'],
            'level': 'INFO',
            'propagate': False,
        },
        'celery.app.trace': {
            'handlers': ['celery_file', 'console'],
            'level': 'WARNING',
            'propagate': False,
        },
        'tasks': {
            'handlers': ['celery_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,  # Ensure logs do not propagate to root logger
        },
        'messaging': {
            'handlers': ['messaging_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,  # Ensure logs do not propagate to root logger
        },
        'authentication': {
            'handlers': ['authentication_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,  # Ensure logs do not propagate to root logger
        },
        'mapdata': {
            'handlers': ['mapdata_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,
        },
        'validation': {
            'handlers': ['validation_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,
        },
        'reconciliation': {
            'handlers': ['reconciliation_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,
        },
        'accession': {
            'handlers': ['accession_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': False,
        },
        "email_access": {
            "handlers": ["email_access_file"],
            "level": "INFO",
            "propagate": False,
        },
        '': {  # Root logger
            'handlers': ['root_file', 'console'],
            'level': LOGGING_LEVELS.get(ENV_CONTEXT, 'DEBUG'),
            'propagate': True,
        },
    },
}

SESSION_COOKIE_AGE = 1209600  # Two weeks, in seconds
SESSION_EXPIRE_AT_BROWSER_CLOSE = False

GDAL_LIBRARY_PATH = '/usr/lib/libgdal.so.28'
GEOS_LIBRARY_PATH = '/usr/lib/x86_64-linux-gnu/libgeos_c.so.1'

SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')  # Needs to be set for proper request.build_absolute_uri handling

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',  # TODO: Remove this after ORCiD migration
    'guardian.backends.ObjectPermissionBackend',
    'accounts.orcid.OIDCBackend',
)

OIDC_RP_SCOPES = "openid"  # ORCiD base url and credentials must be set in `env_template.py`
ORCID_BASE_URL = os.environ.get("ORCID_BASE")
OIDC_RP_CLIENT_ID = os.environ.get("ORCID_CLIENT_ID")
OIDC_RP_CLIENT_SECRET = os.environ.get("ORCID_CLIENT_SECRET")
OIDC_OP_DISCOVERY_ENDPOINT = f"{ORCID_BASE_URL}/.well-known/openid-configuration"
OIDC_OP_AUTHORIZATION_ENDPOINT = f"{ORCID_BASE_URL}/oauth/authorize"
OIDC_OP_TOKEN_ENDPOINT = f"{ORCID_BASE_URL}/oauth/token"
OIDC_OP_USER_ENDPOINT = f"{ORCID_BASE_URL}/oauth/userinfo"

LOGIN_URL = '/accounts/login/'
LOGIN_REDIRECT_URL = '/'
LOGOUT_REDIRECT_URL = '/'

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator', },
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator', },
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator', },
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator', },
]

# Internationalization
# https://docs.djangoproject.com/en/2.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True
USE_L10N = True
USE_THOUSAND_SEPARATOR = True

MEDIA_ROOT = os.path.join(BASE_DIR, 'media/')
MEDIA_URL = '/media/'

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/deployment/
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'datasets', 'static'),
    os.path.join(BASE_DIR, 'main', 'static'),
    os.path.join(BASE_DIR, 'validation', 'static'),
    os.path.join(BASE_DIR, 'whg', 'static'),
    # webpack.config now writes directly to static root /webpack
]

CACHES = {
    'default': {
        'BACKEND': 'utils.mapdata.MapdataFileBasedCache',
        'LOCATION': os.path.join(BASE_DIR, 'cache'),
        'TIMEOUT': None,  # Cache data indefinitely until manually updated
        "OPTIONS": {"MAX_ENTRIES": 10000},  # Increase from default of 300
    },
    'sitemap_cache': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': os.path.join(BASE_DIR, 'sitemap_cache'),
        'TIMEOUT': 3600,  # Cache sitemap for 1 hour (3600 seconds)
        'OPTIONS': {
            'MAX_ENTRIES': 5000  # Configure based on expected sitemap entries
        },
    },
    'remote_datasets': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': os.path.join(BASE_DIR, 'remote_datasets_cache'),
        'TIMEOUT': 2678400,  # Cache for 31 days
        'OPTIONS': {
            'MAX_ENTRIES': 1000,
        },
    },
    'property_cache': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': 'redis://redis:6379/1',  # 0 is used by Celery
        'TIMEOUT': 604800,  # Default cache timeout (1 week)
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
        },
    },
}

SITEMAP_CACHE = 'sitemap_cache'

## GIS Libraries
GDAL_LIBRARY_PATH = '/usr/lib/libgdal.so.28'
GEOS_LIBRARY_PATH = '/usr/lib/x86_64-linux-gnu/libgeos_c.so.1'

SPECTACULAR_SETTINGS = {
    'TITLE': 'WHG API',
    'DESCRIPTION': (
        "The World Historical Gazetteer (WHG) API provides programmatic access to place data, "
        "supporting discovery, reconciliation, and integration of historical geographic information. "
        "It implements both general REST endpoints for querying WHG resources and a full "
        "[Reconciliation Service API v0.2](https://www.w3.org/community/reports/reconciliation/CG-FINAL-specs-0.2-20230410/) "
        "for linking external datasets against WHG identifiers.\n\n"
        "Main features include:\n"
        "- **Place & Period Search & Retrieval**: access detailed information about places or periods, their names, "
        "types, geometries, temporal coverage, and datasets of origin.\n"
        "- **Reconciliation API**: resolve place and period names to WHG or [PeriodO](https://perio.do/) identifiers, with canonical labels and metadata.\n"
        "- **Extension Service**: enrich external records with WHG or PeriodO properties (e.g. country codes, "
        "feature classes, geometries, and temporal ranges).\n\n"
        "Most endpoints require a valid API token for authentication, which can be submitted either "
        "as a URL query parameter or in an Authorization header. "
        "See the [documentation](https://docs.whgazetteer.org/content/400-Technical.html#api-tokens) for details."
    ),
    'TAGS': [
        {"name": "Schema", "description": "API schema and documentation"},
        {"name": "Entity API", "description": "Query (_in some cases modify_) **Places**, **Periods**, **Datasets**, **Collections**, or **Areas**"},
        {"name": "Reconciliation API", "description": "for **Places** and **Periods** ([Reconciliation Service API v0.2](https://www.w3.org/community/reports/reconciliation/CG-FINAL-specs-0.2-20230410/))"},
    ],
    'VERSION': '1.0.0',
    'SWAGGER_UI_SETTINGS': {
        'deepLinking': True,
        'persistAuthorization': True,
    },
    "SWAGGER_UI_FAVICON_HREF": "/static/images/favicon.ico",
    'SORT_OPERATION_PARAMETERS': False,
    'PREPROCESSING_HOOKS': [
        'drf_spectacular.hooks.preprocess_exclude_path_format',
    ],
    'AUTHENTICATION_WHITELIST': [],
    'SECURITY': [],
}

PLAUSIBLE_BASE_URL="https://analytics.whgazetteer.org"  # TODO: Change this to network IP once co-located within K8s
PLAUSIBLE_SITE="whgazetteer.org"
# PLAUSIBLE_API_KEY is in local_settings

# Settings for DataCite API (DOI registration)
DOI_USER_ID = os.environ.get("DOI_USER_ID")
DOI_PASSWORD = os.environ.get("DOI_PASSWORD")
DOI_PREFIX = os.environ.get("DOI_PREFIX")
DOI_API_URL = f"https://api{'' if os.environ.get('ENV_CONTEXT') == 'whgazetteer-org' else '.test'}.datacite.org/dois"
DOI_ENCODED_CREDENTIALS = base64.b64encode(f"{DOI_USER_ID}:{DOI_PASSWORD}".encode('utf-8')).decode('utf-8')

# Page-specific settings
DATASETS_PLACES_LIMIT = 100000

# Remote Dataset Configurations
REMOTE_DATASET_CONFIGS = [
    {  # 2024: 37k+ places
        'dataset_name': 'Pleiades',
        'namespace': 'pleiades',
        'api_item': 'https://pleiades.stoa.org/places/<id>/json',
        'citation': 'Pleiades: A community-built gazetteer and graph of ancient places. Copyright © Institute for the Study of the Ancient World. Sharing and remixing permitted under terms of the Creative Commons Attribution 3.0 License (cc-by). https://pleiades.stoa.org/',
        'files': [
            {
                'url': 'https://atlantides.org/downloads/pleiades/json/pleiades-places-latest.json.gz',
                'file_type': 'json',
                'item_path': '@graph.item',
            }
        ],
    },
    {  # 2024: 12m+ places
        'dataset_name': 'GeoNames',
        'namespace': 'gn',
        'api_item': 'http://api.geonames.org/getJSON?formatted=true&geonameId=<id>&username=<username>&style=full',
        'citation': 'GeoNames geographical database. https://www.geonames.org/',
        'files': [
            {
                'url': 'https://download.geonames.org/export/dump/allCountries.zip',
                'fieldnames': [
                    'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude', 'feature_class',
                    'feature_code', 'country_code', 'cc2', 'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code',
                    'population', 'elevation', 'dem', 'timezone', 'modification_date',
                ],
                'file_name': 'allCountries.txt',
                'file_type': 'csv',
                'delimiter': '\t',
            },
            {
                'url': 'https://download.geonames.org/export/dump/alternateNamesV2.zip',
                'fieldnames': [
                    'alternateNameId', 'geonameid', 'isolanguage', 'alternate_name', 'isPreferredName',
                    'isShortName', 'isColloquial', 'isHistoric', 'from', 'to',
                ],
                'file_name': 'alternateNamesV2.txt',  # Zip file also includes iso-languagecodes.txt
                'file_type': 'csv',
                'delimiter': '\t',
            },
        ],
    },
    {  # 2024: 3m+ places
        'dataset_name': 'TGN',
        'namespace': 'tgn',
        'api_item': 'https://vocab.getty.edu/tgn/<id>.jsonld',
        'citation': 'The Getty Thesaurus of Geographic Names® (TGN) is provided by the J. Paul Getty Trust under the Open Data Commons Attribution License (ODC-By) 1.0. https://www.getty.edu/research/tools/vocabularies/tgn/',
        'files': [
            {
                'url': 'http://tgndownloads.getty.edu/VocabData/full.zip',
                'file_name': 'TGNOut_Full.nt',
                'file_type': 'nt',
                'filter': [  # Filter to only include records with these predicates (examples of each given below)
                    '<http://vocab.getty.edu/ontology#parentString>',
                    # <http://vocab.getty.edu/tgn/7011179> <http://vocab.getty.edu/ontology#parentString> "Siena, Tuscany, Italy, Europe, World"
                    '<http://vocab.getty.edu/ontology#prefLabelGVP>',
                    # '<http://vocab.getty.edu/tgn/7011179> <http://vocab.getty.edu/ontology#prefLabelGVP> <http://vocab.getty.edu/tgn/term/47413-en>
                    '<http://www.w3.org/2008/05/skos-xl#prefLabel>',
                    # <http://vocab.getty.edu/tgn/7011179> <http://www.w3.org/2008/05/skos-xl#prefLabel> <http://vocab.getty.edu/tgn/term/47413-en>
                    '<http://www.w3.org/2008/05/skos-xl#altLabel>',
                    # <http://vocab.getty.edu/tgn/7011179> <http://www.w3.org/2008/05/skos-xl#altLabel> <http://vocab.getty.edu/tgn/term/140808-en>
                    '<http://vocab.getty.edu/ontology#term>',
                    # <http://vocab.getty.edu/tgn/term/47413-en> <http://vocab.getty.edu/ontology#term> "Siena"@en
                    '<http://vocab.getty.edu/ontology#estStart>',
                    # <http://vocab.getty.edu/tgn/term/47413-en> <http://vocab.getty.edu/ontology#estStart> "1200"^^<http://www.w3.org/2001/XMLSchema#gYear>
                    '<http://schema.org/longitude>',
                    # <http://vocab.getty.edu/tgn/7011179-geometry> <http://schema.org/longitude> "11.33"^^<http://www.w3.org/2001/XMLSchema#decimal>
                    '<http://schema.org/latitude>',
                    # <http://vocab.getty.edu/tgn/7011179-geometry> <http://schema.org/latitude> "43.318"^^<http://www.w3.org/2001/XMLSchema#decimal>
                    '<http://vocab.getty.edu/ontology#placeType>',
                    # <http://vocab.getty.edu/tgn/7011179> <http://vocab.getty.edu/ontology#placeType> <http://vocab.getty.edu/aat/300387236>
                ],
            },
        ],
    },
    {  # 2024: 8m+ items classified as places
        'dataset_name': 'Wikidata',
        'namespace': 'wd',
        'api_item': 'https://www.wikidata.org/wiki/Special:EntityData/<id>.json',
        'citation': 'Wikidata is a free and open knowledge base that can be read and edited by both humans and machines. https://www.wikidata.org/',
        'files': [
            {
                'url': 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.gz',
                'file_type': 'json',
                'item_path': 'entities',
            },
        ],
    },
    {  # 2024: 6m+ nodes tagged as places
        'dataset_name': 'OSM',
        'namespace': 'osm',
        'api_item': 'https://nominatim.openstreetmap.org/details.php?osmtype=R&osmid=<id>&format=json',
        'citation': 'OpenStreetMap is open data, licensed under the Open Data Commons Open Database License (ODbL). https://www.openstreetmap.org/',
        'files': [
            {
                'url': 'https://planet.openstreetmap.org/planet/planet-latest.osm.bz2',
                'file_type': 'xml',
            }
        ],
    },
    {
        'dataset_name': 'LOC',
        'namespace': 'loc',
        'api_item': 'https://www.loc.gov/item/<id>/',
        'citation': 'Library of Congress. https://www.loc.gov/',
        'files': [
            {
                'url': 'http://id.loc.gov/download/authorities/names.madsrdf.jsonld.gz',
                'file_type': 'json',
            }
        ],
    },
    {
        'dataset_name': 'GB1900',
        'namespace': 'GB1900',
        'api_item': '',
        'citation': 'GB1900 Gazetteer: British place names, 1888-1914. https://www.pastplace.org/data/#tabgb1900',
        'files': [
            {
                'url': 'https://www.pastplace.org/downloads/GB1900_gazetteer_abridged_july_2018.zip',
                'file_type': 'csv',
                'delimiter': ',',
            }
        ],
    },
    {  # 24,000 place names
        'dataset_name': 'IndexVillaris',
        'namespace': 'IV1680',
        'api_item': '',
        'citation': 'Index Villaris, 1680',
        'files': [
            {
                'url': 'https://github.com/docuracy/IndexVillaris1680/raw/refs/heads/main/docs/data/IV-GB1900-OSM-WD.lp.json',
                'file_type': 'json',
            }
        ],
    },
]

# Dataset Validation
LPF_SCHEMA_PATH = os.path.join(BASE_DIR, 'validation/static/lpf_v2.0.jsonld')
LPF_CONTEXT_PATH = os.path.join(BASE_DIR, 'validation/static/lpo_v2.0.jsonld')
VALIDATION_ALLOWED_EXTENSIONS = ['.csv', '.tsv', '.xlsx', '.ods', '.jsonld', '.geojson', '.json']
VALIDATION_ALLOWED_ENCODINGS = ['ascii', 'us-ascii', 'utf-8']
VALIDATION_SUPPORTED_TYPES = [
    'application/json',
    'text/plain',
    'text/csv',
    'text/tab-separated-values',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.oasis.opendocument.spreadsheet'
]
VALIDATION_CHUNK_ROWS = 500
VALIDATION_BATCH_MEMORY_LIMIT = 1 * 1024 * 1024  # 1 MB
VALIDATION_MAXFIXATTEMPTS = 50  # Maximum number of errors to try to fix on each feature
VALIDATION_MAX_ERRORS = 100  # Stop validation of dataset if this number of unfixed errors is reached (checked only on completion of each batch, so may exceed this number)
VALIDATION_TIMEOUT = 3600  # seconds, after which tasks are revoked and records are removed from redis
VALIDATION_TEST_DELAY = 0  # seconds to pause after each JSON schema validation attempt
VALIDATION_INTEGRITY_RETRIES = 7
