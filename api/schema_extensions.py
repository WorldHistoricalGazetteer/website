# api/schema_extensions.py

from drf_spectacular.extensions import OpenApiAuthenticationExtension

from api.authentication import TokenQueryOrBearerAuthentication


class TokenQueryOrBearerAuthenticationScheme(OpenApiAuthenticationExtension):
    target_class = f'{TokenQueryOrBearerAuthentication.__module__}.TokenQueryOrBearerAuthentication'
    name = 'TokenQueryOrBearerAuth'  # internal reference name

    def get_security_definition(self, auto_schema):
        return {
            'BearerAuth': {
                'type': 'http',
                'scheme': 'bearer',
                'bearerFormat': 'Token',
                'description': 'Enter your API token as: Bearer <token>',
            },
            'ApiKeyAuth': {
                'type': 'apiKey',
                'in': 'query',
                'name': 'token',
                'description': 'API token as ?token= query parameter',
            },
        }
