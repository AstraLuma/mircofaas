"""
Settings for hypercorn
"""
import logging

bind = ["localhost:8000"]

logconfig_dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "root": {"level": "DEBUG", "handlers": ["console"]},
    "loggers": {
        # "asyncio": {
        #     "level": "INFO",
        # },
        # "hypercorn.error": {
        #     "level": "INFO",
        #     "handlers": ["console"],
        #     "propagate": True,
        #     "qualname": "hypercorn.error",
        # },
        # "hypercorn.access": {
        #     "level": "INFO",
        #     "handlers": ["console"],
        #     "propagate": True,
        #     "qualname": "hypercorn.access",
        # },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": "ext://sys.stdout",
        },
    },
    "formatters": {
        "generic": {
            "format": "%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
            "datefmt": "[%Y-%m-%d %H:%M:%S]",
            "class": "logging.Formatter",
        }
    },
}

accesslog = logging.getLogger('hypercorn.access')
errorlog = logging.getLogger('hypercorn.error')
