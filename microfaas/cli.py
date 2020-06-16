import asyncio
import os
import sys

import click
import hypercorn
import uvloop

from . import app


@click.group()
def cli():
    """CLI Interface to microfaas"""


@cli.command('serve')
def serve():
    """
    Serve the application
    """
    # Handle trampolining into buildah unshare
    if '_CONTAINERS_USERNS_CONFIGURED' in os.environ:
        # We're inside buildah, start hypercorn
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        import microfaas.hypercorn_config
        config = hypercorn.Config.from_object(microfaas.hypercorn_config)

        asyncio.run(hypercorn.asyncio.serve(app, config))
    else:
        # Trampoline into buildah
        os.execvp('buildah', ['buildah', 'unshare', *sys.argv])
