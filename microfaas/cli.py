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

def _run_hypercorn(**additional_config):
    # Handle trampolining into buildah unshare
    if '_CONTAINERS_USERNS_CONFIGURED' in os.environ:
        # We're inside buildah, start hypercorn
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        import microfaas.hypercorn_config
        config = hypercorn.Config.from_object(microfaas.hypercorn_config)

        for k, v in additional_config.items():
            setattr(config, k, v)

        asyncio.run(hypercorn.asyncio.serve(app, config))
    else:
        # Trampoline into buildah
        os.execvp('buildah', ['buildah', 'unshare', *sys.argv])

@cli.command()
def serve():
    """
    Serve the application
    """
    _run_hypercorn()


@cli.command()
def dev():
    """
    Serve the application (debug config)
    """
    _run_hypercorn(use_reloader=True)
