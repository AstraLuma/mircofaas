import logging

from quart import Quart, current_app

from .config_app import blueprint as config_blueprint
from .manager import Manager


LOG = logging.getLogger(__name__)

app = Quart(__name__)
app.register_blueprint(config_blueprint)


@app.before_serving
async def create_manager():
    print("create_manager")
    LOG.info("Starting containers")
    current_app.rt_man = Manager()
    await current_app.rt_man.__aenter__()
    print("manager started", flush=True)


@app.after_serving
async def cleanup_manager():
    print("cleanup_manager", flush=True)
    # We shouldn't be serving any more requests, so it's safe to join
    LOG.info("Waiting on queues")
    await current_app.rt_man.join()
    LOG.info("Cleaning up containers")
    await current_app.rt_man.__aexit__(None, None, None)
    current_app.rt_man = None


@app.route("/")
def healthcheck():
    # return {"ok":"yes"}
    return {"bundles": list(current_app.rt_man)}
