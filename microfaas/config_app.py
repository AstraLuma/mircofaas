"""
Quart app for managing things.
"""
from quart import Blueprint, current_app

blueprint = Blueprint('config', __name__)

@blueprint.route("/<slug>", methods=["POST"])
def deploy(slug):
    """
    Deploy bundle
    """
    raise NotImplementedError
