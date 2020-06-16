from quart import Quart

from .config_app import blueprint as config_blueprint

app = Quart(__name__)
app.register_blueprint(config_blueprint)

@app.route("/")
def healthcheck():
    return {"ok":"yes"}
