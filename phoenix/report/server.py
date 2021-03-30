"""Dash - app and server for reporting."""
import dash


# TODO stylesheet needs to be reconsidered
external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Expose server object for gunicorn to serve
server = app.server
