#!/usr/bin/env python

"""Dash - run the app. `python phoenix/report/run.py`."""

import logging
import sys

from phoenix.report.app import app


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.debug("App logger running as '__main__'")
else:
    logger = logging.getLogger(__name__)
    logger.debug("App logger running as __name__")

logger.handlers = []  # Dash naughtily adds a handler to __name__ logger, so remove it!

# Note, this is only called when running as `python phoenix/report/run.py`, and isn't called
# in production when serving app workers via Gunicorn. So fine to hardcode dev
# related config here, e.g. `debug=True` and dev logging setup.
if __name__ == "__main__":
    # dev logging setup - assumes logger is root logger from `logging.getLogger()`
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - pid:%(process)d - "
        "[%(pathname)s:%(lineno)d] - %(name)s - "
        "%(levelname)s ::: %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.debug("Hello World from logging")

    app.run_server(
        host="0.0.0.0", port=8050, debug=True, dev_tools_props_check=True, dev_tools_ui=True
    )
