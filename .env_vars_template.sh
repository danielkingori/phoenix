#!/bin/bash

# This file can be used by:
# - making a copy to `.env` so that it (and its secrets) are out of version control
# - filling in the variables that are needed
# - running `source .env` to export all the variables into the current session
# - starting up a docker or a jupyter server


# CrowdTangle auth token
export CROWDTANGLE_API_TOKEN=""
export CT_RATE_LIMIT_CALLS=6
# This needs to be refactored in to tenants.yaml
export CROWDTANGLE_SCRAPE_LIST_ID=""

# Twitter auth credentials
export TWITTER_CONSUMER_KEY=""
export TWITTER_CONSUMER_SECRET=""
export TWITTER_OAUTH_ACCESS_TOKEN=""
export TWITTER_OAUTH_ACCESS_SECRET=""

# YouTube auth token
export YOUTUBE_API_KEY=""

# Developer specific AWS credentials
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export AWS_DEFAULT_REGION=""

# GCP Service Account credentials location
export GOOGLE_APPLICATION_CREDENTIALS=""

# Production storage bucket URLs
export PRODUCTION_ARTIFACTS_URL_PREFIX=""
# Optional:
# export PRODUCTION_DASHBOARD_URL_PREFIX=""

# Dask cluster
export DASK_CLUSTER_IP=tcp://127.0.0.1:8786
