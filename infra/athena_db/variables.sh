#!/bin/bash

export ATHENA_WORKGROUP="${RG_NAME}-wk"
export ATHENA_DB="${RG_NAME//-/_}"
export ATHENA_DB_LOCATION="${ATHENA_LAKE_URI}/db/"
export ATHENA_DB_RESULTS="${ATHENA_LAKE_URI}/results/"
export ATHENA_WORKGROUP_RESULT_CONF="ResultConfiguration={OutputLocation=${ATHENA_DB_RESULTS}}"
