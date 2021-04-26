#!/bin/bash

export TABLE_LAKE_NAME="${RG_NAME}-us-tables"
export TABLE_LAKE_URI="s3://${TABLE_LAKE_NAME}"

export ATHENA_LAKE_NAME="${RG_NAME}-us-athena"
export ATHENA_LAKE_URI="s3://${ATHENA_LAKE_NAME}"
