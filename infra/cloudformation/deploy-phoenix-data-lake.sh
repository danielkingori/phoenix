#!/bin/bash

########
# Copyright: BuildUp 2021
# Usage: source deploy-phoenix-data-lake.sh <env_type>
# Example: source deploy-phoenix-data-lake.sh dev
########

# Check if argument passed
if [ $# -eq 0 ]; then
    echo "No arguments provided"
    return 0
fi

# Parse passed argument
ENV_TYPE=$1

if [[ "$ENV_TYPE" =~ ^(dev|uat|prod)$ ]]; then
    # AWS CLI Call
    echo "Creating $ENV_TYPE phoenix-data-lake stack"

    aws cloudformation deploy \
        --template-file infra/cloudformation/templates/phoenix-data-lake.yml \
        --stack-name phoenix-data-lake-stack-$ENV_TYPE \
        --parameter-overrides EnvType=$ENV_TYPE

    echo "Stack deploy complete"
else
    # Error out
    echo "$ENV_TYPE is not a valid env type"
    return 0
fi
