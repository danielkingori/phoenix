#!/bin/bash

STR=`aws athena get-work-group --work-group $ATHENA_WORKGROUP --query 'WorkGroup.Name' --output text` &> /dev/null
if [ $? == 0 ]; then
    echo "${ATHENA_WORKGROUP} athena workgroup exists will no be created."
else
    echo "Creating athena workgroup with name ${ATHENA_WORKGROUP}."
    aws athena create-work-group \
        --name $ATHENA_WORKGROUP \
        --configuration $ATHENA_WORKGROUP_RESULT_CONF \
        --tags $RG_TAG
fi


q="CREATE DATABASE IF NOT EXISTS ${ATHENA_DB} LOCATION '${ATHENA_DB_LOCATION}'"

echo "Query to run $q"
aws athena start-query-execution \
  --query-string "$q" \
  --work-group ${ATHENA_WORKGROUP}
