#!/bin/bash

aws athena start-query-execution \
  --query-string "DROP DATABASE IF EXISTS ${ATHENA_DB}" \
  --work-group ${ATHENA_WORKGROUP}

STR=`aws athena get-work-group --work-group $ATHENA_WORKGROUP --query 'WorkGroup.Name' --output text` &> /dev/null
if [ $? == 0 ]; then
    echo "${ATHENA_WORKGROUP} athena workgroup exists deleting."
    aws athena delete-work-group \
        --work-group $ATHENA_WORKGROUP \
        --recursive-delete-option
else
    echo "There is no athena workgroup ${ATHENA_WORKGROUP}."
fi


