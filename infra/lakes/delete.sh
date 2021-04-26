#!/bin/bash

ALLBUCKETS=`aws s3 ls s3://`
echo $ALLBUCKETS | grep $TABLE_LAKE_NAME &> /dev/null
if [ $? == 0 ]; then
  echo "$TABLE_LAKE_NAME bucket exists will delete"
  aws s3 rb $TABLE_LAKE_URI --force
else
  echo "$TABLE_LAKE_URI does not exist."
fi

echo $ALLBUCKETS | grep $ATHENA_LAKE_NAME &> /dev/null
if [ $? == 0 ]; then
  echo "$ATHENA_LAKE_NAME bucket exists will delete"
  aws s3 rb $ATHENA_LAKE_URI --force
else
  echo "$ATHENA_LAKE_URI does not exist."
fi
