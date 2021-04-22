#!/bin/bash


ALLBUCKETS=`aws s3 ls s3://`
echo $ALLBUCKETS | grep $TABLE_LAKE_NAME &> /dev/null
if [ $? == 0 ]; then
  echo "$TABLE_LAKE_NAME bucket exists will not be created"
else
  echo "Creating bucket $TABLE_LAKE_URI"
  aws s3 mb $TABLE_LAKE_URI
  aws s3api put-bucket-tagging --bucket $TABLE_LAKE_NAME --tagging TagSet=[{$RG_TAG}]
fi

echo $ALLBUCKETS | grep $ATHENA_LAKE_NAME &> /dev/null
if [ $? == 0 ]; then
  echo "$ATHENA_LAKE_NAME bucket exists will not be created"
else
  echo "Creating bucket $ATHENA_LAKE_URI"
  aws s3 mb $ATHENA_LAKE_URI
  aws s3api put-bucket-tagging --bucket $ATHENA_LAKE_NAME --tagging TagSet=[{$RG_TAG}]
fi
