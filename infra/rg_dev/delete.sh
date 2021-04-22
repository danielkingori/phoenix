#!/bin/bash

STR=`aws resource-groups get-group --group-name $RG_NAME --query 'Group.Name' --output text`
echo $STR &> /dev/null 2>&1
if [ $? == 0 ]; then
  echo "You will need to delete all the resources by hand:"
  
  RG=`aws resource-groups list-group-resources --group-name $RG_NAME`
  echo $RG
  echo "Delete group ${RG_NAME}."
  aws resource-groups delete-group --group-name $RG_NAME
else
  echo "No resource group ${RG_NAME}."
fi
