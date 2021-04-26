#!/bin/bash

STR=`aws resource-groups get-group --group-name $RG_NAME --query 'Group.Name' --output text` &> /dev/null
if [ $? == 0 ]; then
    echo "${RG_NAME} resource-group exists will not be created."
else    
    echo "Creating resource-group with name ${RG_NAME}."
    aws resource-groups create-group \
        --name $RG_NAME \
        --resource-query '{"Type":"TAG_FILTERS_1_0","Query":"{\"ResourceTypeFilters\":[\"AWS::AllSupported\"],\"TagFilters\":[{\"Key\":\"{$RG_KEY\",\"Values\":[\"$RG_VALUE\"]}]}"}'
fi


