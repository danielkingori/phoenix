# Infrastructure
This folder contains scripts that can be used to initialise infrastructure.

## Setup
You will need to install the aws-cli:
https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

Then configure your aws cli to use your aws IAM user credentials (access key id and secret access key).


## Run
When running scripts you:
- need to run scripts from this directory
- always source the resource group variables and then run the resource group `provision.sh` script
- then source the variables of the resource you want (`source <resource_dir}/variables.sh`)
- then run the `provision.sh` script for that resource (`./<resource_dir}/provision.sh`)
- if you want to delete run `delete.sh` for that resource (`./<resource_dir}/delete.sh`)


### E.g.
```
source ./rg_dev/variables.sh
./rg_dev/provision.sh
source ./lakes/variables.sh
./lakes/provision.sh
```

## Ways of working
The running of these scripts should be done with caution. Many of the delete script will not ask before
deleting resources and data.

In general these scripts should document the resources that have been provisions and make it easier to change
the names and properties of resources.

If there is more time these script should be moved into terraform code.

## Notes:
When you create a new script use ".sh" file extension and run the command:
`find ./ -type f -iname "*.sh" -exec chmod +x {} \;` to make it executable.
