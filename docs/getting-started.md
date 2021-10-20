# Getting Started

## Good to read
Before you start it is recommended that you read the documentation:
- [Project Structure and adding a new project](/docs/project-structure.md)

## Setup your development env
It is recommended that you use some sort of vitualisation to setup your development environment.
This repo has two options that are already set up: docker and virturalenv.

See [development.md](/docs/development.md) for more information.

## Getting Google Drive API access
When first starting a new project from scratch, you'll need to run through the following steps:

###Get a Google service account
Go to a [Google cloud console](https://console.cloud.google.com/)
 - go to APIs and services -> credentials -> create credentials
 - Create a Service Account (not an API key or an OAuth client ID)
    - Download its json credentials.
 - Go to `Google sheets API` and `Google Drive API` on the google cloud platform and activate
  both of them. 

 
Go to [Google drive](https://drive.google.com/) 
Give the service account edit access to g drive folder as if it were another user.
The email address of the account will have the format:
`{service_account_name}.{service_account_id}.iam.gserviceaccount.com`

Add the Service Account json credentials to phoenix folder (see .gitignore what the filename
 should be)