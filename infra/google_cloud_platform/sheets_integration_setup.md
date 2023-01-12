# Google Drive+Sheets integration setup

In specific pipelines within Phoenix, where external human users need to interact with and
contribute to data that Phoenix pipelines then use, we use Google Sheets for persiting/getting data
to and from a source - Sheets - which also enables effective human interface and interaction with
the data.

We use a Service Account user within a Google Cloud Platform (GCP) project to enable Phoenix
systems automated interaction with Google Drive and Sheets.

## Setup

The following steps need to be taken manually to set this up:
- Create a GCP project
- Enable following APIs on GCP project
    - Drive
    - Sheet
- Create a Service Account within GCP
- Create a key for Service Account from GCP and download to system running Phoenix
    - Add path to key to env var `GOOGLE_APPLICATION_CREDENTIALS`
- Give the Service Account email address Editor access to the root Drive folder in Drive for your
  project
- For each tenant Drive folder within the root folder, add their Drive folder ID (the UUID/SHA
  string from the browser URL when viewing the folder) for each tenant to the tenant config yaml
