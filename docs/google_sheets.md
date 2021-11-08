# Google Sheets

Certain analyses and higher-level inferences implemented within Phoenix use supervised machine
learning algorithms. Google Sheets is used for interfacing with users to enable them to label data
which is subsequently used in those supervised learning algorithms. Phoenix both creates Google
Sheets for users, and reads sheets containing user input data.

## Configuration

Each tenant (see [multitenancy](./multitenancy.md) needs to have a folder withing Google Drive created
for it manually. The folder IDs - which is the UUID looking string from the URL when accessing
that folder from a browser - needs to be added to the config file
`local_artefacts/config/google_sheets.csv`, with columns `tenant_id` and `drive_folder_id`.

All user interface sheets will be populated and read from that tenant's Google Drive folder.

## Authentication

In all cases (development, running locally, and in production), a Google Cloud Service Account is
required (which requires creation of a Google Cloud Platform project). This service account needs to
manually be given `Editor` access to all the tenants' Drive folders (or a parent folder of them) by
adding the service account email as `Editor` through the browser.

For each tenant, any users that need to label data or interface with this part of Phoenix's
pipeline will need the appropriate access added manually to their tenant's Google Drive folder.

Similarly for Phoenix devs who may need to interact with/edit a tenant's sheets (likely for fixing
tenant input data issues) they will need to manually be given access to the folders (or a parent
folder of them).

### Development/running locally

Authentication for API access for development or running pipelines that interface with Google
Sheets locally should use Service Account Impersonation as per [this](https://stackoverflow.com/questions/63506885/how-to-authenticate-google-apis-google-drive-api-from-google-compute-engine-an)
.

### Production

If running Phoenix on Google Cloud Platform infrastructure then the Service Account used for the
compute resource should impersonate the Service Account specific for Google Sheets.

If running outside of Google Cloud Platform, then creation and usage of a json credentials key will
be necessary.

## Integration testing

Phoenix includes integration tests for it's Google Sheets usage. To run the integration tests
authentication through Service Account Impersonation will be required as per the above. During
integration testing, the Service Account will create and subsequently read from Google Sheets it
creates within it's own Google Drive to verify functionality - these test folders and sheets won't
be accessible to the developer by default as they will be created within the Service Account's own
Google Drive and thus only be accessible by the Service Account.
