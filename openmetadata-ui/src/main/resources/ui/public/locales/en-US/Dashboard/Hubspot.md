# HubSpot

In this section, we provide guides and references to use the HubSpot connector.

## Requirements

To access the HubSpot APIs and ingest CRM pipeline and schema metadata into OpenMetadata, you'll need a **HubSpot Private App Access Token**.

## HubSpot Account Setup and Permissions

### Step 1: Create a Private App

1. Log into your <a href="https://app.hubspot.com" target="_blank">HubSpot account</a>.
2. Navigate to **Settings** → **Integrations** → **Private Apps**.
3. Click **Create a private app**.
4. Give your app a name and description.
5. Under the **Scopes** tab, grant the following scopes:
   - `crm.objects.deals.read` — required for pipeline ingestion
   - `crm.objects.contacts.read` — required for CRM schema ingestion
   - `crm.objects.companies.read`, `crm.objects.tickets.read` — recommended
   - `behavioral_events.event_definitions.read_write` — optional, for custom event definitions
   - `crm.schemas.custom.read` — optional, for custom object schemas
6. Click **Create app** and copy the generated access token.

### Step 2: Find Your Hub ID

Your Hub ID is the numeric identifier for your HubSpot account. You can find it in the URL when logged in (e.g., `https://app.hubspot.com/contacts/12345678/`) or under **Settings** → **Account Management** → **Account Details**.

You can find further information on the HubSpot connector in the <a href="https://docs.open-metadata.org/connectors/dashboard/hubspot" target="_blank">docs</a>.

## Connection Details

$$section
### Access Token $(id="accessToken")

A HubSpot Private App Access Token used to authenticate API requests.

To generate a token:
1. Go to **Settings** → **Integrations** → **Private Apps** in your HubSpot account
2. Create a new private app with the required scopes
3. Copy the access token from the app details page

**Important**: Store this token securely — it provides access to your HubSpot CRM data.
$$

$$section
### Hub ID $(id="hubId")

Your HubSpot account's numeric Hub ID. This is used to construct direct links to pipelines and records in the OpenMetadata UI.

You can find your Hub ID:
- In the URL when logged in: `https://app.hubspot.com/contacts/{hubId}/`
- Under **Settings** → **Account Management** → **Account Details**

This field is optional. If omitted, source URLs for dashboards and charts will not be set.
$$