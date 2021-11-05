# Multitenancy

Phoenix uses the following specification of [multitenancy](https://en.wikipedia.org/wiki/Multitenancy).

A single instance of Phoenix can serve multiple tenants, scraping data for a different
specification per tenant, and producing a different end dashboard containing different data per
tenant.

Throughout the code base and in corresponding infrastructure, anything which is tenant specific
should be prefixed/denoted as such by using a `tenant_id`. Which is a string that uniquely
identifies that tenant from the set of all current tenants.

The set of `tenant_id`s needs to be specified within `local_artefacts/config/tenant_ids.csv`. This
will be automatically picked up by Phoenix and used to, _for each tenant_, pull config, run all
pipelines, e.g. scrape, process, and analyse, producing output artefacts specific to each tenant.

