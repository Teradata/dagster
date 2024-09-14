---
layout: Integration
status: published
name: GCP BigQuery
title: Dagster & GCP BigQuery
sidebar_label: GCP BigQuery
excerpt: Integrate with GCP BigQuery.
date: 2022-11-07
apireflink: https://docs.dagster.io/_apidocs/libraries/dagster-gcp
docslink: 
partnerlink: 
logo: /integrations/gcp-bigquery.svg
categories:
  - Storage
enabledBy:
enables:
---

### About this integration

The Google Cloud Platform BigQuery integration allows data engineers to easily query and store data in the BigQuery data warehouse through the use of the `BigQueryResource`.

### Installation

```bash
pip install dagster-gcp
```

### Examples

```python
from dagster import Definitions, asset
from dagster_gcp import BigQueryResource


@asset
def my_table(bigquery: BigQueryResource):
    with bigquery.get_client() as client:
        client.query("SELECT * FROM my_dataset.my_table")


defs = Definitions(
    assets=[my_table], resources={"bigquery": BigQueryResource(project="my-project")}
)
```

### About Google Cloud Platform BigQuery

The Google Cloud Platform BigQuery service, offers a fully managed enterprise data warehouse that enables fast SQL queries using the processing power of Google's infrastructure.