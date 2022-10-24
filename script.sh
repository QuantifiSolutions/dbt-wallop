#!/bin/sh

dbt deps  # Pulls the most recent version of the dependencies listed in your packages.yml from git
dbt run --target prod
dbt docs generate