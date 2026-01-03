We can generate "static" files when deploying to production.

We can then push them to dbt-api on Datacoves to skip partial parse in Airflow tasks.

In order for Airflow to skip partial parse these need to be the same when the manifest and partial parse files are generated.

```yaml
      database: MY_DATABASE
      role: MY_ROLE
      schema: MY_SCHEMA
      user: AIRFLOW_USER
```

If these values differ, dbt will detect the difference and parse the project.
