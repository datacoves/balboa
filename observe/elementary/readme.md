# Setup

Install dependencies

```
pip install -r requirements.txt
```

Add this to dbt_project.yml
```
# Added for Elementary support
flags:
  require_explicit_package_overrides_for_builtin_materializations: False
  source_freshness_run_project_hooks: True
```

Add this to packages.yml (check for latest version)
```
  - package: elementary-data/elementary
    version: 0.16.2
```

# Set up elementary models
```
dbt deps
dbt run --select elementary
```

# Run dbt tests
`dbt test`

# Generate elementary report
`./update_profiles.py`
`edr report`

# View Elemenrary Report
`./elementary_web.py`

# Send notifications to Slack

Set up Slack, by going to https://api.slack.com/apps and creating an app.
In the app -> OAuth & Permissions -> Scopes, select
* channels:read
* chat:write
* files:write

In the In the app -> OAuth & Permissions -> OAuth Tokens section, click Install / Reinstall
Copy the `Bot User OAuth Token` and secure it.

In Slack, go to the channel and click the channel name, then Integrations, in the App section click Add apps
Find the app and click `Add`

Rename the `.env.sample` file to `.env` and set the channel name and slack token

Load the environment variables to the shell
`source .env`

Run the elementary notifier as follows:
`edr monitor --slack-channel-name $SLACK_CHANNEL  --slack-token $SLACK_TOKEN`

Send the full report to Slack
`edr send-report --slack-channel-name $SLACK_CHANNEL  --slack-token $SLACK_TOKEN`

# Send notifications to Teams

Set up teams webhook
https://docs.elementary-data.com/oss/guides/alerts/send-teams-alerts#teams-config-as-in-config-yml

Rename the `.env.sample` file to `.env` and set the teams webhook url

Load the environment variables to the shell
`source .env`

Run the elementary notifier as follows:
`edr monitor --teams-webhook $TEAMS_WEBHOOK`
