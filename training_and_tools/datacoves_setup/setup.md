## Setting up Transform Tab

Generate an SSH Key for git access

```
ssh-keygen -t ed25519 -C "<your email>"
cat ~/.ssh/id_ed25519.pub
copy key and add to github
    https://github.com/settings/ssh/new
```

Run dbt coves setup to configure git and dbt profiles
```
dbt-coves setup
    Answers:
    - No
    - <enter your name>
    - <enter your email>
    - git@github.com:datacoves/balboa.git
    - <enter your snowflake psw>
    - <enter your snowflake username>
```

To reset the username/psw using for dbt profiles delete the files below then rerun dbt coves setup above
```
rm ~/workspace/.vscode/settings.json 
rm ~/.dbt/profiles.yml 
```

## Configuring Git aliases

Add the following to `~/.gitconfig`

```
[alias]
  br = !git branch
  co = !git checkout
  l = !git log
  st = !git status
  po = !git pull origin main
  prune-branches = !git remote prune origin && git branch -vv | grep ': gone]' | awk '{print $1}' | xargs -r git branch -D
```

## Loading Airbyte configs
Reveal git secrets

Run
```
dbt-coves load airbyte --path ../load --host http://airbyte-server-svc --port 8001 --secrets ../load/secrets
```

# Setup power tools
install vscode-dbt-0.4.0.vsix
install vscode-dbt-power-user-0.5.2-datacoves.vsix

in terminal intall sync server
`pip install dbt-sync-server`

Update the PATH
`export PATH=$PATH:/config/.local/bin`

Run the server
`dbt_sync_server --inject-rpc`

in Settings -> Text Editor -> Files, add Associations
*.sql     jinja-sql

Open ports 8580 and 8581 on localhost
