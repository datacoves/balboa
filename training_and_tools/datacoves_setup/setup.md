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

