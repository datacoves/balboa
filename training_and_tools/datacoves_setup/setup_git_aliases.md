
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
