import os

# check if this is running in VS Code
in_vs_code = os.getenv('DATACOVES__USER_SLUG', None)
pipelines_dir = ''
if in_vs_code:
    pipelines_dir = os.path.join('/config','.dlt','pipelines')
    print(f"pipelines_dir set to: {pipelines_dir}")
else:
    pipelines_dir = os.path.join('/tmp','.dlt','pipelines')
    print(f"pipelines_dir set to: {pipelines_dir}")
