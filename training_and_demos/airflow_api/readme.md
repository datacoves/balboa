# Creating the lambda function zip file

To run this script in a lambda function you need to upload the requests library
along with the lambda_function.py

```bash
pip install --target ./package requests
cd package
zip -r ../deployment-package.zip .
cd ..
zip -g deployment-package.zip lambda_function.py
```

Set the environment variables in the lambda function

```bash
AIRFLOW_API_URL = "https://..."
DATACOVES_API_KEY = "...."
```

Load `deployment-package.zip` into the Lambda funciton.
