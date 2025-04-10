https://cli.datacontract.com/

pip install uv
uv venv
uv pip install -r ./requirements.txt


source .venv/bin/activate

datacontract test https://datacontract.com/examples/orders-latest/datacontract.yaml
