# Inlined from /metadata-ingestion/examples/library/create_domain.py
from dotenv import load_dotenv
import os
import csv
import logging

from datahub.emitter.mce_builder import make_domain_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass, DomainPropertiesClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Load variables from the .env file
load_dotenv()

datahub_host = os.getenv("DATACOVES__DATAHUB_HOST_NAME")
datahub_api_key = os.getenv("DATAHUB_API_KEY")


# Specify the path to your CSV file
csv_file_path = '/config/workspace/automate/datahub/domains.csv'

# Open the CSV file
with open(csv_file_path, mode='r') as file:
    csv_reader = csv.DictReader(file)

    # Iterate through each row
    for row in csv_reader:
        domain = row['domain']
        description = row['description']

        domain_urn = make_domain_urn(domain.strip().lower())
        domain_properties_aspect = DomainPropertiesClass(
            name = domain,
            description = description,
        )

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityType = "domain",
            changeType = ChangeTypeClass.UPSERT,
            entityUrn = domain_urn,
            aspect = domain_properties_aspect,
        )

        rest_emitter = DatahubRestEmitter(gms_server=datahub_host, token=datahub_api_key)
        rest_emitter.emit(event)
        log.info(f"Created domain {domain_urn}")
