# Inlined from /metadata-ingestion/examples/library/create_term.py
from dotenv import load_dotenv
import os
import csv
import logging

from datahub.emitter.mce_builder import make_term_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Imports for metadata model classes
from datahub.metadata.schema_classes import GlossaryTermInfoClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Load variables from the .env file
load_dotenv()

datahub_host = os.getenv("DATACOVES__DATAHUB_HOST_NAME")
datahub_api_key = os.getenv("DATAHUB_API_KEY")

# Specify the path to your CSV file
csv_file_path = '/config/workspace/automate/datahub/terms.csv'

# Open the CSV file
with open(csv_file_path, mode='r') as file:
    csv_reader = csv.DictReader(file)

    # Iterate through each row
    for row in csv_reader:
        term = row['term']
        term_urn = make_term_urn(term.strip().lower())

        description = row['description']

        term_properties_aspect = GlossaryTermInfoClass(
            definition = description,
            name = term,
            termSource = "",
        )

        event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityUrn = term_urn,
            aspect = term_properties_aspect,
        )

        # Create rest emitter
        rest_emitter = DatahubRestEmitter(gms_server=datahub_host, token=datahub_api_key)
        rest_emitter.emit(event)
        log.info(f"Created term {term_urn}")
