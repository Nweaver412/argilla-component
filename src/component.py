import argilla as rg
import csv
from datetime import datetime
import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from configuration import Configuration


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):

        # Load configuration
        params = Configuration(**self.configuration.parameters)

        # Authenticate with Argilla
        rg.init(api_key=params.pswd_api_token)

        # Get input tables
        input_tables = self.get_input_tables_definitions()
        if len(input_tables) == 0:
            raise UserException("No input tables found")

        input_table = input_tables[0]
        logging.info(f'Processing table: {input_table.name}')

        # Read input data
        records = []
        with open(input_table.full_path, mode='r', encoding='utf-8') as inp_file:
            reader = csv.DictReader(inp_file)
            for row in reader:
                # Extract relevant fields
                status = row.get("status", "").strip()
                server_id = row.get("response", "").strip()
                persona = row.get("persona", "").strip()
                image = row.get("image", "")
                # category = row.get("category", "").strip()
                # references = row.get("references", "")

                # Only create and append a record if `prompt` and `response` are not empty
                # if prompt and response:
                #     # Process keywords and references into strings
                #     keywords_str = ', '.join(kw.strip() for kw in keywords.split(',') if kw.strip()) if keywords else ""
                #     references_str = '; '.join(ref.strip() for ref in references.split(';') if ref.strip()) if references else ""

                    # Create the record
                record = rg.Record(
                    fields={
                        "status": status,
                        "_server_id": server_id,
                        "persona": persona,
                        "image": image,
                        # "category": category,
                        # "references": references_str,
                    },
                )
                records.append(record)

        # Create or append to Argilla dataset
        dataset_name = f"my_keboola_dataset_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        rg.log(records, name=dataset_name, workspace="default")

        logging.info(f"Data written to Argilla dataset: {dataset_name}")

        # Write new state
        self.write_state_file({"last_dataset_name": dataset_name})


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
