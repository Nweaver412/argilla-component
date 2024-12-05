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
        rg.init(
            api_url=params.api_url,
            api_key=params.pswd_api_token
        )

        # Define dataset settings
        settings = rg.Settings(
            guidelines="Please label the data accurately and review additional fields for completeness.",
            fields=[
                rg.TextField(name="text"),
                rg.TextField(name="messageId"),
                rg.TextField(name="partId"),
                rg.TextField(name="mimeType"),
                rg.TextField(name="bodySize"),
                rg.TextField(name="bodyData"),
            ],
            questions=[
                rg.LabelQuestion(
                    name="label",
                    labels=["yes", "no"]
                )
            ],
        )

        # Create the Argilla dataset
        dataset_name = f"{params.argilla.dataset_name_prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        dataset = rg.Dataset(
            name=dataset_name,
            settings=settings,
        )
        dataset.create()
        logging.info(f"Dataset {dataset_name} created in Argilla.")

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
                text = row.get("text", "").strip()
                messageId = row.get("messageId", "").strip()
                partId = row.get("partId", "").strip()
                mimeType = row.get("mimeType", "").strip()
                bodySize = row.get("bodySize", "").strip()  
                bodyData = row.get("bodyData", "").strip()  

                label = row.get("label", "").strip()

                if text and label:  # Ensure necessary fields are present
                    record = {
                        "text": text,
                        "messageId": messageId,
                        "partId": partId,
                        "mimeType": mimeType,
                        "bodySize": bodySize,
                        "bodyData": bodyData,
                        "label": label
                    }
                    records.append(record)

        # Log records to Argilla
        dataset.records.log(records)
        logging.info(f"{len(records)} records logged to Argilla dataset: {dataset_name}")

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
