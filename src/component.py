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
        self._configuration = None
        self.client = None

    def init_configuration(self):
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def run(self):
        self.init_configuration()

        # Auth
        rg.Argilla(
            api_url = self._configuration.api_url,
            api_key = self._configuration.pswd_api_token
        )
        print(self._configuration.api_url)
        print(self._configuration.pswd_api_token)

        # INIT SETTINGS TO ARGILLA SETTINGS

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
        dataset_name = self._configuration.data_name
        print(dataset_name)
        # logging.info(dataset_name) #confirming dataset name

        dataset = rg.Dataset(
            name=dataset_name,
            settings=settings,
        )
        dataset.create()

        # Get input tables
        input_tables = self.get_input_tables_definitions()
        
        if len(input_tables) == 0:
            raise UserException("No input tables found")

        input_table = input_tables[0]

        # Read input data
        records = []

        # Opens table
        with open(input_table.full_path, mode='r', encoding='utf-8') as inp_file:
            reader = csv.DictReader(inp_file)
            for row in reader: # Iterate over rows
                text = row.get("text", "").strip()
                messageId = row.get("messageId", "").strip()
                partId = row.get("partId", "").strip()
                mimeType = row.get("mimeType", "").strip()
                bodySize = row.get("bodySize", "").strip()  
                bodyData = row.get("bodyData", "").strip()  

                label = row.get("label", "").strip()
                record = rg.Record(
                    fields ={
                        "text": text,
                        "messageId": messageId,
                        "partId": partId,
                        "mimeType": mimeType,
                        "bodySize": bodySize,
                        "bodyData": bodyData,
                        "label": label
                    }
                )
                records.append(record)

        # Log records to Argilla
        dataset.records.log(records)
        
        logging.info(f"{len(records)} records logged to dataset: {dataset_name}")



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
