from typing import List

from configuration.config import DEST_URL
from services.service import Service
from steps.data_extractor import DataExtractor
from steps.data_loader import DataLoader
from steps.data_processor import DataProcessor
from steps.data_writer import DataWriter
from steps.processor import Processor
from steps.validate_compliant import ValidateCompliant


def create_workflow(extract_service: Service, load_service: Service, write_service: Service, file_format: str) -> List:
    return [
        DataExtractor(extract_service),
        DataLoader(load_service),
        DataProcessor(),
        ValidateCompliant(),
        DataWriter(write_service, DEST_URL, file_format)
    ]
