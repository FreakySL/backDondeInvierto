import time

from .models import FundClassParser
from .sheets import APISpreadsheet
from .common.utils import (
    get_logger,
    parse_array_list_to_single_list,
)

logger = get_logger(__name__)


def create_initial_funds_database():
    """
    Create the initial funds database.
    """
    start_time = time.time()  # Start time annotation

    # Create the initial funds database
    logger.info("Starting to create the initial funds database")
    logger.info("Checking if the database is empty")
    # Check if the database is empty
    sheet = APISpreadsheet()
    parser = FundClassParser()
    data = sheet.get_data(sheet_name=parser.get_sheet(), _range=parser.get_max_range())

    if len(data) > 1:
        logger.info("The database is not empty")
        logger.info("Aborting")
        return

    logger.info("The database is empty")
    logger.info("Getting all funds from cafci")

    # Get all funds from cafci
    all_fund_classes = parser.get_all_funds()

    # Divide the funds data in 10 chunks
    logger.info("Dividing the funds data in 10 chunks")
    chunks = [all_fund_classes[x:x + 10] for x in range(0, len(all_fund_classes), 10)]

    # Create the initial database
    logger.info("Creating the initial database")

    for chunk in chunks:
        sheet.post_data(
            values=chunk,
            sheet_name=parser.get_sheet(),
        )

    end_time = time.time()  # End time annotation
    elapsed_time = end_time - start_time

    logger.info("Initial database created")
    logger.info(f"Elapsed time: {elapsed_time} seconds")


def update_funds_database():
    """
    Update the database.
    """
    start_time = time.time()  # Start time annotation

    # Update the database

    logger.info("Updating database")

    # Get all funds from our database
    sheet = APISpreadsheet()
    parser = FundClassParser()

    # Get all fund groups from sheet
    funds_cafci_code = sheet.get_data(sheet_name=parser.get_sheet(), _range=parser.get_fund_group_cell_range())
    print(funds_cafci_code)

    fund_groups_formated = parse_array_list_to_single_list(funds_cafci_code)
    print(fund_groups_formated)

    end_time = time.time()  # End time annotation
    elapsed_time = end_time - start_time
    logger.info("Database updated")
    logger.info(f"Elapsed time: {elapsed_time} seconds")
