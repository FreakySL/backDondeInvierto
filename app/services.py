from .models import FundClassParser
from .sheets import APISpreadsheet
from .common.utils import (
    get_logger,
)

logger = get_logger(__name__)


def create_initial_funds_database():
    """
    Create the initial funds database.
    """
    # Create the initial funds database
    logger.info("Starting to create the initial funds database")
    logger.info("Checking if the database is empty")
    # Check if the database is empty
    sheet = APISpreadsheet()
    parser = FundClassParser()
    data = sheet.get_all_rows(sheet_name=parser.get_sheet(), _range=parser.get_max_range())

    if len(data) > 1:
        logger.info("The database is not empty")
        logger.info("Aborting")
        return

    logger.info("The database is empty")
    logger.info("Getting all fund groups")

    # Create all fund groups
    # fund_groups = parser.create_all_funds()





def update_funds_database():
    """
    Update the database.
    """
    # Update the database
    logger.info("Updating database")
