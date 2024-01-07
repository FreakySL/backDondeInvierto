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

    logger.info("Initial database created")


def update_funds_database():
    """
    Update the database.
    """
    # Update the database
    logger.info("Updating database")
