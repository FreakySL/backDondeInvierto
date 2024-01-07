import time

from .models import FundClassParser
from .sheets import APISpreadsheet
from .common.utils import (
    get_logger,
    get_current_time,
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
    now = get_current_time().strftime("%d-%m-%Y %H:%M:%S")

    # Get all fund groups from sheet
    funds_cafci_codes = sheet.get_data(sheet_name=parser.get_sheet(), _range=parser.get_fund_codes_range())

    new_data = []

    for fund_code in funds_cafci_codes:
        logger.info("Getting data from fund code %s", fund_code)
        # Get the TEM for the fund
        first_price, last_price = parser.get_seven_days_price(class_id=fund_code[0], fund_id=fund_code[1])

        # Append the TEM to the new data
        tem = parser.get_tem(first_price, last_price)

        # Now get the monthly performance
        monthly_performance = parser.get_last_monthly_performance(class_id=fund_code[0], fund_id=fund_code[1])

        # Append the tem and monthly performance to the new data
        new_data.append([tem, monthly_performance, now])

    # Update the sheet database
    logger.info("Updating sheet database")

    sheet.update_data(
        values=new_data,
        sheet_name=parser.get_sheet(),
        _range=parser.get_tem_monthly_updated_range(),
    )

    end_time = time.time()  # End time annotation
    elapsed_time = end_time - start_time
    logger.info("Database updated")
    logger.info(f"Elapsed time: {elapsed_time} seconds")


def search_fund_by_name():
    """
    Search a fund by name.
    """
    fund_name = input("Ingrese el nombre del fondo: ")  # Santander Ahorro PESOS
    logger.info("Searching fund by name %s", fund_name)
    sheet = APISpreadsheet()

    # Get all funds from our database
    funds = sheet.get_all_rows_formated()  # {"2942": {"name": "Santander"}}
    # search the fund by name
    logger.info("Searching fund by name %s", fund_name)
    fund_name = fund_name.lower()
    for fund in funds:
        if fund_name == funds[fund]["name"].lower():
            logger.info("Fund %s found", fund_name)
            logger.info("Fund data: %s", funds[fund])
            return funds[fund]

    logger.info("Fund %s not found", fund_name)
    return None
