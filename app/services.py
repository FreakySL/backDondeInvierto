import time

from .models import FundClassParser
from .sheets import APISpreadsheet
from .common.utils import (
    get_logger,
    get_current_time,
)
from multiprocessing import Pool
from emoji import emojize


logger = get_logger(__name__)


def start_debug_mode():
    """
    Start the debug mode.
    """
    # start all clases
    parser = FundClassParser() # noqa
    api = APISpreadsheet() # noqa

    import ipdb
    ipdb.set_trace()

    return None


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

    # Create the initial database
    logger.info("Creating the initial database")

    sheet.post_data(
        values=all_fund_classes,
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

    logger.info(emojize(":rocket: Initializing database update"))

    # Get all funds from our database
    sheet = APISpreadsheet()
    parser = FundClassParser()
    # now = get_current_time().strftime("%d-%m-%Y")

    # Get all fund groups from sheet
    funds_cafci_codes = sheet.get_data(sheet_name=parser.get_sheet(), _range=parser.get_fund_codes_range())
    logger.info(f"Got {len(funds_cafci_codes)} funds from sheet")

    new_data = []

    # Create a pool of worker processes
    with Pool(processes=8) as pool:
        # Use the map method to distribute the work among the workers
        new_data = pool.map(calc_data_by_fund, funds_cafci_codes)

    # Update the sheet database
    logger.info(emojize(":rocket: Updating sheet database"))
    try:
        sheet.reload()
        sheet.update_data(
            values=new_data,
            sheet_name=parser.get_sheet(),
            _range=parser.get_calc_data_range(),
        )
    except Exception as e:
        logger.error(emojize(f":warning: Error updating sheet database: {e}"))
        import ipdb
        ipdb.set_trace()

    end_time = time.time()  # End time annotation
    elapsed_time = end_time - start_time
    logger.info(emojize(":check_mark_button: Database updated"))
    logger.info(emojize(f":stopwatch: Elapsed time: {elapsed_time} seconds"))


def calc_data_by_fund(fund_code: list) -> list:
    """
    Calculate the data for a fund.
    """
    parser = FundClassParser()
    class_id = fund_code[0]
    fund_id = fund_code[1]
    now = get_current_time().strftime("%d-%m-%Y")

    logger.info(emojize(f":hourglass_not_done: Getting cafci data from class id {class_id} and fund id {fund_id}"))
    # Get the TEM for the fund
    first_price, last_price = parser.get_prices_by_range(class_id=class_id, fund_id=fund_id, date_range=7)

    # Append the TEM to the new data
    if first_price == 0 or last_price == 0:
        tem = 0
        tna = 0
        tea = 0
    else:
        tem, tna, tea = parser.get_proyection(initial_price=first_price, final_price=last_price)

    # Now get the monthly performance
    monthly_performance = parser.get_performance_by_range(class_id=class_id, fund_id=fund_id, date_range=30)
    six_month_performance = parser.get_performance_by_range(class_id=class_id, fund_id=fund_id, date_range=180)
    year_performance = parser.get_performance_by_range(class_id=class_id, fund_id=fund_id, date_range=365)

    # Return the tem and monthly performance
    return [
        str(tna),
        str(tea),
        str(tem),
        str(monthly_performance),
        str(six_month_performance),
        str(year_performance),
        now
    ]


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
