from .common.utils import (
    get_logger,
    validate_option,
)

from .services import (
    create_initial_funds_database,
    search_fund_by_name,
    start_debug_mode,
    update_funds_database,
    check_database_integrity,
)

logger = get_logger(__name__)

if __name__ == '__main__':
    # Give to the user 2 options, create the initial funds database or update it
    print("1. Create initial funds database")
    print("2. Update funds database")
    print("3. Search fund by name")
    print("4. Check database integrity")
    print("5. Start debug mode")
    option = input("Select an option: ")

    option = validate_option(option)

    switcher = {
        "1": create_initial_funds_database,
        "2": update_funds_database,
        "3": search_fund_by_name,
        "4": check_database_integrity,
        "5": start_debug_mode,
    }

    # Get the function from switcher dictionary
    func = switcher.get(option, lambda: "Invalid option")

    # Execute the function
    func()
