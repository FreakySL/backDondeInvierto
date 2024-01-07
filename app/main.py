from .common.utils import (
    get_logger,
    validate_option,
)

from .services import (
    create_initial_funds_database,
    update_funds_database,
)


logger = get_logger(__name__)

if __name__ == '__main__':
    # Give to the user 2 options, create the initial funds database or update it
    print("1. Create initial funds database")
    print("2. Update funds database")
    option = input("Select an option: ")

    option = validate_option(option)

    switcher = {
        "1": create_initial_funds_database,
        "2": update_funds_database,
    }

    # Get the function from switcher dictionary
    func = switcher.get(option, lambda: "Invalid option")

    # Execute the function
    func()
