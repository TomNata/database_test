from pathlib import Path

ROOT_PATH = Path(__file__).parent
CSV_FILES_PATH = Path.joinpath(ROOT_PATH, "test_data")
BRANCHES_PATH = Path.joinpath(CSV_FILES_PATH, "t_branches.csv")
CITIES_PATH = Path.joinpath(CSV_FILES_PATH, "t_cities.csv")
PRODUCTS_PATH = Path.joinpath(CSV_FILES_PATH, "t_products.csv")
SALES_PATH = Path.joinpath(CSV_FILES_PATH, "t_sales.csv")
NEW_PRODUCTS_PATH = Path.joinpath(CSV_FILES_PATH, "t_new_products.csv")
NEW_BRANCHES_PATH = Path.joinpath(CSV_FILES_PATH, "t_new_branches.csv")
EXCEL_PATH = Path.joinpath(ROOT_PATH, "product_class.xlsx")
