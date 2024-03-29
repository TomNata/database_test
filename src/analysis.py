from services.db_manager import DBManager


def get_data():
    db_manager = DBManager('dns_retail')
    db_manager.get_top_sales()
    db_manager.get_top_sales_daytime()


if __name__ == '__main__':
    get_data()


