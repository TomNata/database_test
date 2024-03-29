from services.db_manager import DBManager


def add_product_class():
    db_manager = DBManager('dns_retail')
    db_manager.assign_class_to_product()


if __name__ == '__main__':
    add_product_class()
