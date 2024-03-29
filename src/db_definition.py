from services.db_manager import DBManager


def csv_to_database():
    """ Работа с БД: создание и заполнение таблиц первичными данными
    """

    db_manager = DBManager('dns_retail')
    db_manager.create_database()
    print("БД 'dns_retail' успешно создана")

    db_manager.create_table_cities()
    print("Таблица 'cities' успешно создана")

    db_manager.create_table_branches()
    print("Таблица 'branches' успешно создана")

    db_manager.create_table_products()
    print("Таблица 'products' успешно создана")

    db_manager.create_table_sales()
    print("Таблица 'sales' успешно создана")

    db_manager.create_table_product_class()
    print("Таблица 'product_class' успешно создана")
    print()

    db_manager.fill_table_cities()
    print("Таблица 'cities' успешно заполнена")

    db_manager.fill_table_branches()
    print("Таблица 'branches' успешно заполнена")

    db_manager.fill_table_products()
    print("Таблица 'products' успешно заполнена")

    db_manager.fill_table_sales()
    print("Таблица 'sales' успешно заполнена")


if __name__ == '__main__':
    csv_to_database()
