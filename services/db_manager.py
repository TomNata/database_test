import io

import pandas as pd
import psycopg2
import matplotlib.pyplot as plt

from config import config
from services.funcs import time_of_function, divide_products_to_classes
from settings import BRANCHES_PATH, CITIES_PATH, PRODUCTS_PATH, SALES_PATH, \
    EXCEL_PATH


class DBManager:
    """ Класс для работы с базой данных (БД):
    создает и заполняет таблицы данными из csv-файлов,
    делает выборки из БД, делает расчёты
    """
    def __init__(self, db_name):
        self.db_name = db_name
        self.params = config()

    def create_database(self):
        """Создаёт базу данных"""

        conn = psycopg2.connect(database='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'DROP DATABASE IF EXISTS {self.db_name}')
        cur.execute(f'CREATE DATABASE {self.db_name}')
        conn.close()

    def create_table_cities(self):
        """Создаёт таблицу cities"""

        query = """
            CREATE TABLE cities
            (
                id int NOT NULL,
                ссылка varchar(100) PRIMARY KEY,
                наименование varchar(100) NOT NULL
            )
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        conn.close()

    def create_table_branches(self):
        """Создаёт таблицу branches"""

        query = """
            CREATE TABLE branches
            (
                id int NOT NULL,
                ссылка varchar(100) PRIMARY KEY,
                наименование varchar(100),
                город varchar(100) REFERENCES cities(ссылка) NOT NULL,
                краткое_наименование varchar(100),
                регион varchar(100)
            )
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        conn.close()

    def create_table_products(self):
        """Создаёт таблицу products"""

        query = """
            CREATE TABLE products
            (
                id int NOT NULL,
                ссылка varchar(100) PRIMARY KEY,
                наименование text NOT NULL
            )
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        conn.close()

    def create_table_sales(self):
        """Создаёт таблицу sales"""

        query = """
            CREATE TABLE sales
            (
                id int NOT NULL,
                период timestamp NOT NULL,
                филиал varchar(100) REFERENCES branches NOT NULL,
                номенклатура varchar(100) REFERENCES products NOT NULL,
                количество real NOT NULL,
                продажа real NOT NULL
            )
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
        conn.close()

    def create_table_product_class(self):
        """Создаёт таблицу product_class"""

        # Создание таблицы 'КлассТовара' в БД.
        query = """
            CREATE TABLE IF NOT EXISTS product_class
            (
                id int NOT NULL,
                номенклатура varchar(100) REFERENCES products NOT NULL,
                КлассТовара varchar(100) NOT NULL
            )
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)

    @time_of_function
    def fill_table_branches(self):
        """Заполняет таблицу branches данными из csv-файла"""

        # для приведения к чистому виду csv-файл загружен в Pandas DataFrame
        df = pd.read_csv(BRANCHES_PATH, delimiter=',',
                         skipinitialspace=True, encoding='utf-8')
        df['Наименование'] = df['Наименование'].replace('^я', '', regex=True)

        # преобразованный DataFrame загружен в таблицу 'products' БД
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.copy_from(output, 'branches')
        conn.close()

    @time_of_function
    def fill_table_cities(self):
        """Заполняет таблицу cities"""

        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur, open(CITIES_PATH,
                                            'r', encoding='utf8') as file:
                next(file)
                cur.copy_from(file, 'cities', sep=",")
        conn.close()

    @time_of_function
    def fill_table_products(self):
        """Заполняет таблицу products"""

        # для приведения csv к чистому виду файл загружен в Pandas DataFrame
        df = pd.read_csv(PRODUCTS_PATH, delimiter=',',
                         skipinitialspace=True, encoding='utf-8')
        df['Наименование'] = df['Наименование'].replace(
            {r'\\': r'/', r'\t': ' '}, regex=True
        )

        # преобразованный DataFrame загружен в таблицу 'products' БД
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)

        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.copy_from(output, 'products')

        conn.close()

    @time_of_function
    def fill_table_sales(self):
        """Заполняет таблицу sales"""

        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur, open(SALES_PATH,
                                            'r', encoding='utf-8') as file:
                next(file)
                cur.copy_from(file, 'sales', sep=",")
        conn.close()

    @time_of_function
    def get_top_sales(self):
        """Получает данные из БД:
        ТОП 10 магазинов, складов, товаров по количеству продаж.
        """
        # запрос ТОП 10 магазинов
        query_1 = """SELECT b.наименование, SUM(s.количество)
            FROM sales s
            LEFT JOIN branches b on s.филиал=b.ссылка
            WHERE b.наименование NOT LIKE '%клад%'
            GROUP BY b.наименование
            ORDER BY SUM(s.количество) DESC
            LIMIT 10
        """
        # запрос ТОП 10 складов
        query_2 = """SELECT b.наименование, SUM(s.количество)
           FROM sales s
           LEFT JOIN branches b on s.филиал=b.ссылка
           WHERE b.наименование LIKE '%клад%'
           GROUP BY b.наименование
           ORDER BY SUM(s.количество) DESC
           LIMIT 10
        """
        # запрос ТОП 10 товаров по складам
        query_3 = """SELECT p.наименование, SUM(s.количество)
           FROM sales s
           JOIN products p on s.номенклатура=p.ссылка
           JOIN branches b on s.филиал=b.ссылка
           WHERE b.наименование LIKE '%клад%'
           GROUP BY p.наименование
           ORDER BY SUM(s.количество) DESC
           LIMIT 10
        """
        # запрос ТОП 10 товаров по магазинам
        query_4 = """SELECT p.наименование, SUM(s.количество)
           FROM sales s
           JOIN products p on s.номенклатура=p.ссылка
           JOIN branches b on s.филиал=b.ссылка
           WHERE b.наименование NOT LIKE '%клад%'
           GROUP BY p.наименование
           ORDER BY SUM(s.количество) DESC
           LIMIT 10
        """
        # запрос ТОП 10 городов по продажам
        query_5 = """SELECT c.наименование, SUM(s.количество)
           FROM sales s
           JOIN branches b on s.филиал=b.ссылка
           JOIN cities c on b.город=c.ссылка
           GROUP BY c.наименование
           ORDER BY SUM(s.количество) DESC
           LIMIT 10
        """

        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query_1)
                print('10 первых магазинов по количеству продаж:')
                for i, row in enumerate(cur.fetchall(), start=1):
                    print(i, *row)
                print()

                cur.execute(query_2)
                print('10 первых складов по количеству продаж:')
                for i, row in enumerate(cur.fetchall(), start=1):
                    print(i, *row)
                print()

                cur.execute(query_3)
                print('10 самых продаваемых товаров по складам:')
                for i, row in enumerate(cur.fetchall(), start=1):
                    print(i, *row)
                print()

                cur.execute(query_4)
                print('10 самых продаваемых товаров по магазинам:')
                for i, row in enumerate(cur.fetchall(), start=1):
                    print(i, *row)
                print()

                cur.execute(query_5)
                print('10 городов, в которых больше всего '
                      'продавалось товаров:')
                for i, row in enumerate(cur.fetchall(), start=1):
                    print(i, *row)
        conn.close()

    def get_top_sales_daytime(self):
        """Рассчитывает и выводит данные, в т.ч. графически:
        в какие часы и в какой день недели
        происходит максимальное количество продаж.
        """

        # группирует продажи по часам и
        # выводит время с наибольшим значением
        query_1 = """SELECT EXTRACT('hour' FROM период), SUM(количество)
            FROM sales
            GROUP BY EXTRACT('hour' FROM период)
            ORDER BY SUM(количество) DESC
        """
        # группирует продажи по дням недели и
        # выводит день с наибольшим значением
        query_2 = """SELECT EXTRACT(DOW FROM период), SUM(количество)
            FROM sales
            GROUP BY EXTRACT(DOW FROM период)
            ORDER BY SUM(количество) DESC
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query_1)
                data = cur.fetchall()
                sales_time, sales_time_vol = zip(*sorted(data))
                max_sales_time = data[0][0]

                cur.execute(query_2)
                data = cur.fetchall()
                sales_day, sales_day_vol = zip(*sorted(data))
                max_sales_day = data[0][0]
                print(f'Максимальное количество продаж происходит'
                      f' в {max_sales_time} ч., '
                      f'и в {max_sales_day} день недели.')
        # Построение графиков
        plt.subplot(1, 2, 1)
        x1 = sales_time
        y1 = sales_time_vol
        plt.scatter(x1, y1, color='green', label='по часам')
        plt.legend(loc=0)

        plt.subplot(1, 2, 2)
        x2 = sales_day
        y2 = sales_day_vol
        plt.scatter(x2, y2, color='red', label='по дням недели')
        plt.legend(loc=0)
        # вывод на экран
        plt.show()

        conn.close()

    @time_of_function
    def assign_class_to_product(self):
        """ Разделяет товары на 3 класса по количеству продаж
        (относительно квантилей 0.3 и 0.9),
         сохраняет результат в таблицу БД 'product_class'
         и excel-файл 'product_class'"""

        # Запрос необходимых данных из таблицы 'sales'
        query = """SELECT номенклатура, SUM(количество)
            FROM sales
            GROUP BY номенклатура
            ORDER BY SUM(количество)
        """
        with psycopg2.connect(database=self.db_name,
                              **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)

                # применение функции разделения товаров на классы,
                # запись в DataFrame
                result_df = divide_products_to_classes(cur.fetchall())

                # сохранение полученной таблицы в БД
                output = io.StringIO()
                result_df.to_csv(output, sep='\t',
                                 header=False, index=False)
                output.seek(0)
                cur.copy_from(output, 'product_class')

                # сохранение полученной таблицы в excel-файл
                result_df.to_excel(EXCEL_PATH, index=False)

        conn.close()
