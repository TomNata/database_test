import time

import numpy as np
import pandas as pd


def time_of_function(function):
    """Функция-декоратор для фиксирования
    времени работы скриптов."""
    def wrapped(*args, **kwargs):
        start_time = time.perf_counter()
        res = function(*args, **kwargs)
        elapsed = time.perf_counter() - start_time
        print(f'Время выполнения: {elapsed:0.4}')
        return res
    return wrapped


def divide_products_to_classes(data, columns=('номенклатура', 'количество'), quantiles=(.3, .9)):
    """Присваивает товару класс в зависимости от количества продаж
    """

    # сохранение данных в массив Pandas
    df = pd.DataFrame(data, columns=columns)

    # преобразование элементов столбца к типу 'float64'
    df['количество'] = pd.to_numeric(df['количество'], errors="coerce")

    # вычисление квантилей
    low_quantile, high_quantile = df['количество'].quantile(quantiles)

    # сортировка строк относительно вычисленных квантилей и
    # присвоение значения класса товару в новый столбец 'КлассТовара'
    df.loc[df['количество'].le(low_quantile), 'КлассТовара'] \
        = 'Наименее продаваемый'
    df.loc[df['количество'].gt(high_quantile), 'КлассТовара'] \
        = 'Наиболее продаваемый'
    df.loc[df['количество'].gt(low_quantile)
           & df['количество'].le(high_quantile), 'КлассТовара'] \
        = 'Средне продаваемый'

    # редактирование итоговой таблицы: удаление столбца
    # 'количество' и добавление 'id'
    result_df = df.drop(columns='количество')
    result_df.insert(0, 'id', np.arange(0, len(df)))

    return result_df
