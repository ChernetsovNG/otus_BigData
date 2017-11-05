"""
Зачем нужны __init__.py файлы
https://stackoverflow.com/questions/448271/what-is-init-py-for

Про документирование в Python проекте
https://www.python.org/dev/peps/pep-0257/

Про оформление Python кода
https://www.python.org/dev/peps/pep-0008/


Примеры сбора данных:
https://habrahabr.ru/post/280238/

Для запуска тестов в корне проекта:
python3 -m unittest discover

Для запуска проекта из корня проекта:
python3 -m gathering gather
или
python3 -m gathering transform
или
python3 -m gathering stats


Для проверки стиля кода всех файлов проекта из корня проекта
pep8 .


ЗАДАНИЕ

Выбрать источник данных и собрать данные по некоторой предметной области.

Цель задания - отработать навык написания программ на Python.
В процессе выполнения задания затронем области:
- организация кода в виде проекта, импортирование модулей внутри проекта
- unit тестирование
- работа с файлами
- работа с протоколом http
- работа с pandas
- логирование

Требования к выполнению задания:

- собрать не менее 1000 объектов

- в каждом объекте должно быть не менее 5 атрибутов
(иначе просто будет не с чем работать.
исключение - вы абсолютно уверены что 4 атрибута в ваших данных
невероятно интересны)

- сохранить объекты в виде csv файла

- считать статистику по собранным объектам


Этапы:

1. Выбрать источник данных.

Это может быть любой сайт или любое API

Примеры:
- Пользователи vk.com (API)
- Посты любой популярной группы vk.com (API)
- Фильмы с Кинопоиска
(см. ссылку на статью выше)
- Отзывы с Кинопоиска
- Статьи Википедии
(довольно сложная задача,
можно скачать дамп википедии и распарсить его,
можно найти упрощенные дампы)
- Статьи на habrahabr.ru
- Объекты на внутриигровом рынке на каком-нибудь сервере WOW (API)
(желательно англоязычном, иначе будет сложно разобраться)
- Матчи в DOTA (API)
- Сайт с кулинарными рецептами
- Ebay (API)
- Amazon (API)
...

Не ограничивайте свою фантазию. Это могут быть любые данные,
связанные с вашим хобби, работой, данные любой тематики.
Задание специально ставится в открытой форме.
У такого подхода две цели -
развить способность смотреть на задачу широко,
пополнить ваше портфолио (вы вполне можете в какой-то момент
развить этот проект в стартап, почему бы и нет,
а так же написать статью на хабр(!) или в личный блог.
Чем больше у вас таких активностей, тем ценнее ваша кандидатура на рынке)

2. Собрать данные из источника и сохранить себе в любом виде,
который потом сможете преобразовать

Можно сохранять страницы сайта в виде отдельных файлов.
Можно сразу доставать нужную информацию.
Главное - постараться не обращаться по http за одними и теми же данными много раз.
Суть в том, чтобы скачать данные себе, чтобы потом их можно было как угодно обработать.
В случае, если обработать захочется иначе - данные не надо собирать заново.
Нужно соблюдать "этикет", не пытаться заддосить сайт собирая данные в несколько потоков,
иногда может понадобиться дополнительная авторизация.

В случае с ограничениями api можно использовать time.sleep(seconds),
чтобы сделать задержку между запросами

3. Преобразовать данные из собранного вида в табличный вид.

Нужно достать из сырых данных ту самую информацию, которую считаете ценной
и сохранить в табличном формате - csv отлично для этого подходит

4. Посчитать статистики в данных
Требование - использовать pandas (мы ведь еще отрабатываем навык использования инструментария)
То, что считаете важным и хотели бы о данных узнать.

Критерий сдачи задания - собраны данные по не менее чем 1000 объектам (больше - лучше),
при запуске кода командой "python3 -m gathering stats" из собранных данных
считается и печатается в консоль некоторая статистика

Код можно менять любым удобным образом
Можно использовать и Python 2.7, и 3

"""

import logging
import sys
import pandas as pd
import numpy as np

from scrappers.scrapper import Scrapper
from storages.file_storage import FileStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'


def gather_process():
    logger.info("gather")
    storage = FileStorage(SCRAPPED_FILE)

    # You can also pass a storage
    scrapper = Scrapper()
    scrapper.scrap_process(storage)


def convert_data_to_table_format():
    logger.info("transform")
    # Читаем данные из .txt файла и сохраняем в .csv файле
    data = pd.read_csv(SCRAPPED_FILE, sep="\t")
    data.to_csv(TABLE_FORMAT_FILE, sep='\t')

def stats_of_data():
    logger.info("stats")
    data = pd.read_csv(TABLE_FORMAT_FILE, sep="\t")
    print(data.info())  # общая информация о данных

    data_non_zero_rating = data[data['rating'] > 0.000]
    data_rating_more_six = data[data['rating'] >= 6.000]
    data_non_zero_duration = data[data['duration_min'] > 0]
    data_director_is_present = data[data['director'] != ""]

    # разбиение фильмов по годам (сколько фильмов было каждый год)
    print('Количество фильмов по годам:')
    print(data.groupby(['year'])['name_rus'].count())

    print('Количество фильмов с рейтингом >= 6.0 по годам:')
    print(data_rating_more_six.groupby(['year'])['name_rus'].count())

    # средний рейтинг фильмов данной категории (R) по годам
    print('Средний рейтинг фильмов по годам (не учитываем фильмы с невыставленным рейтингом)')

    print(data_non_zero_rating.groupby('year', as_index=False)['rating'].mean())

    # среднее количество голосов за фильмы с рейтингом
    print('Среднее количество голосов за фильмы с рейтингом (не учитываем фильмы с невыставленным рейтингом)')
    print(data_non_zero_rating.groupby('year', as_index=False)['vote_count'].mean())
    print('Среднее количество голосов за фильмы с рейтингом >= 6.0 (не учитываем фильмы с невыставленным рейтингом)')
    print(data_rating_more_six.groupby('year', as_index=False)['vote_count'].mean())

    # продолжительность фильмов
    print("Минимальная, максимальная и средняя продолжительность фильмов (в минутах)")
    print("Min:")
    print(data_non_zero_duration.groupby(['year'])['duration_min'].min())
    print("Average:")
    print(data_non_zero_duration.groupby(['year'])['duration_min'].mean())
    print("Max:")
    print(data_non_zero_duration.groupby(['year'])['duration_min'].max())

    # множество режиссёров (без повторений)
    print("Режиссёры фильмов")
    directors = set(data_director_is_present['director'])
    print("Общее количество режиссёров")
    print(len(directors))
    print("Первые 15 режиссёров, у которых больше одного фильма в списке")
    data_group_by_director = data_director_is_present.groupby(['director'])
    count_film_by_director = data_group_by_director['name_rus'].count().reset_index(name="count")

    print(count_film_by_director[count_film_by_director['count'] > 1][['director', 'count']].sort_values(by=['count'], ascending = [0]).head(15))


def main():
    print("Hello")


if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    logger.info("Work started")

    gather_process()
    convert_data_to_table_format()
    stats_of_data()

    logger.info("work ended")