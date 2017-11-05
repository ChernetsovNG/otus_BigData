import logging
import requests
import re
from bs4 import BeautifulSoup
from csv import DictWriter

logger = logging.getLogger(__name__)

class Scrapper(object):
    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects

    def scrap_process(self, storage):
        s = requests.Session()
        # loading files
        # Загружаем первую страницу
        from_year = 2012
        to_year = 2018

        page = 1
        data = self.load_films_list(from_year, to_year, page, s)
        with open('./pages/page_%d.html' % (page), 'wb') as output_file:
            output_file.write(data.encode('cp1251'))

        page_count = self.get_page_count(data)  # общее кол-во страниц по 100 фильмов

        # Загружаем оставшиеся страницы
        for page in range(2, page_count + 1):
            data = self.load_films_list(from_year, to_year, page, s)
            with open('./pages/page_%d.html' % (page), 'wb') as output_file:
                output_file.write(data.encode('cp1251'))

        # Извлекаем информацию из загруженных страниц
        results = []
        for page in range(1, page_count + 1):
            HtmlFile = open('./pages/page_%d.html' % (page), 'r', encoding='cp1251')
            source_code = HtmlFile.read()
            results.extend(self.get_films_info(source_code))  # добавляем в список результаты извлечения данных из HTML

        # записываем результат в .txt файл
        keys = results[0].keys()
        with open('scrapped_data.txt', "w") as file:
            dict_writer = DictWriter(file, keys, delimiter="\t")
            dict_writer.writeheader()
            for raw in results:
                dict_writer.writerow(raw)


    def get_page_count(self, data):
        soup = BeautifulSoup(data)
        result = soup.find("span", {"class": "search_results_topText"})  # сколько всего найдено результатов (их по 100 на каждой странице)
        films_count = int(re.findall('(\d+)', result.text)[0])
        pages_count = films_count//100 + 1
        return pages_count

    def load_films_list(self, from_year, to_year, page, session):
        # Фильмы США с рейтингом R с одного года по другой
        url = 'https://www.kinopoisk.ru/s/type/film/list/1/order/rating/m_act%%5Bfrom_year%%5D/%d/m_act%%5Bto_year%%5D/%d' \
              '/m_act%%5Bcountry%%5D/1/m_act%%5Bmpaa%%5D/R/m_act%%5Btype%%5D/film/page/%d/' % (from_year, to_year, page)
        request = session.get(url)
        return request.text

    def get_films_info(self, data):
        results = []
        soup = BeautifulSoup(data)
        films_table = soup.find('div', {'class': 'search_results'})
        # Извлекаем информацию о фильмах на странице
        films = films_table.find_all('div', {'class': 'element'})
        for film in films:
            film_right = film.find('div', {'class': 'right'})
            film_info = film.find('div', {'class': 'info'})

            rating = 0.0    # есть фильмы без рейтинга, для них будем использовать 0
            vote_count = 0
            rating_info = film_right.find('div', {'class': 'rating'})
            if (rating_info is not None):
                rating_title = rating_info['title']
                rating = float(re.findall("\d+\.\d+", rating_title)[0])

                vote_count_str = rating_title[rating_title.find("(")+1:rating_title.find(")")]  # текст в скобочках
                vote_count_str = "".join(vote_count_str.split())  # удаляем из числа все пробельные символы

                vote_count = int(vote_count_str)

            name_rus_and_year = film_info.find('p', {'class': 'name'})  # название на русском и год выхода

            name_rus = name_rus_and_year.find('a').text
            film_year = int(name_rus_and_year.find('span', {'class': 'year'}).text)

            film_attributes = film_info.findAll('span', {'class': 'gray'})

            eng_name_and_duration = film_attributes[0].text.split(', ')  # название на англ. и продолжительность

            name_eng = eng_name_and_duration[0]  # название на английском

            # у некоторых фильмов длительность не указана
            if len(eng_name_and_duration) == 2:
                duration_min = int(re.findall('(\d+)', eng_name_and_duration[1])[0])  # продолжительности в минутах
            else:
                duration_min = 0

            # имя режисёра
            director = ""
            director_info = film_attributes[1].find('i', {'class': 'director'})
            if director_info is not None:  # есть фильмы, где режиссёр не указан
                director = director_info.find('a').text

            actors_info = film_attributes[2].findAll('a')

            actors = []
            for actor in actors_info:
                if actor.text != '...':
                    actors.append(actor.text)

            results.append({
                'name_rus': name_rus,
                'name_eng': name_eng,
                'year': film_year,
                'duration_min': duration_min,
                'director': director,
                'main_actors': actors,
                'rating': rating,
                'vote_count': vote_count
            })

        return results













































