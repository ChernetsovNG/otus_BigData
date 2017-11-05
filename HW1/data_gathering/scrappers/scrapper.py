import logging
import requests
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)

class Scrapper(object):
    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects

    def scrap_process(self, storage):
        s = requests.Session()
        # loading files
        # Загружаем первую страницу
        page = 1
        data = self.load_films_list(2017, 2018, page, s)
        with open('./page_%d.html' % (page), 'wb') as output_file:
            output_file.write(data.encode('cp1251'))

        page_count = self.get_page_count(data)  # общее кол-во страниц по 100 фильмов

        # Загружаем оставшиеся страницы
        for page in range(2, page_count + 1):
            data = self.load_films_list(2017, 2018, page, s)
            with open('./page_%d.html' % (page), 'wb') as output_file:
                output_file.write(data.encode('cp1251'))
        # storage.write_data([url + '\t' + data.replace('\n', '')])

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

    def contain_movies_data(self, text):
        soup = BeautifulSoup(text)
        result = soup.find("span", {"class": "search_results_topText"})