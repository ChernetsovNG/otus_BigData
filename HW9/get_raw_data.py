import csv
import json
import logging
import os
import shutil
import tarfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Разархивируем .json файлы с данными
def extract_files():
    logger.info("Extracting .json files from archive")
    tar = tarfile.open("data/users_posts.tar.gz", "r:gz")
    files_count = 0
    for item in tar:
        tar.extract(item, "data/extract")
        files_count += 1
    logger.info("Extracted %d files" % files_count)


# Получаем список всех точек
def get_data_points():
    logger.info("Load data points from separate .json files")

    data_points = []  # список словарей, содержащих данные о пользователе и его посты

    users_id_age = {}
    users_info = {}
    users_posts = {}

    with open("data/users_id.json", "r") as file1:
        users_id_age = json.load(file1)

    with open("data/users_info.json", "r") as file2:
        users_info = json.load(file2)

    for filename in os.listdir("data/extract"):
        with open("data/extract/" + filename, "r") as file:
            posts = json.load(file)
            for user_id, posts_list in posts.items():
                all_user_posts = '\n'.join(posts_list)
                users_posts[user_id] = all_user_posts

    user_count = 0
    for user_id, user_posts in users_posts.items():
        if user_posts == "":  # если постов нет
            continue

        if not user_id in users_id_age:
            continue
        user_age = users_id_age[user_id]

        if not user_id in users_info:
            continue
        user_info = users_info[user_id]

        user_count += 1

        data_point = {'id': user_id,
                      'first_name': user_info['first_name'] if 'first_name' in user_info else None,
                      'last_name': user_info['last_name'] if 'last_name' in user_info else None,
                      'age': user_age,
                      'bdate': user_info['bdate'] if 'bdate' in user_info else None,
                      'sex': user_info['sex'] if 'sex' in user_info else None,
                      'country': user_info['country']['title'] if
                      ('country' in user_info and 'title' in user_info['country']) else None,
                      'city': user_info['city']['title'] if
                      ('city' in user_info and 'title' in user_info['city']) else None,
                      'relation': user_info['relation'] if 'relation' in user_info else None,
                      'university': user_info['university'] if 'university' in user_info else None,
                      'political': user_info['personal']['political'] if
                      ('personal' in user_info and 'political' in user_info['personal']) else None,
                      'langs': user_info['personal']['langs'] if
                      ('personal' in user_info and 'langs' in user_info['personal']) else None,
                      'religion': user_info['personal']['religion'] if
                      ('personal' in user_info and 'religion' in user_info['personal']) else None,
                      'life_main': user_info['personal']['life_main'] if
                      ('personal' in user_info and 'life_main' in user_info['personal']) else None,
                      'smoking': user_info['personal']['smoking'] if
                      ('personal' in user_info and 'smoking' in user_info['personal']) else None,
                      'alcohol': user_info['personal']['alcohol'] if
                      ('personal' in user_info and 'alcohol' in user_info['personal']) else None,
                      'verified': user_info['verified'] if 'verified' in user_info else None,
                      'posts': user_posts}

        data_points.append(data_point)

        if user_count % 1000 == 0:
            logger.info("Processed %d users" % user_count)

    logger.info("Loaded %d points from all .json files" % (len(data_points)))
    return data_points


# Записываем все точки в .csv файл
def write_csv_file(data_points):
    logger.info("Write all data points into one .csv file")

    # Ищем заголовки столбцов для таблицы. Берём их из точки, содержащей obuid и serverutc
    # (просматриваем несколько точек т.к. эти признаки содержатся не во всех точках)
    keys = {}
    for data_point in data_points:
        keys = data_point.keys()
        break

    with open('data/vk_users_data.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data_points)

    logger.info(".csv file successfully write")


if __name__ == '__main__':
    extract_files()
    all_data_points = get_data_points()
    write_csv_file(all_data_points)
    shutil.rmtree('data/extract')
