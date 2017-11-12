import logging
import tarfile
import os
import json
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Разархивируем .json файлы с данными
def extract_files():
    logger.info("Extracting .json files from archive")
    tar = tarfile.open("result_short_teledata.tar.gz", "r:gz")
    files_count = 0
    for item in tar:
        tar.extract(item, "extract")
        files_count += 1
    logger.info("Extracted %d files" % files_count)


# Получаем список всех точек
def get_all_data_points():
    logger.info("Load data points from separate .json files")

    data_points = []
    for filename in os.listdir("extract"):
        with open("extract/" + filename, "r") as file:
            points_list = json.load(file)
            data_points.extend(points_list)

    logger.info("Loaded %d points from all .json files" % (len(data_points)))
    return data_points


# Записываем все точки в .csv файл
def write_csv_file(data_points):
    logger.info("Write all data points into one .csv file")

    # Ищем заголовки столбцов для таблицы. Берём их из точки, содержащей obuid и serverutc
    # (просматриваем несколько точек т.к. эти признаки содержатся не во всех точках)
    keys = {}
    for data_point in data_points:
        if ('obuid' not in data_point.keys()) or ('serverutc' not in data_point.keys()):
            continue
        else:
            keys = data_point.keys()
            break

    with open('raw_data.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data_points)

    logger.info(".csv file successfully write")


if __name__ == '__main__':
    extract_files()
    all_data_points = get_all_data_points()
    write_csv_file(all_data_points)
    print("Hello!")
