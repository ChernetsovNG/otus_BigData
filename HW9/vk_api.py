import json
import logging
from pathlib import Path

import requests

import os
import errno

logger = logging.getLogger(__name__)


# def encoded_dict(in_dict):
#     out_dict = {}
#     for k, v in in_dict.iteritems():
#         if isinstance(v, unicode):
#             v = v.encode('utf8')
#         elif isinstance(v, str):
#             v.decode('utf8')
#         out_dict[k] = v
#     return out_dict

def encoded_dict(in_dict):
    out_dict = {}
    for k, v in iter(in_dict.items()):
        out_dict[k] = v
    return out_dict


class VKApiConnector(object):
    __base_url = "https://api.vk.com/method/"
    __v = 5.69
    __resolve_screen_name_method = "utils.resolveScreenName"
    __wall_get_method = "wall.get"
    __users_search_method = "users.search"
    __users_get_method = "users.get"
    __token = NotImplemented
    __client_id = NotImplemented
    __sleep_time = NotImplemented
    __users_id_age = {}
    __users_info = {}
    __users_posts = {}
    __users_additional = {}
    __users_status = {}

    @classmethod
    def config(cls, version, client_id, token, sleep_time=1):
        cls.version = version
        cls.__sleep_time = sleep_time
        cls.__client_id = client_id
        cls.__token = token

    @classmethod
    def __get_base_params(cls):
        return {
            'v': cls.__v,
            'client_id': cls.__client_id,
            'access_token': cls.__token
        }

    @classmethod
    def get_random_users(cls, bot, chat_id):
        logger.info("Access {} method".format(cls.__users_search_method))
        try:
            age_from = 0
            delta_age = 4
            while age_from < 110:
                age_to = age_from + delta_age
                age_middle = (age_from + age_to) * 1.0 / 2

                items = cls.__get_1000_random_users(sex=0, age_from=age_from, age_to=age_to)
                if items is not None:
                    items_any = items['items']
                else:
                    items_any = []

                items = cls.__get_1000_random_users(sex=1, age_from=age_from, age_to=age_to)
                if items is not None:
                    items_women = items['items']
                else:
                    items_women = []

                items = cls.__get_1000_random_users(sex=2, age_from=age_from, age_to=age_to)
                if items is not None:
                    items_men = items['items']
                else:
                    items_men = []

                # запоминаем примерный (средний) возраст
                for item in items_any: cls.__users_id_age[item['id']] = age_middle
                for item in items_women: cls.__users_id_age[item['id']] = age_middle
                for item in items_men: cls.__users_id_age[item['id']] = age_middle

                age_from = age_to

            bot.send_message(chat_id=chat_id, text='OK. I got id and approximate age for ' +
                                                   str(len(cls.__users_id_age)) + ' random VK users')
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def __get_1000_random_users(cls, sex=2, age_from=None, age_to=None):
        request_params = cls.__get_base_params()
        request_params['count'] = 1000
        request_params['sex'] = sex

        if age_from is not None:
            request_params['age_from'] = age_from

        if age_to is not None:
            request_params['age_to'] = age_to

        url = '{}{}'.format(cls.__base_url, cls.__users_search_method)

        try:
            response = requests.post(url, encoded_dict(request_params) if request_params else None)
            if not response.ok:
                logger.error(response.text)
                return None
            else:
                response_json = response.json()
                if 'response' in response_json:
                    return response.json()['response']
                else:
                    return None
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def get_random_users_info(cls, bot, chat_id):
        try:
            random_users_id_chunks = cls.__chunks(list(cls.__users_id_age.keys()), 1000)
            for chunk in random_users_id_chunks:
                users_info = cls.__get_users_info(chunk)
                if users_info is not None:
                    for user_info in users_info:
                        cls.__users_info[user_info['id']] = user_info

            bot.send_message(chat_id=chat_id,
                             text='OK. I got info for ' + str(len(cls.__users_info)) + ' random VK users')
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def __get_users_info(cls, users_ids):
        request_params = cls.__get_base_params()
        request_params['user_ids'] = str(users_ids).strip('[]').replace(" ", "")
        request_params['fields'] = 'verified,sex,bdate,city,country,education,relation,followers_count,personal'

        url = '{}{}'.format(cls.__base_url, cls.__users_get_method)

        try:
            response = requests.post(url, encoded_dict(request_params) if request_params else None)
            if not response.ok:
                logger.error(response.text)
                return None
            else:
                return response.json()['response']
        except Exception as ex:
            logger.exception(ex)

    # Разбить список на части размера n
    @classmethod
    def __chunks(cls, l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @classmethod
    def get_random_users_posts(cls, bot, chat_id):
        user_count = 0
        number = 0
        user_posts = {}

        try:
            for user_id in cls.__users_id_age.keys():
                user_wall = cls.get_wall(user_id)
                if (user_wall is None) or (not user_wall) or (not user_wall['items']):
                    continue
                else:
                    items = user_wall['items']
                    items_text = []
                    for item in items:
                        item_text = item['text']
                        if item_text != "":  # добавляем, только если есть какой-то текст
                            items_text.append(item_text)

                    cls.__users_posts[user_id] = items_text
                    user_posts[user_id] = items_text

                user_count += 1

                # сохраняем посты по 500 пользователей (т.к. это очень долгий процесс, и файлы большие)
                if user_count % 500 == 0:
                    bot.send_message(chat_id=chat_id,
                                     text='I got posts for ' +
                                          str(user_count) + ' users. Saving posts for 500 users into file')
                    number += 1
                    cls.save_users_posts(user_posts, number)
                    user_posts.clear()

            # сохраняем в файл последние накопившиеся записи
            cls.save_users_posts(user_posts, number + 1)
            user_posts.clear()
            bot.send_message(chat_id=chat_id, text='OK. I finish work')
        except Exception as ex:
            logger.exception(ex)

    # получаем дополнительные параметры для пользователей
    @classmethod
    def get_random_users_additional(cls, bot, chat_id):
        try:
            # берём уже собранные ранее id пользователей
            with open("data/users_id.json", "r") as file1:
                users_id_age = json.load(file1)
            users_id = list(users_id_age.keys())
            random_users_id_chunks = cls.__chunks(users_id, 1000)
            for chunk in random_users_id_chunks:
                users_additional = cls.__get_users_additional(chunk)
                if users_additional is not None:
                    for user_additional in users_additional:
                        cls.__users_additional[user_additional['id']] = user_additional

            bot.send_message(chat_id=chat_id,
                             text='OK. I got additional data (interests, movies, music, tv) for ' + str(len(cls.__users_additional)) + ' random VK users')
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def __get_users_additional(cls, users_ids):
        request_params = cls.__get_base_params()
        request_params['user_ids'] = ",".join(users_ids)
        request_params['fields'] = 'interests,movies,music,tv'

        url = '{}{}'.format(cls.__base_url, cls.__users_get_method)

        try:
            response = requests.post(url, encoded_dict(request_params) if request_params else None)
            if not response.ok:
                logger.error(response.text)
                return None
            else:
                return response.json()['response']
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def resolve_screen_name(cls, screen_name):
        try:
            logger.info("Access {} method".format(cls.__resolve_screen_name_method))
            request_params = cls.__get_base_params()
            request_params['screen_name'] = screen_name

            url = '{}{}'.format(cls.__base_url, cls.__resolve_screen_name_method)
            response = requests.post(url, encoded_dict(request_params) if request_params else None)

            if not response.ok:
                logger.error(response.text)
                return

            return response.json()['response']
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def get_wall(cls, owner_id):
        request_params = cls.__get_base_params()
        request_params['owner_id'] = owner_id
        request_params['count'] = 100  # Получаем со стены пользователя 100 записей

        url = '{}{}'.format(cls.__base_url, cls.__wall_get_method)

        try:
            response = requests.post(url, encoded_dict(request_params) if request_params else None)
            if not response.ok:
                logger.error(response.text)
                return None
            else:
                return response.json()['response']
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def save_users_id(cls):
        home = str(Path.home())
        filename = home + '/VKApi/users_id.json'
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        file = open(filename, 'w+')
        json.dump(cls.__users_id_age, file)
        return filename

    @classmethod
    def save_users_info(cls):
        home = str(Path.home())
        filename = home + '/VKApi/users_info.json'
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        file = open(filename, 'w+')
        json.dump(cls.__users_info, file)
        return filename

    @classmethod
    def save_users_additional(cls):
        home = str(Path.home())
        filename = home + '/VKApi/users_additional.json'
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        file = open(filename, 'w+')
        json.dump(cls.__users_additional, file)
        return filename

    @classmethod
    def save_users_posts(cls, posts, number):
        home = str(Path.home())
        filename = home + '/VKApi/users_posts/users_posts_' + str(number) + '.json'
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        file = open(filename, 'w+')
        json.dump(posts, file)
        return filename

    # @classmethod
    # def get_random_users_posts(cls):
    #     session = FuturesSession(executor=ThreadPoolExecutor(max_workers=10))
    #
    #     request_params = cls.__get_base_params()
    #     request_params['count'] = 100  # Получаем со стены пользователя 100 записей
    #
    #     posts_count = 0  # общее количество постов
    #
    #     future_response_dict = {}
    #
    #     try:
    #         requests_count = 0
    #
    #         for user_id in cls.__users_id:
    #             request_params['owner_id'] = user_id
    #
    #             url = '{}{}'.format(cls.__base_url, cls.__wall_get_method)
    #             future_response_dict[user_id] = session.post(url, encoded_dict(request_params) if request_params else None)
    #
    #             requests_count += 1
    #
    #             if requests_count % 10 == 0:
    #                 time.sleep(0.010)
    #
    #         for user_id, future_response in future_response_dict.items():
    #             response = future_response.result()
    #             if not response.ok:
    #                 logger.error(response.text)
    #                 continue
    #             else:
    #                 user_wall = response.json()['response']
    #                 if not user_wall or not user_wall['items']:
    #                     continue
    #                 else:
    #                     posts_count += len(user_wall['items'])
    #                     cls.__users_posts[user_id] = user_wall['items']
    #
    #         return len(cls.__users_info), posts_count
    #     except Exception as ex:
    #         logger.exception(ex)
