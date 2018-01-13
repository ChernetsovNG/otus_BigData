import re
from urllib.parse import urlparse

from constants import START_COMMAND_TEXT, NOT_A_VK_LINK, ERROR
from visualisation import plot_hist_buffer, items_stats
from vk_api import VKApiConnector

vk_profile_link_validator = re.compile(r"^https?://(www.)?(m.)?(vkontakte.ru|vk.com)/.*$")


def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text=START_COMMAND_TEXT)


def command(bot, update):
    receive_command = update.message.text

    bot.send_message(chat_id=update.message.chat_id,
                     text='I receive command: ' + receive_command + ' Please wait until processing.')

    if receive_command == '/get_users_id':
        VKApiConnector.get_random_users(bot, update.message.chat_id)  # множество id пользователей
    elif receive_command == '/get_users_info':
        VKApiConnector.get_random_users_info(bot, update.message.chat_id)
    elif receive_command == '/get_users_posts':
        VKApiConnector.get_random_users_posts(bot, update.message.chat_id)
    elif receive_command == '/get_users_additional':
        VKApiConnector.get_random_users_additional(bot, update.message.chat_id)
    elif receive_command == '/save_users_id':
        filename = VKApiConnector.save_users_id()
        bot.send_message(chat_id=update.message.chat_id, text='OK. File saved: ' + filename)
    elif receive_command == '/save_users_info':
        filename = VKApiConnector.save_users_info()
        bot.send_message(chat_id=update.message.chat_id, text='OK. File saved: ' + filename)
    elif receive_command == '/save_users_additional':
        filename = VKApiConnector.save_users_additional()
        bot.send_message(chat_id=update.message.chat_id, text='OK. File saved: ' + filename)


def text(bot, update):
    entered_text = update.message.text
    if not vk_profile_link_validator.match(entered_text):
        answer = NOT_A_VK_LINK
    else:
        parse_result = urlparse(entered_text)
        path = parse_result.path.lstrip('/')  # удаляем из пути символ /

        data = VKApiConnector.resolve_screen_name(path)
        if data is not None:
            if not data:
                answer = NOT_A_VK_LINK
            else:
                object_id = data['object_id']
                if data['type'] == 'group':
                    object_id = -object_id

                wall = VKApiConnector.get_wall(object_id)
                if not wall or not wall['items']:
                    answer = ERROR
                else:
                    stats = items_stats(wall['items'])
                    buf = plot_hist_buffer(stats)
                    bot.send_photo(chat_id=update.message.chat_id, photo=buf)
                    buf.close()
                    return

        else:
            answer = ERROR

    bot.send_message(chat_id=update.message.chat_id, text=answer)
