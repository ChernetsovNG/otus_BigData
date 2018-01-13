# coding=utf-8
"""
Код работает на python2.7

Запуск приложения локально:
python main.py

Запуск приложения как сервис (использую supervisorctl, возможны другие
варианты, для работы нужна настройка конфига):
supervisorctl start vkstat

Для работы приложения необходимо скопировать файл vkstat_example.cfg в файл vkstat.cfg
Путь до конфига можно исправить

В конфиг нужно добавить токен бота и токен приложения ВК

Бота регистрировать через @botfather прямо в телеграме

Приложение в ВК регистрировать на странице https://vk.com/apps?act=manage
Получение токена для приложения ВК:
https://vk.com/dev/implicit_flow_user

Рекомендую токен получать бессрочный (обратить внимание на scope)

НЕЛЬЗЯ КОММИТИТЬ КОНФИГ В ГИТ РЕПОЗИТОРИЙ
не забывайте правильно настраивать .gitignore или использовать другие пути для конфига (что предпочтительнее)
(и вообще никогда никакие ключи не добавляйте в гит)
"""

import configparser
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

from bot_handlers import start, text, command
from vk_api import VKApiConnector

config = configparser.ConfigParser()
config.read('vkstat.cfg')
token = config.get('VKStat', 'token')
vk_api_v = config.get('VKApi', "v")
vk_api_token = config.get('VKApi', 'token')
client_id = config.get('VKApi', 'client_id')
VKApiConnector.config(vk_api_v, client_id, vk_api_token)

updater = Updater(token=token)
dispatcher = updater.dispatcher
start_handler = CommandHandler('start', start)
dispatcher.add_handler(start_handler)
text_handler = MessageHandler(Filters.text, text)
dispatcher.add_handler(text_handler)
command_handler = MessageHandler(Filters.command, command)
dispatcher.add_handler(command_handler)

if __name__ == '__main__':
    updater.start_polling()
    updater.idle()
