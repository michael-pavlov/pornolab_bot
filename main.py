# -*- coding: utf-8 -*-
# @Author Michael Pavlov
#
# version 1.00 2019-04-05
# первая версия
# version 1.10 2019-06-18
# первая рабочая версия
# version 1.20 2019-06-19
# +история поиска, +фикс количество файлов
# version 1.33 2019-06-21
# управление фильтрами и path
# version 1.34 2019-06-21
# поиск по произвольному тексту
# настройки, описание
# version 1.35 2019-06-28
# new connection pool
# bugs with search command
# version 1.36 2019-07-03
# donate, description

# TODO
# "/subscribe", "/lucky"
# ограничения на поиск
# отдавать ссылки без файла

import os
import telebot
from flask import Flask, request
import mysql.connector
from mysql.connector import Error
from mysql.connector.connection import MySQLConnection
from mysql.connector import pooling
import logging
import time
import sys
import math
import config
from logging.handlers import RotatingFileHandler

VERSION = "1.36"

class PlabBot:

    def __init__(self, env = 'heroku', mode = 'online'):

        self.env = env

        self.logger = logging.getLogger("Plab_Bot")
        self.logger.setLevel(logging.INFO)

        if self.env == 'heroku':
            handler = logging.StreamHandler(sys.stdout)
            # handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(name)s: %(levelname)s: %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            self.TG_BOT_TOKEN = os.environ['TOKEN']
            self.HEROKU_NAME = os.environ['HEROKU_NAME'] #'pornolab-bot'
            self.DB_USER = os.environ['DB_USER']
            self.DB_PASSWORD = os.environ['DB_PASSWORD']
            self.DB_HOST = os.environ['DB_HOST']
            self.DB_PORT = os.environ['DB_PORT']
            self.DB_DATABASE = "bots"
            self.TMP_PATH = ""

            self.GLOBAL_RECONNECT_COUNT = int(os.environ['GLOBAL_RECONNECT_COUNT'])

            self.bot = telebot.TeleBot(self.TG_BOT_TOKEN)

            # Настройка Flask
            self.server = Flask(__name__)
            self.TELEBOT_URL = 'telebot_webhook/'
            self.BASE_URL = "https://" + self.HEROKU_NAME + ".herokuapp.com/"

            self.server.add_url_rule('/' + self.TELEBOT_URL + self.TG_BOT_TOKEN, view_func=self.process_updates,
                                     methods=['POST'])
            self.server.add_url_rule("/", view_func=self.webhook)

        elif self.env == 'local':
            handler = RotatingFileHandler("plab_bot.log", mode='a', encoding='utf-8', backupCount=5,
                                     maxBytes=16 * 1024 * 1024)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            self.TG_BOT_TOKEN = config.TG_BOT_TOKEN
            self.DB_USER = config.DB_USER
            self.DB_PASSWORD = config.DB_PASSWORD
            self.DB_HOST = config.DB_HOST
            self.DB_PORT = config.DB_PORT
            self.DB_DATABASE = config.DB_DATABASE
            self.TMP_PATH = "C:\\log\\pbot_searches\\"

            self.GLOBAL_RECONNECT_COUNT = int(config.GLOBAL_RECONNECT_COUNT)

            self.bot = telebot.TeleBot(self.TG_BOT_TOKEN)
            telebot.apihelper.proxy = config.PROXY
        else:
            print("PlabBot() Exit! Unknown environment:" + str(env))
            quit()

        # common operations
        self.reconnect_count = self.GLOBAL_RECONNECT_COUNT
        self.GLOBAL_RECONNECT_INTERVAL = 5
        self.RECONNECT_ERRORS = []
        self.ADMIN_ID = '211558'
        self.MAIN_HELP_LINK = "https://telegra.ph/Bot-dlya-poiska-po-pornolabnet-06-21"

        self.markup_commands = ["/help", "/search", "/usage", "/settings", "/donate"]

        # привязываем хенделер сообщений к боту:
        self.bot.set_update_listener(self.handle_messages)
        handler_dic = self.bot._build_handler_dict(self.handle_callback_messages)
        # привязываем хенделр колбеков inline-клавиатуры к боту:
        self.bot.add_callback_query_handler(handler_dic)

        try:
            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="my_pool",
                                                                          pool_size=32,
                                                                          pool_reset_session=True,
                                                                          host=self.DB_HOST, port=self.DB_PORT,
                                                                          database=self.DB_DATABASE,
                                                                          user=self.DB_USER,
                                                                          password=self.DB_PASSWORD)

            connection_object = self.connection_pool.get_connection()

            if connection_object.is_connected():
                db_Info = connection_object.get_server_info()
                cursor = connection_object.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()

        except Error as e:
            self.logger.critical("Error while connecting to MySQL using Connection pool ", e)
        finally:
            # closing database connection.
            if (connection_object.is_connected()):
                cursor.close()
                connection_object.close()

    def process_updates(self):
        self.bot.process_new_updates([telebot.types.Update.de_json(request.stream.read().decode("utf-8"))])
        return "!", 200

    def webhook(self):
        self.bot.remove_webhook()
        self.bot.set_webhook(url=self.BASE_URL + self.TELEBOT_URL + self.TG_BOT_TOKEN)
        return "!", 200

    # method for inserts|updates|deletes
    def db_execute(self, query, params, comment=""):
        error_code = 1
        try:
            self.logger.debug("db_execute() " + comment)
            connection_local = self.connection_pool.get_connection()
            if connection_local.is_connected():
                cursor_local = connection_local.cursor()
                result = cursor_local.execute(query, params)
                connection_local.commit()
                error_code = 0
        except mysql.connector.Error as error:
            connection_local.rollback()  # rollback if any exception occured
            print("Failed {}".format(error))
        finally:
            # closing database connection.
            if (connection_local.is_connected()):
                cursor_local.close()
                connection_local.close()
        if error_code == 0:
            return True
        else:
            return False

    # method for selects
    def db_query(self, query, params, comment=""):
        try:
            self.logger.debug("db_query() " + comment)
            connection_local = self.connection_pool.get_connection()
            if connection_local.is_connected():
                cursor_local = connection_local.cursor()
                cursor_local.execute(query, params)
                result_set = cursor_local.fetchall()

                self.logger.debug("db_query().result_set:" + str(result_set))
                if result_set is None or len(result_set) <= 0:
                    result_set = []
                cursor_local.close()
        except mysql.connector.Error as error:
            print("Failed {}".format(error))
            result_set = []
        finally:
            # closing database connection.
            if (connection_local.is_connected()):
                connection_local.close()
        return result_set

    def run(self):
        if self.env == 'heroku':
            while True:
                try:
                    self.logger.info("Server run. Version: " + VERSION)
                    self.webhook()
                    self.server.run(host="0.0.0.0", port=int(os.environ.get('PORT', 5000)))
                except Exception as e:
                    self.logger.critical("Cant start PlabBot. RECONNECT" + str(e))
                    time.sleep(2)
        if self.env == 'local':
            while True:
                try:
                    self.bot.remove_webhook()
                    self.logger.info("Server run. Version: " + VERSION)
                    self.bot.polling()
                except Exception as e:
                    self.logger.critical("Cant start PlabBot. RECONNECT " + str(e))
                    time.sleep(2)

    def command_start(self, message):
        self.logger.info("Receive Start command from chat ID:" + str(message.chat.id))
        if message.from_user.username is not None:
            user_name = message.from_user.username
        else:
            user_name = message.from_user.first_name

        if self.new_user(message.chat.id, user_name):
            self.bot.send_message(message.chat.id,  "Welcome.\n"
                                                    "Read the manual first: " + self.MAIN_HELP_LINK + "\n"
                                                    "It is non commercial project, so feel free to /donate\n"
                                                    "Support, bugs, offer feature - @m_m_pa\n"
                                                    "\n"
                                                    "Добро пожаловать.\n"
                                                    "Сначала прочитайте инструкцию: " + self.MAIN_HELP_LINK + "\n"
                                                    "Поддержите этот некоммерческий проект /donate\n"
                                                    "Вопросы, баги, предложения - @m_m_pa\n",
                                  reply_markup=self.markup_keyboard(self.markup_commands))
            self.bot.send_message(self.ADMIN_ID, "New user: " + str(user_name))
        else:
            self.bot.send_message(message.chat.id, "Welcome back " + str(message.from_user.username) + ". Tap /help",
                                  reply_markup=self.markup_keyboard(self.markup_commands))

    def new_user(self, user_id, user_name):
        if len(self.db_query("select user_id from plab_bot_users where user_id=%s", (user_id,),
                             "Check User exist")) > 0:
            return False
        # add user:
        elif self.db_execute("insert into plab_bot_users (name,user_id) values (%s,%s)", (user_name, user_id),
                             "Add new User"):
            return True
        else:
            return False

    def command_help(self, message):
        try:
            self.db_execute("update plab_bot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
            self.logger.info("Receive Help command from chat ID:" + str(message.chat.id))
            self.bot.send_message(message.chat.id, "Help:\n"
                                                   # "/help - show this message\n"
                                                   "/usage - show usage\n"
                                                   "/search - search something\n"
                                                   # "/lucky - I'm Felling Lucky\n"
                                                   "/setings - edit settings\n"
                                                   "\n"
            # "/... - ...\n"
                                                   "readme(ru) - " + self.MAIN_HELP_LINK + "\n"
                                                   "support - @m_m_pa\n\n"
                                                   "version - " + VERSION + "\n"
                                                   "\n",
                                  disable_web_page_preview=True, reply_markup=self.markup_keyboard(self.markup_commands))
        except Exception as e:
            self.logger.critical("Cant execute Help command. " + str(e))
        return

    def command_donate(self, message):
        try:
            self.db_execute("update plab_bot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
            self.logger.info("Receive Donate command from chat ID:" + str(message.chat.id))
            self.bot.send_message(message.chat.id, "*Donate project*:\n"
                                                   "PayPal: https://paypal.me/mrmichaelpavlov\n"
                                                   "VK pay: https://vk.me/moneysend/to/23QW\n"
                                                   "Mastercard: 5321 3046 4588 4500\n"
                                                   "ETH: 0x6dD6E739891D15A3dcEFF9587dECC780a3809246\n"
                                                   "BTC: 1FzUWXtViv1MtvyrgcPGSmwnusHAaUXxLM\n"
                                                   # "\n"
            # "/... - ...\n"
            #                                        "readme(ru) - " + self.MAIN_HELP_LINK + "\n"
            #                                        "support - @m_m_pa\n\n"
            #                                        "version - " + VERSION + "\n"
                                                   "\n",
                                  disable_web_page_preview=True, parse_mode='Markdown',reply_markup=self.markup_keyboard(self.markup_commands))
        except Exception as e:
            self.logger.critical("Cant execute Donate command. " + str(e))
        return

    def command_usage(self, message):
        try:
            self.logger.info("Receive Usage command from chat ID:" + str(message.chat.id))
            self.db_execute("update plab_bot_users set state = %s where user_id = %s", ("", message.chat.id),"Update State")
            self.bot.send_message(message.chat.id, "*Filters*:\n"
                                                   "tags: +<tag>, -<tag>\n"
                                                   "site: +<site>, -<site>\n"
                                                   "title: +<word(s)>, -<word(s)>\n"
                                                   "year, month, day: +<int>, -<int>\n"
                                                   "forum, subforum: +<word(s)>, -<word(s)>\n"
                                                   "limit: <int>\n"
                                                   "qa: min/max\n"
                                                   "history: true/false\n"
                                                   "save: true/false\n"
                                                   "\n"
                                                   "*Examples:*\n"
                                                   "tags: +teen,+solo,-big tits\n"
                                                   "site: +wowporn\n"
                                                   "\n"
                                                   "title: sasha grey\n"
                                                   "year: +2019, +2018\n"
                                                   "limit: 100\n"
                                                   "\n", parse_mode='Markdown', reply_markup=self.markup_keyboard(self.markup_commands))
        except Exception as e:
            self.logger.critical("Cant execute Usage command. " + str(e))
        return

    def command_stop(self, message):
        try:
            self.logger.info("Receive Stop command from chat ID:" + str(message.chat.id))
            self.bot.send_message(self.ADMIN_ID, "Stop user: " + str(message.chat.id))
            #TODO Delete from DB
        except Exception as e:
            self.logger.critical("Cant execute Stop command. " + str(e))
        return

    def broadcast(self, message):
        try:
            for item in self.db_query("select user_id from plab_bot_users", (), "Get all Users"):
                try:
                    self.bot.send_message(item[0], message)
                    self.logger.info("Successfully sent broadcast for user:" + str(item[0]))
                except Exception as err:
                    self.logger.warning("Cant send broadcast message to user:" + str(item[0]) + "; with error: " + str(err))
        except Exception as e:
            self.logger.warning("Cant send broadcast message" + str(e))

    def markup_keyboard(self, list, remove=False):
        if not remove:
            markupkeyboard = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
            markupkeyboard.add(*[telebot.types.KeyboardButton(name) for name in list])
        else:
            markupkeyboard = telebot.types.ReplyKeyboardRemove(selective=False)
        return markupkeyboard

    def inline_keyboard(self, list):
        inlinekeyboard = telebot.types.InlineKeyboardMarkup(row_width=7)
        inlinekeyboard.add(*[telebot.types.InlineKeyboardButton(text=name, callback_data=name) for name in list])
        return inlinekeyboard

    def command_search(self, message, raw_search = False):
        try:
            self.logger.info("Receive Search command from user id:" + str(message.chat.id))
            # достаем данные пользователя
            try:
                user_data = self.db_query("select fixed_filters,max_limit,browser_path,last_search_id from plab_bot_users where user_id=%s", (message.chat.id,),"Get User Data")
                user_filters = user_data[0][0]
                user_max_limit = int(user_data[0][1])
                user_browser_path = user_data[0][2]
                user_last_search_id = int(user_data[0][3])
                user_search_id = user_last_search_id + 1
            except Exception as e:
                self.logger.warning("___")
                self.bot.send_message(message.chat.id, "Error. =(\n" + str(e), reply_markup=self.markup_commands)
                return

            # парсим сообщение и строим запрос
            search_data = self.create_search_request(message.text, str(message.chat.id),user_max_limit=user_max_limit,fixed_filters=user_filters,raw_search=raw_search)
            if search_data["isvalid"]:
                # запоминаем сам запрос и обновляем last_search_id
                try:
                    global_search_id = str(message.chat.id) + "_" + str(user_search_id)
                    self.db_execute("update plab_bot_users set last_search_id = %s where user_id = %s", (user_search_id, message.chat.id), "Update search_id")
                    self.db_execute("insert into plab_bot_history (user_id, global_search_id, serach_request)  values (%s,%s,%s)", (message.chat.id, global_search_id, search_data["search_request"]), "Insert new search")

                except Exception as e:
                    self.logger.warning("Command Search() Could not save search for user:" + str(message.chat.id) + "; E:" + str(e))
                    return False

                # выполняем запрос
                urls = self.db_query(search_data["search_request"].replace("%s","%\\s"),(), "Searching for urls")
                self.logger.debug("Command Search() Found " + str(len(urls)) + " urls")
            else:
                self.logger.warning("Command Search() Error: " + str(search_data["error_message"]))
                self.bot.send_message(message.chat.id, "Error. =(\n"+ str(search_data["error_message"]), reply_markup=self.markup_commands)
                return False

            if len(urls) < 1:
                self.bot.send_message(message.chat.id, "Nothing found. =(", reply_markup=self.markup_keyboard(self.markup_commands))
                self.logger.info("Command Search() Nothing found for user:" + str(message.chat.id) + "; search id: " + str(user_search_id))
                return True

            # формируем файлы
            file_names = self.create_out_files(urls, str(message.chat.id) + "_" + str(user_search_id), browser_path=user_browser_path)

            # отправляем файлы
            for file_name in file_names:
                doc = open(file_name, 'rb')
                self.bot.send_document(message.chat.id, doc, reply_markup=self.markup_keyboard(self.markup_commands))
                doc.close()
            self.logger.info("Command Search() Sent " + str(len(urls)) + " urls in " + str(len(file_names)) + " files to user:" + str(message.chat.id) + "; search id: " + str(user_search_id))

            # запоминаем историю поиска
            if search_data.get("save") is not None:
                try:
                    for url in urls:
                        url_id = int(url[0][url[0].rfind("=") + 1:])
                        self.db_execute("insert into plab_bot_history_detailed (global_search_id, url_id) values (%s,%s)",(global_search_id, url_id), "Save search details")
                except Exception as e:
                    self.logger.warning("Command Search() Cant save search history for user: " + str(message.chat.id) + "; " + str(e))
                    pass

            # удяляем файлы
            try:
                for file_name in file_names:
                    os.remove(file_name)
            except Exception as e:
                self.logger.warning("Command Search() Could not Delete files:" + str(e))
                pass

            return True
        except Exception as err:
            self.logger.critical("Command Search() Cant execute Search command for user: " + str(message.chat.id) + "; " + str(err))
        return False

    def command_upgrade(self, message):
        try:
            self.logger.info("Receive Upgrade command from chat ID:" + str(message.chat.id))
            self.bot.send_message(self.ADMIN_ID, "Receive Upgrade command from chat ID: " + str(message.chat.id))
            self.bot.send_message(message.chat.id, "Plans:\n"
                                                   "10 urls - $10 per month\n"
                                                   "20 urls - $15 per month\n"
                                                   "dedicated instance (1-2 min delay) - ask @m_m_pa\n"
                                                   "custom - ask @m_m_pa\n"
                                                   "\n",
                                  disable_web_page_preview=True)
        except Exception as e:
            self.logger.critical("Cant execute Upgrade command. " + str(e))
        return

    def handle_messages(self, messages):
        for message in messages:
            try:
                self.bot.send_message(self.ADMIN_ID, "New message from " + str(message.chat.id) + "\n" + message.text)
                if message.reply_to_message is not None:
                    # TODO Process reply message
                    return
                if message.text.startswith("/start"):
                    self.command_start(message)
                    self.db_execute("update plab_bot_users set state = %s where user_id = %s", ("", message.chat.id),
                                    "Update State")
                    return
                if message.text.startswith("/help"):
                    self.command_help(message)
                    return
                if message.text.startswith("/donate"):
                    self.command_donate(message)
                    return
                if message.text.startswith("/usage"):
                    self.command_usage(message)
                    self.db_execute("update plab_bot_users set state = %s where user_id = %s", ("", message.chat.id),
                                    "Update State")
                    return
                if message.text.startswith("/search"):
                    if self.db_execute("update bots.plab_bot_users set state = %s where user_id = %s", ("wait_search", message.chat.id), "Update State"):
                        self.bot.send_message(message.chat.id, "Paste search request.\n"
                                                               "For examples tap /usage\n"
                                                               "\n"
                                                               "\n", reply_markup=self.markup_keyboard([], remove=True))
                    return
                if message.text.startswith("/settings"):
                    settings_command = ["Edit Filters", "Edit Path"]
                    user_data = self.db_query("select fixed_filters,browser_path from plab_bot_users where user_id=%s", (message.chat.id,),"Get User Data")
                    if user_data[0][0] is not None:
                        user_filters = user_data[0][0]
                    else:
                        user_filters = ""
                    if user_data[0][1] is not None:
                        user_browser_path = user_data[0][1]
                    else:
                        user_browser_path = ""
                    self.bot.send_message(message.chat.id,reply_markup=pBot.inline_keyboard(settings_command), parse_mode='Markdown', text="*Filters*:\n" + \
                                                                                                                    user_filters.replace("|","\n") + \
                                                                                                                    "\n\n" + \
                                                                                                                    "*Browser Path*: \n" + \
                                                                                                                    user_browser_path + \
                                                                                                                    "\n")
                    return
                if message.text.startswith("/upgrade"):
                    self.command_upgrade(message)
                    return
                if message.text.startswith("/stop"):
                    self.command_stop(message)
                    return
                if message.text.startswith("/broadcast"):
                    if int(message.chat.id) == int(self.ADMIN_ID):
                        self.broadcast(message.text.replace("/broadcast ", ""))
                    else:
                        self.bot.reply_to(message, "You are not admin")
                    return
                if message.text.startswith("/"):
                    self.bot.reply_to(message, "Unknown command. Tap /help")
                    return

                # проверка на статусы:
                state = self.db_query("select state from plab_bot_users where user_id=%s", (message.chat.id,), "Get State")[0][0]

                if state == "wait_search":
                    if self.command_search(message):
                        self.db_execute("update plab_bot_users set state = %s where user_id = %s", ("", message.chat.id),
                                        "Update State")
                    else:
                        self.bot.reply_to(message,
                                          "Not a valid format\nExample: /usage")
                    return

                if state.startswith("wait_filters"):
                    if self.if_filters_valid(message.text):
                        self.db_execute("update plab_bot_users set fixed_filters = %s where user_id = %s", (message.text.replace("\n","|"), message.chat.id),
                                        "Update Filters")
                        self.bot.reply_to(message, "Success! Filters updated. /help")
                        self.db_execute("update plab_bot_users set state = %s where user_id = %s",("", message.chat.id), "Update State")
                    else:
                        self.bot.reply_to(message, "Not a valid format \nHelp: " + self.MAIN_HELP_LINK)
                    return

                if state.startswith("wait_path"):
                    self.db_execute("update plab_bot_users set browser_path = %s where user_id = %s", ("\"" + message.text + "\"", message.chat.id),
                                    "Update Filters")
                    self.bot.reply_to(message, "Success! Path updated. /help")
                    self.db_execute("update plab_bot_users set state = %s where user_id = %s",("", message.chat.id), "Update State")
                    return

                # Если ничего не сработало, считаем что юзер вставил запрос поиска без команды
                # сначала проверим на форматный запрос
                if message.text.find(":") > 0:
                    if self.command_search(message):
                        return
                # не формат. поиск по raw_title
                if self.command_search(message, raw_search=True):
                        return

                # print(message)
                self.bot.reply_to(message, text="Tap command", reply_markup=self.markup_keyboard(self.markup_commands),
                                  parse_mode='Markdown')
            except Exception as e:
                self.logger.warning("Cant process message:" + str(message) + str(e))
                self.bot.reply_to(message, text="Unknown error. Tap command", reply_markup=self.markup_keyboard(self.markup_commands),
                                  parse_mode='Markdown')

    def handle_callback_messages(self, callback_message):
        # обязательный ответ в API
        self.bot.answer_callback_query(callback_message.id)

        # Разбор команд
        # команда ввода новых фильтров
        if callback_message.data == "Edit Filters":
            # запоминаем и ставим статус с ожиданием
            self.db_execute("update plab_bot_users set state = %s where user_id = %s",("wait_filters",callback_message.message.chat.id),"Update State")
            self.bot.send_message(callback_message.message.chat.id, "Please provide new filter list..")
            return

        # команда ввода нового Path
        if callback_message.data == "Edit Path":
            # запоминаем и ставим статус с ожиданием
            self.db_execute("update plab_bot_users set state = %s where user_id = %s",
                            ("wait_path", callback_message.message.chat.id), "Update State")
            self.bot.send_message(callback_message.message.chat.id, "Please provide new path..")
            return

        return

    def if_filters_valid(self, filters):
        if filters.find("|") >= 0:
            return False
        else:
            return True

    def parse_values(self, str_):
        return str_.split(",")

    def create_search_request(self, search_, user_id, user_max_limit = 1000, fixed_filters = "", raw_search = False):
        data = {}
        fixed_filters_dict = {}
        return_dict = {}
        return_dict["isvalid"] = False

        # possible values:
        # tags: +<tag>, -<tag>
        # site: +<site>, -<site>
        # title: +<word(s)>, -<word(s)>
        # year, month, day: +<int>, -<int>
        # forum, subforum: +<word(s)>, -<word(s)>
        # limit: <int>
        # qa: min/max
        # history: any
        # save: any

        # проверяем и добавляем персональные фильтры, если есть
        try:
            # разбиваем текст на словарь
            for line in fixed_filters.lower().replace("|","\n").split("\n"):
                key = line[0:line.find(":")].strip()
                value = line[line.find(":") + 1:].strip()
                if len(key) > 0:
                    fixed_filters_dict[key] = value
                    # системные настройки тоже могут быть в персональных фильтрах,
                    # они будут перезаписаны таковыми из основного запроса
                    if key in ['limit','qa','history','save']:
                        data[key] = value
        except Exception as e:
            self.logger.warning("Create_search_request() Error on parsing fixed filters:" + str(e))
            return_dict["error_message"] = str(e)
            return return_dict

        # парсим всю кучу
        if not raw_search:
            try:
                # разбиваем текст на словарь
                for line in search_.lower().split("\n"):
                    key = line[0:line.find(":")].strip()
                    value = line[line.find(":") + 1:].strip()
                    if len(key) > 0: data[key] = value
            except Exception as e:
                self.logger.warning("Create_search_request() Error on parsing main search:" + str(e))
                return_dict["error_message"] = str(e)
                return return_dict
        else:
            data["raw_title"] = search_.replace("\n"," ")

        try:
            # основной запрос
            search_request_main = "SELECT url from plab_engine_urls table1\n"

            # условия where
            where_condition = " where parse_ok = '1' AND "
            for key in data:
                # для строковых и неуникальных полей
                if key in ['tags','title','forum','site','subforum','raw_title']:
                    for item in self.parse_values(data[key]):
                        if item.strip().startswith("-"):
                            value_ = item.strip()[1:]
                            where_condition += key + " not like \'%" + value_ + "%\' AND\n"
                        elif item.strip().startswith("+"):
                            value_ = item.strip()[1:]
                            where_condition += key + " like \'%" + value_ + "%\' AND\n"
                        else:
                            value_ = item.strip()
                            where_condition += key + " like \'%" + value_ + "%\' AND\n"

                # для числовых полей
                # условия на включения идут через OR
                if key in ['year','month','day']:
                    where_condition += " ("
                    for item in self.parse_values(data[key]):
                        # либо явно указан +
                        if item.strip().startswith("+"):
                            value_ = item.strip()[1:]
                            where_condition += key + " = \'" + value_ + "\' OR  "
                        # либо без знака
                        elif not item.strip().startswith("-"):
                            value_ = item.strip()
                            where_condition += key + " = \'" + value_ + "\' OR  "
                    if where_condition.strip().endswith("OR"):
                        where_condition = where_condition.strip()[:-3]
                    where_condition += ") AND\n "
                # условия на исключения идут через AND
                if key in ['year','month','day']:
                    for item in self.parse_values(data[key]):
                        if item.strip().startswith("-"):
                            value_ = item.strip()[1:]
                            where_condition += key + " != \'" + value_ + "\' AND\n"

            # добавляем фиксированные фильтры
            for fixed_key in fixed_filters_dict:
                if fixed_key in ['tags', 'title', 'forum', 'site', 'subforum']:
                    for item in self.parse_values(fixed_filters_dict[fixed_key]):
                        if item.strip().startswith("-"):
                            value_ = item.strip()[1:]
                            where_condition += fixed_key + " not like \'%" + value_ + "%\' AND\n"

            # убираем последний AND
            if where_condition.endswith("AND\n"):
                where_condition = where_condition[:-4] + "\n"

            provided_limit = 0
            if data.get("limit") is not None:
                search_limit = min(user_max_limit, int(data["limit"]))
            else:
                search_limit = user_max_limit

            # условие на выбор максимального/минимального размера
            search_size_options = ""
            if data.get("qa") is not None:
                if data["qa"].strip().startswith("min"):
                    search_size_options = "AND qa = (SELECT min(qa) from plab_engine_urls table2 where table1.title = table2.title)"
                if data["qa"].strip().startswith("max"):
                    search_size_options = "AND qa = (SELECT max(qa) from plab_engine_urls table2 where table1.title = table2.title)"

            # условие на проверку истории поиска
            search_history = ""
            if data.get("history") is not None and not str(data.get("history")).lower().strip().startswith("false"):
                # кусок запроса на исключение просмотренных данных из поиска
                search_history = "AND url_id not in (select url_id from plab_bot_history_detailed \
                inner join plab_bot_history on plab_bot_history.global_search_id = plab_bot_history_detailed.global_search_id \
                where user_id = \'" + str(user_id) + "\')"

            search_order = "url_id"
            search_request_order =  " order by " + search_order + " desc limit " + str(search_limit)

            search_request = search_request_main + where_condition + search_size_options + search_history + search_request_order
            self.logger.debug("Create_search_request() Search request: " + search_request)
            return_dict["search_request"] = search_request

            # если стоит флаг записи истории поиска, то сохраняем в result
            if data.get("save") is not None and str(data.get("save")).lower().strip().startswith("true"):
                return_dict["save"] = True

            print(search_request)
            return_dict["isvalid"] = True
        except Exception as e:
            self.logger.warning("Create_search_request() Error in create search request:")
            return_dict["error_message"] = str(e)
            return return_dict

        return return_dict

    def create_out_files(self, urls, search_id_str, browser_path = "", num_url_per_file = 50):
        file_names = []
        try:
            files_count = int(math.ceil(len(urls) / num_url_per_file))
            last_file_id = 1
            current_file_id = 1
            url_index = 1
            file_names.append(self.TMP_PATH + "plab_search_" + search_id_str + "_part_" + str(current_file_id) + "_of_" + str(files_count) + ".bat")
            for url in urls:
                out_filename = "plab_search_" + search_id_str + "_part_" + str(current_file_id) + "_of_" + str(files_count) + ".bat"
                out_file = open(self.TMP_PATH + out_filename, mode='a')
                out_file.write(browser_path + " " + str(url[0]) + "\n")
                if current_file_id != last_file_id:
                    file_names.append(out_file.name)
                    last_file_id = current_file_id
                current_file_id = url_index // num_url_per_file + 1
                url_index += 1
                out_file.close()

        except Exception as e:
            self.logger.error("Create files() Error:" + str(e))
        return file_names


if __name__ == '__main__':
    pBot = PlabBot()
    pBot.run()
