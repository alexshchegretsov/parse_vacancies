# -*- coding: utf-8 -*-
import requests
import argparse
import csv
import mysql.connector
import os
import datetime
from urls import TUT_BY_URL, JOOBLE_URL, BELMETA_URL
from http_request_randomizer.requests.useragent.userAgent import UserAgentManager as UserAgent
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod


class Parser(ABC):
    def __init__(self, first_request_url):
        self.first_request_url = first_request_url
        self.pages_urls = []
        self.headers = {
            "Accept": "text/css,*/*;q=0.1",
            "User-Agent": UserAgent().get_random_user_agent(),
        }
        self.new_vacancies = []
        self.page_amount = 1
        self.session = None
        self.response = None

    def set_session(self):
        self.session = requests.Session()

    def recieve_response(self):
        self.response = self.session.get(url=self.first_request_url, headers=self.headers)
        if self.response.status_code != 200:
            raise ConnectionError("status_code is ", self.response.status_code)


class ParserInterface(ABC):

    @abstractmethod
    def define_pages_amount(self):
        raise NotImplementedError

    @abstractmethod
    def get_all_urls(self):
        raise NotImplementedError

    @abstractmethod
    def parse_pages(self):
        raise NotImplementedError


class TUTbyParser(Parser, ParserInterface):

    def define_pages_amount(self):
        soup = BeautifulSoup(self.response.content, "lxml")
        pagination = soup.find("div", attrs={"data-qa": "pager-block"})
        if pagination:
            self.page_amount = int(pagination.find("a", class_="bloko-button HH-Pager-Control").text)

    def get_all_urls(self):
        for page_number in range(1, self.page_amount + 1):
            url = f"{self.first_request_url}&page={page_number-1}"
            self.pages_urls.append(url)

    def parse_pages(self):
        for url in self.pages_urls:

            response = self.session.get(url=url, headers=self.headers)
            soup = BeautifulSoup(response.content, "lxml")
            divs = soup.find_all("div", attrs={"data-qa": "vacancy-serp__vacancy"})

            for div in divs:
                title = div.find("a", attrs={"data-qa": "vacancy-serp__vacancy-title"}).text.strip()
                company = div.find("a", attrs={"data-qa": "vacancy-serp__vacancy-employer"}).text
                href = div.find("a", attrs={"data-qa": "vacancy-serp__vacancy-title"})["href"]
                content_1 = div.find("div",
                                     attrs={"data-qa": "vacancy-serp__vacancy_snippet_responsibility"}).text.strip()
                content_2 = div.find("div", attrs={"data-qa": "vacancy-serp__vacancy_snippet_requirement"}).text.strip()
                short_description = f"{content_1}\r\n {content_2}\r\n"
                date_add = div.find("span", attrs={"class": "vacancy-serp-item__publication-date"}).text
                self.new_vacancies.append({
                    "title": title,
                    "company": company,
                    "href": href,
                    "short_description": short_description,
                    "date_add": date_add
                })


class JoobleParser(Parser, ParserInterface):

    def define_pages_amount(self):
        if self.response.status_code == 200:
            soup = BeautifulSoup(self.response.content, "lxml")
            pagination = soup.find("div", attrs={"class": "paging"})
            if pagination:
                self.page_amount = int(pagination.find_all("a")[-1].text)

    def get_all_urls(self):
        for page_number in range(1, self.page_amount + 1):
            url = f"{self.first_request_url}&p={page_number}"
            self.pages_urls.append(url)

    def parse_pages(self):
        if self.response.status_code == 200:
            for url in self.pages_urls:
                response = self.session.get(url=url, headers=self.headers)
                soup = BeautifulSoup(response.content, "lxml")
                divs = soup.find_all("div", attrs={"class": "result saved paddings"})
                for div in divs:
                    title = div.find("h2", class_="position").text.strip()
                    company = div.find("span", class_="gray_text company-name").text
                    href = div.find("a", class_="link-position job-marker-js")["href"]
                    short_descr = div.find("span", class_="description").text
                    date_add = div.find("span", class_="date_location").text
                    self.new_vacancies.append({
                        "title": title,
                        "company": company,
                        "href": href,
                        "short_description": short_descr,
                        "date_add": date_add,
                    })


class BelmetaParser(Parser, ParserInterface):

    def define_pages_amount(self):
        soup = BeautifulSoup(self.response.content, "lxml")
        try:
            page_amount = \
                soup.find("div", class_="count-sort clearfix").find("div", class_="search-count").text.split()[-1]
            page_amount = int(page_amount)
            if page_amount > 10:
                self.page_amount = page_amount // 10 if not page_amount % 10 else page_amount // 10 + 1
        except AttributeError as e:
            print(e)
            self.page_amount = 0

    def get_all_urls(self):
        for page_number in range(1, self.page_amount + 1):
            url = f"{self.first_request_url}&page={page_number}"
            self.pages_urls.append(url)

    def parse_pages(self):
        for url in self.pages_urls:
            response = self.session.get(url=url, headers=self.headers)
            soup = BeautifulSoup(response.content, "lxml")
            articles = soup.find_all("article", attrs={"class": "job no-logo"})
            for article in articles:
                title = article.find("div", class_="col-xs-12 title-wrap").find("h2", class_="title").find("a").text
                href = article.find("div", class_="col-xs-12 title-wrap").find("h2", class_="title").find("a")["href"]
                company = article.find("div", class_="company").text
                short_description = article.find("div", class_="desc").text
                date_add = article.find("div", class_="row bottom-wrap").find("div", class_="from").find("span").text
                self.new_vacancies.append({
                    "title": title,
                    "company": company,
                    "href": f"https://belmeta.com{href}",
                    "short_description": short_description,
                    "date_add": date_add
                })


class MySQLSaver:
    def __init__(self):
        self.db_connect = None
        self.cursor = None
        self.fresh_vacancies = []

    def connect_to_db(self, host="localhost", user="parse_user", passwd="Dexter89!", database="parse_db"):
        self.db_connect = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )

    def create_cursor(self):
        self.cursor = self.db_connect.cursor()

    def extract_all_saved_entities(self, table):
        self.cursor.execute(f"SELECT * FROM {table}")
        return self.cursor.fetchall()

    def define_fresh_entities(self, new_vacancies, saved_db_vacancies):
        for new_job in new_vacancies:
            for saved_job in saved_db_vacancies:
                if new_job["short_description"] == saved_job[4]:
                    break
            else:
                self.fresh_vacancies.append(new_job)

    def save_to_db(self, table_name):
        sql_formula = f"INSERT INTO {table_name} (date_add, title, company, short_description) VALUES (%s, %s, %s, %s)"
        for vacancy in self.fresh_vacancies:
            job = (vacancy["date_add"], vacancy["title"], vacancy["company"], vacancy["short_description"])
            self.cursor.execute(sql_formula, job)
        self.db_connect.commit()


class CSVSaver:

    def save_to_csv(self, file_name_with_format, jobs):
        with open(f"{file_name_with_format}", "w") as f:
            csv_file = csv.writer(f)
            csv_file.writerow(("date add", "vacancy title", "company", "short description", "link"))
            for job in jobs:
                csv_file.writerow(
                    (job["date_add"], job["title"], job["company"], job["short_description"], job["href"]))


class Sender:

    def notify_send(self, fresh_vacancies):
        while fresh_vacancies:
            vacancy = fresh_vacancies.pop()
            message = f"""
            Haunted at: <i>{datetime.datetime.now()}</i>\r\n\r\n

            {vacancy["title"]}\r\n

            {vacancy["short_description"]}\r\n
            {vacancy["date_add"]}\r\n
            {vacancy["href"]}
                    """
            company = vacancy["company"]
            os.system(f"/usr/bin/notify-send -t 100 'Fresh job by -> {company}' '{message}'")


class Creator:
    choices = {
        "tut": (TUTbyParser, TUT_BY_URL),
        "jooble": (JoobleParser, JOOBLE_URL),
        "belmeta": (BelmetaParser, BELMETA_URL)
    }

    @classmethod
    def return_parser(cls, resource, query):
        if resource in cls.choices:
            parser, url = cls.choices[resource]
            query_url = url.format(query)
            return parser(query_url)
        raise ValueError("not valid resource")


def call_correct_parser(resource, query):
    # initialization
    p = Creator.return_parser(resource, query)
    p.set_session()
    p.recieve_response()
    # parser works
    p.define_pages_amount()
    p.get_all_urls()
    p.parse_pages()
    new_vacancies = p.new_vacancies
    print(f"[{datetime.datetime.now()}] [{resource} parser] works, vacancies found {len(new_vacancies)}")
    # mysql
    sql_saver = MySQLSaver()
    sql_saver.connect_to_db()
    sql_saver.create_cursor()
    saved_entities = sql_saver.extract_all_saved_entities("jobs")
    sql_saver.define_fresh_entities(new_vacancies, saved_entities)
    sql_saver.save_to_db("jobs")
    # send
    to_send = sql_saver.fresh_vacancies
    sender = Sender()
    sender.notify_send(to_send)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--resource", required=True, choices=["tut", "jooble", "belmeta"],
                        help="select between 'jooble' and 'tut'")
    parser.add_argument("-q", "--query", required=True, help="'junior+python+django', for example")
    args = parser.parse_args()
    call_correct_parser(args.resource, args.query)
