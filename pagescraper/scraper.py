import requests
from abc import ABC, abstractmethod


class Scraper(ABC):

    def __init__(self, site_url, form_key):
        self.site_url = site_url
        self.form_key = form_key

    @abstractmethod
    def get_page_data(self):
        raise NotImplementedError
