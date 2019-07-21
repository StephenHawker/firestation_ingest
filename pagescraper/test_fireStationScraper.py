from unittest import TestCase
from firestation_scraper import FireStationScraper


class TestFireStationScraper(TestCase):

    def test_get_page_data(self):
        test_url = 'http://www.firestations.org.uk/Fire_Stations_Page.php'
        test_form_key = 'brigade'
        scraper = FireStationScraper(test_url, test_form_key)
        actual_html = scraper.get_page_data()
        print(actual_html)
        self.assertIsNotNone(actual_html, 'should return some text')
