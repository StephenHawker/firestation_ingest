from pagescraper.scraper import Scraper
import requests
import urllib.parse

class FireStationScraper(Scraper):

    header = {
        'User-Agent':
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36",
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        'Content-type': "application/x-www-form-urlencoded",
    }

    def get_page_data(self):
        """get html from page

        Keyword arguments:
        site_url -- Site URL to post to
        """

        form_data = urllib.parse.urlencode({self.form_key: '%', 'Submit': 'Select'})

        # Post form data to get
        r = requests.post(self.site_url, data=form_data, headers=self.header, verify=True)

        output_html = str(r.text)

        return output_html
