import os
import time
from datetime import datetime

import evpn
import pandas as pd
import scrapy
from scrapy.cmdline import execute


def extract_heading(response):
    """
    Extracts the heading from the response.

    This function looks for the text content of the first <p> element
    within a <div> that has the class "span6 about-description".

    Parameters:
        response (scrapy.http.Response): The response object containing the HTML to parse.

    Returns:
        str: The extracted heading text if found, otherwise 'N/A'.
    """
    # Use XPath to extract the text content of the first matching <p> element
    heading = response.xpath('//div[@class="span6 about-description"]/p/text()').extract_first()

    # If heading is found, return it; otherwise, return 'N/A'
    if heading:
        return heading
    else:
        return 'N/A'


def extract_date(row_date):
    """
    Extracts and formats a date from the given row.

    Parameters:
        row_date (scrapy.selector.Selector): The row containing the date.

    Returns:
        str: The date formatted as 'YYYY-MM-DD' if parsing is successful,
             otherwise the original date string or 'N/A' if no date is found.
    """
    # Extract the text content of the row
    date = row_date.xpath('./text()').extract_first()
    if date:
        try:
            # Attempt to parse the date assuming the format '%B %d, %Y' (e.g., 'January 1, 2024')
            parsed_date = datetime.strptime(date.strip(), '%B %d, %Y')
            return parsed_date.strftime('%Y-%m-%d')  # Format the parsed date as 'YYYY-MM-DD'
        except ValueError:
            # Return the original date string if parsing fails
            return date.strip()
    else:
        # Return 'N/A' if no date is found
        return 'N/A'


def extract_text(row):
    """
    Extracts text content from a given row.

    Parameters:
        row (scrapy.selector.Selector): The row containing the text.

    Returns:
        str: The extracted text if available, otherwise 'N/A'.
    """
    # Extract the text content from the row using XPath
    text = row.xpath('./text()').extract_first()

    if text:
        # If text is found, return it
        return text
    else:
        # Return 'N/A' if no text is found
        return 'N/A'


def extract_pdf_link(row):
    """
    Extracts the PDF link from a given row.

    Parameters:
        row (scrapy.selector.Selector): The row containing the link.

    Returns:
        str: The full URL of the PDF link if available, otherwise 'N/A'.
    """
    # Extract the href attribute from the row using XPath
    pdf_link = row.xpath('./@href').extract_first()

    if pdf_link:
        # If a link is found, prepend the base URL and return the full link
        return 'https://www.nyc.gov' + pdf_link
    else:
        # Return 'N/A' if no link is found
        return 'N/A'


class NycPublicReportsSpider(scrapy.Spider):
    """
    Scrapy Spider to scrape public reports from the NYC website.
    """

    name = "nyc_public_reports"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize an empty list to store scraped data
        self.data_list = []
        # Start timer to measure the execution time of the spider
        self.start = time.time()
        super().__init__()

        # Connect to a VPN (USA) using ExpressVpnApi
        print('Connecting to VPN (USA)')
        self.api = evpn.ExpressVpnApi()
        self.api.connect(country_id='207')  # '207' is the country code for the USA
        time.sleep(5)  # Allow time for the VPN connection to establish
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.cookies = {
            '_ga': 'GA1.1.1134427543.1732860263',
            'ak_bmsc': '53888DF3961FAC8FD4D91C542A55B53D~000000000000000000000000000000~YAAQT13SF3tK+amTAQAAHn3Uqhodit75F9hIivYy6ryGnxJlOPy5rhRwr3LnRVIyS5tYKVf0LOrwjssPZHLxic8Q3TCxWBRE6ipFZpQhuuU05rFQlo1YdK6cXhDzpslpfYcjn8jpKvgS0bnWDE38GrHkChDg3qMwUzC6lRB8HQM3EKTtdkcxFEvvAQFZ8+Y4XpyZUp0YQq01I3H7PlB1ql0PfTkgoSFcMzgpUSvoAIO6Q2tIhrfN1ZtIRhGNbrHZUlG4koxMFFvQKshSnSN1z2z3sFrPywruqeQ/N1t6ZFIHQ/caEieGg6bBOd5cwdV2dZAtF58Q4wlRIC4BtDRMHxWATp0Hh6yFyqYAf8VUUBb8tMWkoJXNguD+5xa2',
            'JSESSIONID': '9F62B6D3A44BF3D4D7CEC797C1F60949',
            'AWSALB': 'fWJzyFhAD0o8h3Ayhw/riDkNO/XmYW225X0mP4d4cTq33QK2QxVyFNMiO0QIiwMIL6MoCc8LhmNllBrSQldmvgSBlaUg78vLhXgFlaE3CFwnzu3/lAe69csyCV6S',
            'AWSALBCORS': 'fWJzyFhAD0o8h3Ayhw/riDkNO/XmYW225X0mP4d4cTq33QK2QxVyFNMiO0QIiwMIL6MoCc8LhmNllBrSQldmvgSBlaUg78vLhXgFlaE3CFwnzu3/lAe69csyCV6S',
            'bm_sv': 'A575C0BD98428C2E8B3CA6FBC2C00559~YAAQT13SF1qK+qmTAQAAmDMKqxqPgnn9QXqgX4t3mHw0YthY7VufCurPWdmY5wYTmdVK7bwwjS5SkZlzkn+64EqqCvqxSAlkMoJwW7YLi358KYZcFfZO6eG+PgX3tv3b7YsWBjten7pRhr+H2Bh2ZOBSBgtg2iehFrhS3dFwse5FKHeP6ai5cqNmwb6kiQAuUFZr/vTYXoszq18ku8gvDhStWbnRnQA09QZF2ormbj3WYQb1eEmwKjPQ0bFV~1',
            'RT': '"z=1&dm=nyc.gov&si=db1m07lw3gi&ss=m4guo17h&sl=0&tt=0"',
            '_ga_YK7QQQ1MWK': 'GS1.1.1733740188.6.1.1733741332.0.0.0',
        }

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            # 'Cookie': '_ga=GA1.1.1134427543.1732860263; ak_bmsc=53888DF3961FAC8FD4D91C542A55B53D~000000000000000000000000000000~YAAQT13SF3tK+amTAQAAHn3Uqhodit75F9hIivYy6ryGnxJlOPy5rhRwr3LnRVIyS5tYKVf0LOrwjssPZHLxic8Q3TCxWBRE6ipFZpQhuuU05rFQlo1YdK6cXhDzpslpfYcjn8jpKvgS0bnWDE38GrHkChDg3qMwUzC6lRB8HQM3EKTtdkcxFEvvAQFZ8+Y4XpyZUp0YQq01I3H7PlB1ql0PfTkgoSFcMzgpUSvoAIO6Q2tIhrfN1ZtIRhGNbrHZUlG4koxMFFvQKshSnSN1z2z3sFrPywruqeQ/N1t6ZFIHQ/caEieGg6bBOd5cwdV2dZAtF58Q4wlRIC4BtDRMHxWATp0Hh6yFyqYAf8VUUBb8tMWkoJXNguD+5xa2; JSESSIONID=9F62B6D3A44BF3D4D7CEC797C1F60949; AWSALB=fWJzyFhAD0o8h3Ayhw/riDkNO/XmYW225X0mP4d4cTq33QK2QxVyFNMiO0QIiwMIL6MoCc8LhmNllBrSQldmvgSBlaUg78vLhXgFlaE3CFwnzu3/lAe69csyCV6S; AWSALBCORS=fWJzyFhAD0o8h3Ayhw/riDkNO/XmYW225X0mP4d4cTq33QK2QxVyFNMiO0QIiwMIL6MoCc8LhmNllBrSQldmvgSBlaUg78vLhXgFlaE3CFwnzu3/lAe69csyCV6S; bm_sv=A575C0BD98428C2E8B3CA6FBC2C00559~YAAQT13SF1qK+qmTAQAAmDMKqxqPgnn9QXqgX4t3mHw0YthY7VufCurPWdmY5wYTmdVK7bwwjS5SkZlzkn+64EqqCvqxSAlkMoJwW7YLi358KYZcFfZO6eG+PgX3tv3b7YsWBjten7pRhr+H2Bh2ZOBSBgtg2iehFrhS3dFwse5FKHeP6ai5cqNmwb6kiQAuUFZr/vTYXoszq18ku8gvDhStWbnRnQA09QZF2ormbj3WYQb1eEmwKjPQ0bFV~1; RT="z=1&dm=nyc.gov&si=db1m07lw3gi&ss=m4guo17h&sl=0&tt=0"; _ga_YK7QQQ1MWK=GS1.1.1733740188.6.1.1733741332.0.0.0',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }


    def start_requests(self):
        """
        Initiates the scraping by sending an HTTP request to the target URL.
        """
        yield scrapy.Request(
            url='https://www.nyc.gov/site/doi/newsroom/public-reports.page',
            cookies=self.cookies,
            headers=self.headers,
            callback=self.parse_data
        )

    def parse_data(self, response):
        """
        Parses the response to extract data from the webpage.

        Parameters:
            response (scrapy.http.Response): The HTTP response object.
        """
        # Extract the heading from the page
        description = extract_heading(response)
        # Select all rows and dates from the page
        all_rows = response.xpath('//div[@class="span6 about-description"]/ul/li/a')
        all_dates = response.xpath('//div[@class="span6 about-description"]/p/strong')

        # Iterate through rows and dates simultaneously
        for row, row_date in zip(all_rows, all_dates):
            date = extract_date(row_date)  # Extract and format the date
            text = extract_text(row)  # Extract the text content
            pdf_link = extract_pdf_link(row)  # Extract the PDF link

            # Append the extracted data to the list
            self.data_list.append({
                'date': date,
                'text': text,
                'description': description,
                'pdf_link': pdf_link
            })

    def closed(self, reason):
        """
        Called when the spider finishes scraping. Saves the data to an Excel file
        and performs cleanup operations.

        Parameters:
            reason (str): The reason the spider was closed.
        """
        if self.data_list:
            # Ensure the 'output' directory exists
            output_dir = '../output'
            os.makedirs(output_dir, exist_ok=True)

            # Define the output file path
            filename = os.path.join(output_dir, 'nyc_public_reports.xlsx')

            # Convert the data list to a pandas DataFrame
            df = pd.DataFrame(self.data_list)

            # Add a unique ID column
            df.insert(0, 'id', range(1, len(df) + 1))

            # Save the DataFrame to an Excel file
            df.to_excel(filename, index=False)

            self.logger.info(f"Data saved to {filename}")
        else:
            self.logger.info("No data was scraped to save.")

        # Disconnect the VPN if connected
        if self.api.is_connected:
            self.api.disconnect()

        # Calculate and print the total scraping time
        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute('scrapy crawl nyc_public_reports'.split())