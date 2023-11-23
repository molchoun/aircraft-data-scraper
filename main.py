import pandas as pd
import asyncio
from bs4 import BeautifulSoup
from io import StringIO
import logging
import aiohttp


class Scraper:
    """
       Scraper class for extracting aircraft information from the FAA registry.

       Args:
       - input_file (str): Path to the CSV file containing the list of aircraft N-NUMBERs.
       - output_file (str, optional): Path to the output CSV file. If not provided, the input file will be overwritten.
       - timeout (float, optional): Timeout duration for HTTP requests, in seconds.

       Attributes:
       - URL (str): The URL for fetching aircraft information from the FAA registry.
       - columns (list): List of column names representing the information to be extracted.
       - logger (Logger): Logger instance for recording events during scraping.
       - timeout (float): Timeout duration for HTTP requests, in seconds.
    """
    URL = 'https://registry.faa.gov/aircraftinquiry/Search/NNumberResult'

    def __init__(self, input_file, output_file=None, timeout=0):
        self.input_file = input_file
        if not output_file:
            self.output_file = input_file
        else:
            self.output_file = output_file
        self.columns = [
            'STATUS', 'MANUFACTURER NAME', 'MODEL', 'TYPE AIRCRAFT',
            'TYPE ENGINE', 'PENDING NUMBER CHANGE', 'DATE CHANGE AUTHORIZED',
            'TYPE REGISTRATION', 'COUNTY', 'ENGINE MANUFACTURER', 'ENGINE MODEL'
        ]
        self.timeout = timeout
        self.logger = self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    async def fetch_html(self, session, form_data):
        """
            Asynchronously fetch HTML content for a given N-NUMBER using aiohttp.

            Args:
                session (aiohttp.ClientSession): The aiohttp session for making asynchronous HTTP requests.
                form_data (str): The N-NUMBER(unique ID) for which HTML content is to be fetched.

            Returns:
                str or None: The HTML content if successful, None if an error occurs.
        """
        form_data = {'NNumbertxt': form_data}
        headers = {
            'Origin': 'https://registry.faa.gov',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        }
        try:
            async with session.post(self.URL, data=form_data, headers=headers) as response:
                return await response.text()
        except (aiohttp.ClientError, aiohttp.http.HttpProcessingError) as e:
            self.logger.error(f"aiohttp error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error during HTTP request: {e}")
            return None

    async def process_record(self, session, id_value):
        """
            Asynchronously process HTML content to extract aircraft information.
            Parses html tables with BeautifulSoup and processes to pandas DataFrames
            using pandas `read_html()`
            Args:
                session (aiohttp.ClientSession):
                id_value (str): The N-NUMBER unique ID

            Returns:
                dict: A dictionary containing aircraft information.

        """
        def process_dataframe(data):
            try:
                df1 = data.T[:2]
                df1.columns = df1.loc[0]
                df1.drop(0, inplace=True)
                df1.reset_index(drop=True, inplace=True)

                df2 = data.T[2:]
                df2.columns = df2.loc[2]
                df2.drop(2, inplace=True)
                df2.reset_index(drop=True, inplace=True)
                return [df1, df2]
            except Exception as e:
                self.logger.error(f"Error processing DataFrame: {e}")
                return None

        html = await self.fetch_html(session, id_value)
        if html is None:
            self.logger.warning(f"Skipping record with N-NUMBER {id_value} due to HTTP request error.")
            return {}

        soup = BeautifulSoup(html, 'html.parser')
        html_tables = soup.select("table.devkit-table")
        # passing str will be deprecated in future
        html_buffer = StringIO(str(html_tables))
        data = pd.read_html(html_buffer)
        data_dict = dict()
        df_list = []

        count = 1 if len(data) <= 2 else 3

        for i in range(count):
            df = process_dataframe(data[i])
            if df is not None:
                df_list += df

        for df in df_list:
            data_dict.update(df.loc[0])

        return {column: data_dict.get(column.title(), "N/A") for column in self.columns}

    def get_first_empty_index(self, df):
        """
            Get the index of the first empty value in the specified DataFrame
            depending on STATUS column.

            Args:
                df (pandas.DataFrame): The DataFrame to search for the first empty value.

            Returns:
                int: The index of the first empty value, or the length of the DataFrame if none is found.
        """
        if ~df.columns.isin(["STATUS"]).any():
            index = 0
        else:
            try:
                index = df.index[(df["STATUS"].isnull() | (df["STATUS"].astype(str).apply(len) == 0))].tolist()[0]
            except IndexError:
                index = len(df)  # Return the length of the DataFrame if no empty index is found
        return index

    async def write_record(self, session, index, df):
        id_value = df.at[index, 'N-NUMBER']
        data_dict = await self.process_record(session, id_value)
        for column, value in data_dict.items():
            df.at[index, column] = value

    async def gather_with_concurrency(self):
        df = pd.read_csv(self.input_file, low_memory=False)
        total_records = len(df)
        start_index = self.get_first_empty_index(df)
        batch_size = 20

        self.logger.info(f"Total records: {total_records}, Starting index: {start_index}")
        try:
            async with aiohttp.ClientSession() as session:
                for index in range(start_index, total_records, batch_size):
                    end_index = min(index + batch_size, total_records)
                    tasks = [self.write_record(session, index, df) for index in range(index, end_index)]
                    await asyncio.gather(*tasks)
                    self.logger.info(f"Processed records {index} to {end_index}.")

                    df.to_csv(self.output_file, index=False)

                df.to_csv(self.output_file, index=False)
                self.logger.info("Processing complete.")
        except Exception as e:
            self.logger.info(f"Scraper was interrupted: {e} \nWrote extracted records into file.")

    def run_scraper(self):
        self.logger.info("Starting the scraper...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.gather_with_concurrency())
        self.logger.info("Scraper completed.")


if __name__ == "__main__":
    scraper = Scraper('data/ARMASTER11-21-23.csv', timeout=1)
    scraper.run_scraper()
