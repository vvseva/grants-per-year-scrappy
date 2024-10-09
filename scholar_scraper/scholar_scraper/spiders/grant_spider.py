import scrapy
import pandas as pd
import csv
import re


class GrantsSpider(scrapy.Spider):
    name = "grants_spider"

    def start_requests(self):
        # Load the links and names from the CSV file
        links_df = pd.read_csv('scrapped_links.csv')
        links = links_df['link'].tolist()
        self.names = links_df['name'].tolist()  # Store the names for later use

        # Iterate through the links and pass the corresponding name as metadata
        for idx, link in enumerate(links):
            yield scrapy.Request(url=link+'/projects', callback=self.parse, meta={'name': self.names[idx]})

    def parse(self, response):
        # Get the name from the response metadata
        name = response.meta['name']

        try:
            # Extract the script element text content
            script_content = response.xpath('//*[@id="main-content"]/div/section/div[1]/div/script/text()').get()

            if not script_content:
                self.logger.warning(f"No script content found at {response.url}")
                return

            # Use regular expressions to extract the 'labels' and 'values' from the script content
            labels = re.findall(r'labels:\s*\[([^\]]+)\]', script_content)
            values = re.findall(r'values:\s*\[([^\]]+)\]', script_content)

            # Clean up and convert the extracted labels and values
            if labels and values:
                labels = [label.strip() for label in labels[0].split(',')]
                values = [value.strip() for value in values[0].split(',')]

                # Create a dictionary to store the labels, values, and corresponding name
                data = [{'name': name, 'year': label, 'count': value} for label, value in zip(labels, values)]
            else:
                self.logger.warning(f"Labels or values missing in script content at {response.url}")
                return

        except Exception as e:
            self.logger.error(f"Error occurred while parsing {response.url}: {str(e)}")
            return

        # Save the data to CSV after parsing all rows
        if data:
            self.save_to_csv(data)

    def save_to_csv(self, data):
        # Save the data to CSV
        with open('grants-per-year.csv', 'a', newline='') as f:  # Open in append mode
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            # Write header only if the file is empty
            if f.tell() == 0:
                writer.writeheader()
            writer.writerows(data)
