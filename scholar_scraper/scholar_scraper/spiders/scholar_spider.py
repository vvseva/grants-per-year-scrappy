import scrapy
import pandas as pd
import csv

class ScholarSpider(scrapy.Spider):
    name = 'scholar_spider'

    # Load the names from the spreadsheet
    df = pd.read_csv('NU_faculty_external_grant_count_MASTER_v2_2024_1017.csv', encoding='latin1')  # assuming the spreadsheet is 'names.csv'
    names = df['investigator_full_name'].unique().tolist()  # replace 'NAME_COLUMN' with the actual column name

    # Initialize an empty list to store scraped links
    scraped_links = []

    def start_requests(self):
        # Generate URLs for each name in the spreadsheet
        for name in self.names:
            name_cleaned = name.replace(' ', '+')
            url = f'https://www.scholars.northwestern.edu/en/persons/?search={name_cleaned}&pageSize=50&ordering=rating&descending=false&showAdvanced=false&allConcepts=true&inferConcepts=true&searchBy=PartOfNameOrTitle&community=false&direct2experts=false'
            yield scrapy.Request(url=url, callback=self.parse, meta={'name': name})

    def parse(self, response):
        name = response.meta['name']

        try:
            # Extract the link to the first element using the provided XPath
            first_result_link = response.xpath('//*[@id="main-content"]/div/div[2]/ul/li/div/div[1]/h3/a/@href').get()

            if first_result_link:
                full_link = response.urljoin(first_result_link)  # Convert relative URL to absolute
                self.log(f'Name: {name} - Link: {full_link}')
                self.scraped_links.append({'name': name, 'link': full_link})  # Store the name and link
            else:
                self.log(f'No result found for {name}')

        except Exception as e:
            self.log(f'Error processing {name}: {str(e)}')


    def closed(self, reason):
        """This method is called when the spider finishes scraping."""
        self.log("Scraping completed.")

        # Save to CSV file
        with open('scrapped_links.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=['name', 'link'])
            writer.writeheader()
            writer.writerows(self.scraped_links)

        self.log("Links saved to scraped_links.csv")