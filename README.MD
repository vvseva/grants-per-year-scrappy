## Small script with two spiders

### Problem

1. Go to this website: https://www.scholars.northwestern.edu/en/persons/
2. Search a name from the attached list (e.g., Abel N. Kho) - if a name is in the spreadsheet by not on the website, just enter "NA" in the spreadsheet next to their name.
3. Click on "Grants" and there's an interactive chart that has grant counts by year 
4. Then you populate the number of grants per year listed (but only for the years in my spreadsheet), which may vary by person.

## How to use

### Prerequisites

- install python
- install scrappy
- File with names of scholars

### Running

cd grants_scraper
scrapy crawl scholar_spider
scrapy crawl grants_spider

### Explanations

scholar_spider creates a csv for scholars that exists in NU database
grants_spider scraps number of grants from a JS script that was used to plot graphs

output is saved to grants-per-year.csv

## Existing problems

I am super mage crazy and programmed 0 delay between requests. You may gat banned. Consider reaching out the DB owners. They might be willing to help in a way that would not violate TOS.