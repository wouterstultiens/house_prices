import requests
from bs4 import BeautifulSoup
import pandas as pd
import unidecode

def clean_city_names(series):
    return series.apply(lambda x: unidecode.unidecode(x).lower().replace(' ', '-').replace("'", ''))

def scrape_city_data(city):
    base_url = f"https://www.funda.nl/zoeken/koop?selected_area=%5B%22{city}%22%5D&availability=%5B%22unavailable%22%5D"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # Initial request to get pagination details
    response = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extracting the maximum page number
    pagination = soup.find("ul", {"class": "pagination"})
    max_page = int(pagination.find_all("li")[-2].text) if pagination else 1
    
    results = []
    for page in range(1, max_page + 1):
        page_url = f"{base_url}&search_result={page}"  # Adjust URL construction as necessary
        print(page_url)
        response = requests.get(page_url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        properties = soup.find_all("div", {"data-test-id": "search-result-item"})
        for property in properties:
            try:
                address = property.find("h2").text.strip()
                price = property.find("p", {"data-test-id": "price-sale"}).text.strip()
                url = property.find("a", {"data-test-id": "object-image-link"})['href']
                results.append((address, price, url))
            except (AttributeError, TypeError):
                continue

    return results

def scrape_house_details(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    def get_detail(label):
        element = soup.find("dt", string=label)
        return element.find_next_sibling("dd").text.strip() if element and element.find_next_sibling("dd") else None

    # For living and total area, since the search and filter pattern repeats, define a generalized method
    def get_area(label):
        elements = soup.find_all("span", {"data-test-kenmerken-highlighted-value": True, "class": "kenmerken-highlighted__value"})
        return next((span.text.strip() for span in elements if "m²" in span.text and label in span.parent.text), None)
    
    return {
        'asking_price_per_m2': get_detail("Vraagprijs per m²"),
        'status': get_detail("Status"),
        'type': get_detail("Soort woonhuis"),
        'year_built': get_detail("Bouwjaar"),
        'living_area': get_area("wonen"),
        'total_area': get_area("perceel"),
    }

def fetch_and_scrape_house_details(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return scrape_house_details(response.text)
    else:
        return {}

if __name__ == '__main__':
    # Assuming zip_codes.csv contains cities to scrape
    df = pd.read_csv("zip_codes/zip_codes.csv")
    cities = df["city"].drop_duplicates().sort_values()
    cleaned_cities = clean_city_names(cities)

    all_results = []
    for city in cleaned_cities[0:2]:  # Limit to the first city for demonstration
        city_results = scrape_city_data(city)
        for result in city_results:
            detailed_info = fetch_and_scrape_house_details(result[2])  # Fetch and scrape using the URL
            all_results.append({
                "City": city.replace('-', ' ').title(),
                "Address": result[0],
                "Asking Price": result[1],
                **detailed_info,  # Merge dictionaries
                "URL": result[2]
            })
    
    final_df = pd.DataFrame(all_results)
    print(final_df)
    final_df.to_csv("final_scraped_houses.csv", index=False)
