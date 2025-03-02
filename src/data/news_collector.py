import requests
import json
import pandas as pd
import os
import datetime
from bs4 import BeautifulSoup
import time

class NewsDataCollector:
    def __init__(self, output_dir='data/raw'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def fetch_gdelt_news(self, company_name, days_back=30):
        """Fetch news from GDELT Project"""
        try:
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.datetime.timedelta(days=days_back)
            
            # Format dates for GDELT
            start_str = start_date.strftime('%Y%m%d%H%M%S')
            end_str = end_date.strftime('%Y%m%d%H%M%S')
            
            # GDELT V2 API endpoint (JSON version)
            base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
            query = f"sourcelang:english {company_name}"
            mode = "artlist"
            format_param = "json"
            
            url = f"{base_url}?query={query}&mode={mode}&format={format_param}&startdatetime={start_str}&enddatetime={end_str}&maxrecords=250"
            
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()
                articles = data.get('articles', [])
                
                # Convert to DataFrame
                df = pd.DataFrame(articles)
                return df
            else:
                print(f"Failed to fetch GDELT news. Status code: {response.status_code}")
                return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching GDELT news for {company_name}: {e}")
            return pd.DataFrame()
     
    def fetch_news_content(self, url):
        """Fetch and extract content from a news article URL"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract article content - this is a simplified approach
                # Real implementation would need more sophisticated extraction
                paragraphs = soup.find_all('p')
                text = ' '.join([p.get_text() for p in paragraphs])
                
                return text
            return ""
        except Exception as e:
            print(f"Error fetching content from {url}: {e}")
            return ""
        
    def save_news_data(self, company_name, ticker_symbol, days_back=30):
        """Save news data for a company"""
        news_df = self.fetch_gdelt_news(company_name, days_back)
        
        if not news_df.empty:
            # Save the news data
            news_df.to_csv(f"{self.output_dir}/{ticker_symbol}_news_data.csv", index=False)
            
            # Fetch content for a subset of articles (to avoid too many requests)
            # In a real implementation, you'd want to be more selective and respectful of websites
            sample_size = min(10, len(news_df))
            news_df = news_df.sample(sample_size)
            
            # Add content column
            contents = []
            for url in news_df['url']:
                content = self.fetch_news_content(url)
                contents.append(content)
                time.sleep(2)  # Be respectful with scraping
                
            news_df['content'] = contents
            news_df.to_csv(f"{self.output_dir}/{ticker_symbol}_news_content_sample.csv", index=False)
            
            return True
        return False

# Example usage
if __name__ == "__main__":
    collector = NewsDataCollector()
    companies = [
        ("Microsoft Corporation", "MSFT"),
        ("Apple Inc.", "AAPL"),
        ("Alphabet Inc.", "GOOG"),
        ("Tesla, Inc.", "TSLA")
    ]
    
    for company_name, ticker in companies:
        print(f"Collecting news for {company_name} ({ticker})...")
        result = collector.save_news_data(company_name, ticker, days_back=30)
        print(f"News collection for {ticker} {'successful' if result else 'failed'}")