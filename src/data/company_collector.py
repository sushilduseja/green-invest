import yfinance as yf
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import time

class CompanyDataCollector:
    def __init__(self, output_dir='data/raw'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def get_stock_data(self, ticker_symbol, period="1y"):
        """Fetch stock data for a given ticker"""
        try:
            stock = yf.Ticker(ticker_symbol)
            data = stock.history(period=period)
            return data
        except Exception as e:
            print(f"Error fetching stock data for {ticker_symbol}: {e}")
            return pd.DataFrame()
            
    def get_company_profile(self, ticker_symbol):
        """Fetch company profile information"""
        try:
            stock = yf.Ticker(ticker_symbol)
            info = stock.info
            return info
        except Exception as e:
            print(f"Error fetching company info for {ticker_symbol}: {e}")
            return {}
            
    def fetch_annual_report_urls(self, ticker_symbol):
        """Find links to annual reports (10-K) and sustainability reports"""
        try:
            # This is a simplified approach - in a real implementation, you might use
            # SEC EDGAR API or more sophisticated web scraping
            stock = yf.Ticker(ticker_symbol)
            # Get company website from Yahoo Finance
            website = stock.info.get('website', '')
            if not website:
                return []
                
            # Simple approach to find investor relations or sustainability pages
            potential_urls = [
                f"{website}/investor-relations",
                f"{website}/sustainability",
                f"{website}/esg",
                f"{website}/corporate-responsibility"
            ]
            
            report_urls = []
            for url in potential_urls:
                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        # Look for PDF links that might be reports
                        for link in soup.find_all('a', href=True):
                            href = link['href']
                            text = link.text.lower()
                            if href.endswith('.pdf') and any(keyword in text for keyword in 
                                                           ['annual', 'report', 'sustainability', 'esg', '10-k']):
                                if not href.startswith('http'):
                                    href = website + href if href.startswith('/') else website + '/' + href
                                report_urls.append(href)
                    time.sleep(1)  # Be respectful with scraping
                except Exception as e:
                    print(f"Error accessing {url}: {e}")
            
            return report_urls
        except Exception as e:
            print(f"Error finding reports for {ticker_symbol}: {e}")
            return []
        
    def save_company_data(self, ticker_symbol):
        """Save all company data to files"""
        # Stock price history
        stock_data = self.get_stock_data(ticker_symbol)
        if not stock_data.empty:
            stock_data.to_csv(f"{self.output_dir}/{ticker_symbol}_stock_data.csv")
        
        # Company profile
        company_info = self.get_company_profile(ticker_symbol)
        if company_info:
            pd.Series(company_info).to_json(f"{self.output_dir}/{ticker_symbol}_company_info.json")
        
        # Report URLs
        report_urls = self.fetch_annual_report_urls(ticker_symbol)
        if report_urls:
            with open(f"{self.output_dir}/{ticker_symbol}_report_urls.txt", 'w') as f:
                for url in report_urls:
                    f.write(f"{url}\n")
        
        return bool(not stock_data.empty or company_info or report_urls)

# Example usage
if __name__ == "__main__":
    collector = CompanyDataCollector()
    # Test with a few tickers
    for ticker in ["MSFT", "AAPL", "GOOG", "TSLA"]:
        print(f"Collecting data for {ticker}...")
        result = collector.save_company_data(ticker)
        print(f"Data collection for {ticker} {'successful' if result else 'failed'}")