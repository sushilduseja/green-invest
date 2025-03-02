import os
import pandas as pd
import json
from sqlalchemy import create_engine
import sqlite3

class DataIntegrator:
    def __init__(self, input_dir='data/processed', output_dir='data/processed'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Create SQLite database
        self.db_path = f"{output_dir}/esg_data.db"
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        
    def integrate_company_data(self, ticker_symbol):
        """Integrate all data for a company into the database"""
        try:
            # Get company info
            company_info_file = f"data/raw/{ticker_symbol}_company_info.json"
            if os.path.exists(company_info_file):
                with open(company_info_file, 'r') as f:
                    company_info = json.load(f)
                
                # Extract basic company info
                company_data = {
                    'ticker': ticker_symbol,
                    'name': company_info.get('shortName', ''),
                    'sector': company_info.get('sector', ''),
                    'industry': company_info.get('industry', ''),
                    'country': company_info.get('country', ''),
                    'website': company_info.get('website', ''),
                    'employees': company_info.get('fullTimeEmployees', 0),
                    'market_cap': company_info.get('marketCap', 0)
                }
                
                # Create companies table
                companies_df = pd.DataFrame([company_data])
                companies_df.to_sql('companies', self.engine, if_exists='append', index=False)
                
            # Get stock data
            stock_file = f"data/raw/{ticker_symbol}_stock_data.csv"
            if os.path.exists(stock_file):
                stock_df = pd.read_csv(stock_file)
                stock_df['ticker'] = ticker_symbol
                stock_df.to_sql('stock_prices', self.engine, if_exists='append', index=False)
                
            # Get news data
            news_file = f"data/raw/{ticker_symbol}_news_data.csv"
            if os.path.exists(news_file):
                news_df = pd.read_csv(news_file)
                news_df['ticker'] = ticker_symbol
                news_df.to_sql('news', self.engine, if_exists='append', index=False)
                
            # Get news content sample
            news_content_file = f"data/raw/{ticker_symbol}_news_content_sample.csv"
            if os.path.exists(news_content_file):
                news_content_df = pd.read_csv(news_content_file)
                news_content_df['ticker'] = ticker_symbol
                news_content_df.to_sql('news_content', self.engine, if_exists='append', index=False)
                
            # Report text (if available)
            report_text_file = f"{self.output_dir}/{ticker_symbol}_all_reports.txt"
            if os.path.exists(report_text_file):
                with open(report_text_file, 'r', encoding='utf-8') as f:
                    report_text = f.read()
                
                # Store report text
                report_data = {
                    'ticker': ticker_symbol,
                    'report_text': report_text
                }
                
                reports_df = pd.DataFrame([report_data])
                reports_df.to_sql('reports', self.engine, if_exists='append', index=False)
                
            return True
        except Exception as e:
            print(f"Error integrating data for {ticker_symbol}: {e}")
            return False
            
    def create_portfolio_table(self, portfolio_data):
        """Create a portfolio table from user input"""
        try:
            portfolio_df = pd.DataFrame(portfolio_data)
            portfolio_df.to_sql('portfolio', self.engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            print(f"Error creating portfolio table: {e}")
            return False

# Example usage
if __name__ == "__main__":
    integrator = DataIntegrator()
    
    # Example portfolio
    portfolio = [
        {'ticker': 'MSFT', 'shares': 10, 'purchase_price': 250.0},
        {'ticker': 'AAPL', 'shares': 15, 'purchase_price': 150.0},
        {'ticker': 'GOOG', 'shares': 5, 'purchase_price': 2500.0},
        {'ticker': 'TSLA', 'shares': 8, 'purchase_price': 700.0}
    ]
    
    # Create portfolio table
    integrator.create_portfolio_table(portfolio)
    
    # Integrate data for each company
    for ticker in ["MSFT", "AAPL", "GOOG", "TSLA"]:
        print(f"Integrating data for {ticker}...")
        result = integrator.integrate_company_data(ticker)
        print(f"Data integration for {ticker} {'successful' if result else 'failed'}")