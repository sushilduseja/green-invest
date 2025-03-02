import os
import requests
import pdfplumber
import pandas as pd
import time

class ReportProcessor:
    def __init__(self, input_dir='data/raw', output_dir='data/processed'):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def download_report(self, url, output_path):
        """Download a PDF report from a URL"""
        try:
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            else:
                print(f"Failed to download {url}. Status code: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return False
            
    def extract_text_from_pdf(self, pdf_path):
        """Extract text content from a PDF file"""
        try:
            text = ""
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() or ""
            return text
        except Exception as e:
            print(f"Error extracting text from {pdf_path}: {e}")
            return ""
            
    def process_company_reports(self, ticker_symbol):
        """Process all reports for a given company"""
        report_urls_file = f"{self.input_dir}/{ticker_symbol}_report_urls.txt"
        
        if not os.path.exists(report_urls_file):
            print(f"No report URLs found for {ticker_symbol}")
            return False
            
        # Create directory for downloaded reports
        reports_dir = f"{self.output_dir}/{ticker_symbol}_reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Read URLs
        with open(report_urls_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
            
        # Download and process each report
        all_text = ""
        for i, url in enumerate(urls):
            pdf_filename = f"{reports_dir}/report_{i+1}.pdf"
            
            # Download the report
            print(f"Downloading report from {url}...")
            if self.download_report(url, pdf_filename):
                # Extract text
                print(f"Extracting text from {pdf_filename}...")
                text = self.extract_text_from_pdf(pdf_filename)
                
                if text:
                    # Save text to file
                    with open(f"{reports_dir}/report_{i+1}.txt", 'w', encoding='utf-8') as f:
                        f.write(text)
                    
                    all_text += text + "\n\n"
                
            time.sleep(2)  # Be respectful with downloading
        
        # Save combined text
        if all_text:
            with open(f"{self.output_dir}/{ticker_symbol}_all_reports.txt", 'w', encoding='utf-8') as f:
                f.write(all_text)
            return True
            
        return False

# Example usage
if __name__ == "__main__":
    processor = ReportProcessor()
    for ticker in ["MSFT", "AAPL", "GOOG", "TSLA"]:
        print(f"Processing reports for {ticker}...")
        result = processor.process_company_reports(ticker)
        print(f"Report processing for {ticker} {'successful' if result else 'failed or no reports found'}")