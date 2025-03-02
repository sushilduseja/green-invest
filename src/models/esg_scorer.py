import os
import pandas as pd
import sqlite3
import numpy as np
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
import nltk
from nltk.tokenize import sent_tokenize
import torch

# Download required NLTK data
nltk.download('punkt')

class ESGScorer:
    def __init__(self, db_path='data/processed/esg_data.db'):
        self.db_path = db_path
        self.setup_models()
        
    def setup_models(self):
        """Set up the NLP models for ESG analysis"""
        # For environmental classification
        print("Loading environmental classification model...")
        self.env_tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
        self.env_model = AutoModelForSequenceClassification.from_pretrained("distilbert-base-uncased", num_labels=2)
        
        # For social classification
        print("Loading social classification model...")
        self.social_tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
        self.social_model = AutoModelForSequenceClassification.from_pretrained("distilbert-base-uncased", num_labels=2)
        
        # For governance classification
        print("Loading governance classification model...")
        self.gov_tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
        self.gov_model = AutoModelForSequenceClassification.from_pretrained("distilbert-base-uncased", num_labels=2)
        
        # Sentiment analysis
        print("Loading sentiment analysis model...")
        self.sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
        
    def fine_tune_models(self):
        """
        Note: This is a placeholder for model fine-tuning
        In a real implementation, you would fine-tune these models on ESG data
        For this example, we'll simulate the effect of fine-tuning
        """
        print("Models would be fine-tuned with ESG data here.")
        print("For this example, we'll use pre-trained models with simulated ESG classification.")
        
    def classify_text_environmental(self, text):
        """Classify text for environmental factors (placeholder implementation)"""
        # In a real implementation, use the fine-tuned model
        # This is a simplified simulation of classification
        env_keywords = [
            "climate", "carbon", "emission", "renewable", "sustainable", "green",
            "environment", "pollution", "waste", "recycle", "energy efficiency"
        ]
        
        text_lower = text.lower()
        keyword_count = sum(1 for keyword in env_keywords if keyword in text_lower)
        score = min(100, keyword_count * 10)  # Simple scoring based on keyword frequency
        
        return score
        
    def classify_text_social(self, text):
        """Classify text for social factors (placeholder implementation)"""
        social_keywords = [
            "diversity", "inclusion", "community", "employee", "human rights", "fair wage",
            "health", "safety", "welfare", "education", "training", "equality"
        ]
        
        text_lower = text.lower()
        keyword_count = sum(1 for keyword in social_keywords if keyword in text_lower)
        score = min(100, keyword_count * 10)
        
        return score
        
    def classify_text_governance(self, text):
        """Classify text for governance factors (placeholder implementation)"""
        governance_keywords = [
            "governance", "board", "executive", "compliance", "ethics", "risk management",
            "transparency", "accountability", "shareholder", "audit", "compensation", "responsibility"
        ]
        
        text_lower = text.lower()
        keyword_count = sum(1 for keyword in governance_keywords if keyword in text_lower)
        score = min(100, keyword_count * 10)
        
        return score
        
    def analyze_sentiment(self, text):
        """Analyze sentiment of text"""
        try:
            # Break text into smaller chunks for analysis
            sentences = sent_tokenize(text)
            sentiment_scores = []
            
            # Process in batches to avoid memory issues
            batch_size = 10
            for i in range(0, len(sentences), batch_size):
                batch = sentences[i:i+batch_size]
                results = self.sentiment_analyzer(batch)
                scores = [1 if result['label'] == 'POSITIVE' else 0 for result in results]
                sentiment_scores.extend(scores)
            
            if sentiment_scores:
                return sum(sentiment_scores) / len(sentiment_scores) * 100
            return 50  # Neutral default
        except Exception as e:
            print(f"Error analyzing sentiment: {e}")
            return 50
        
    def score_company(self, ticker):
        """Calculate ESG scores for a company"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get report text
            report_df = pd.read_sql(f"SELECT report_text FROM reports WHERE ticker = '{ticker}'", conn)
            report_text = report_df['report_text'].iloc[0] if not report_df.empty else ""
            
            # Get news content
            news_df = pd.read_sql(f"SELECT content FROM news_content WHERE ticker = '{ticker}'", conn)
            news_text = " ".join(news_df['content'].tolist()) if not news_df.empty else ""
            
            # Combined text for analysis
            combined_text = report_text + " " + news_text
            
            # If no text available, return default scores
            if not combined_text.strip():
                return {
                    'ticker': ticker,
                    'environmental_score': 50,
                    'social_score': 50,
                    'governance_score': 50,
                    'sentiment_score': 50,
                    'overall_esg_score': 50
                }
            
            # Calculate scores
            env_score = self.classify_text_environmental(combined_text)
            social_score = self.classify_text_social(combined_text)
            gov_score = self.classify_text_governance(combined_text)
            sentiment = self.analyze_sentiment(combined_text)
            
            # Calculate overall ESG score (weighted average)
            overall_score = (env_score * 0.4 + social_score * 0.3 + gov_score * 0.3)
            
            scores = {
                'ticker': ticker,
                'environmental_score': env_score,
                'social_score': social_score,
                'governance_score': gov_score,
                'sentiment_score': sentiment,
                'overall_esg_score': overall_score
            }
            
            # Save scores to database
            scores_df = pd.DataFrame([scores])
            scores_df.to_sql('esg_scores', conn, if_exists='replace' if ticker in scores_df['ticker'].values else 'append', index=False)
            
            conn.close()
            return scores
        except Exception as e:
            print(f"Error scoring company {ticker}: {e}")
            return None
            
    def score_portfolio(self, tickers):
        """Score all companies in a portfolio"""
        all_scores = []
        for ticker in tickers:
            print(f"Scoring {ticker} for ESG factors...")
            scores = self.score_company(ticker)
            if scores:
                all_scores.append(scores)
                
        return pd.DataFrame(all_scores)

# Example usage
if __name__ == "__main__":
    scorer = ESGScorer()
    portfolio_tickers = ["MSFT", "AAPL", "GOOG", "TSLA"]
    results = scorer.score_portfolio(portfolio_tickers)
    print("\nESG Scoring Results:")
    print(results)