import pandas as pd
import numpy as np
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st

class ESGVisualizer:
    def __init__(self, db_path='data/processed/esg_data.db'):
        self.db_path = db_path
        
    def load_data(self):
        """Load all necessary data for visualization"""
        conn = sqlite3.connect(self.db_path)
        
        self.portfolio = pd.read_sql("SELECT * FROM portfolio", conn)
        self.esg_scores = pd.read_sql("SELECT * FROM esg_scores", conn)
        self.companies = pd.read_sql("SELECT * FROM companies", conn)
        self.comparisons = pd.read_sql("SELECT * FROM company_benchmark_comparisons", conn)
        
        # Merge data for portfolio analysis
        self.portfolio = self.portfolio.merge(self.companies, on='ticker')
        self.portfolio = self.portfolio.merge(self.esg_scores, on='ticker')
        
        conn.close()
        
    def create_portfolio_summary(self):
        """Create portfolio ESG summary visualization"""
        # Calculate portfolio value and weights
        self.portfolio['current_price'] = np.random.uniform(0.8, 1.2, len(self.portfolio)) * self.portfolio['purchase_price']  # Simulated current price
        self.portfolio['value'] = self.portfolio['shares'] * self.portfolio['current_price']
        total_value = self.portfolio['value'].sum()
        self.portfolio['weight'] = self.portfolio['value'] / total_value
        
        # Calculate weighted ESG scores
        self.portfolio['weighted_env'] = self.portfolio['environmental_score'] * self.portfolio['weight']
        self.portfolio['weighted_social'] = self.portfolio['social_score'] * self.portfolio['weight']
        self.portfolio['weighted_gov'] = self.portfolio['governance_score'] * self.portfolio['weight']
        self.portfolio['weighted_overall'] = self.portfolio['overall_esg_score'] * self.portfolio['weight']
        
        # Create summary plot
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "domain"}, {"type": "bar"}]],
            subplot_titles=("Portfolio Composition", "Weighted ESG Scores")
        )
        
        # Portfolio composition pie chart
        fig.add_trace(
            go.Pie(
                labels=self.portfolio['ticker'],
                values=self.portfolio['value'],
                textinfo='label+percent',
                hole=0.4,
            ),
            row=1, col=1
        )
        
        # Weighted ESG scores
        weighted_scores = pd.DataFrame({
            'Category': ['Environmental', 'Social', 'Governance', 'Overall'],
            'Score': [
                self.portfolio['weighted_env'].sum(),
                self.portfolio['weighted_social'].sum(),
                self.portfolio['weighted_gov'].sum(),
                self.portfolio['weighted_overall'].sum()
            ]
        })
        
        fig.add_trace(
            go.Bar(
                x=weighted_scores['Category'],
                y=weighted_scores['Score'],
                marker_color=['#4CAF50', '#2196F3', '#FFC107', '#673AB7'],
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            title="Portfolio ESG Summary",
            height=500,
            width=900
        )
        
        return fig
        
    def create_company_comparison(self):
        """Create company ESG score comparison visualization"""
        # Prepare data
        company_data = []
        for _, row in self.comparisons.iterrows():
            company_data.extend([
                {'Ticker': row['ticker'], 'Category': 'Environmental', 'Score': row['company_env_score'], 'Type': 'Company'},
                {'Ticker': row['ticker'], 'Category': 'Environmental', 'Score': row['sector_env_benchmark'], 'Type': 'Sector Benchmark'},
                {'Ticker': row['ticker'], 'Category': 'Social', 'Score': row['company_social_score'], 'Type': 'Company'},
                {'Ticker': row['ticker'], 'Category': 'Social', 'Score': row['sector_social_benchmark'], 'Type': 'Sector Benchmark'},
                {'Ticker': row['ticker'], 'Category': 'Governance', 'Score': row['company_gov_score'], 'Type': 'Company'},
                {'Ticker': row['ticker'], 'Category': 'Governance', 'Score': row['sector_gov_benchmark'], 'Type': 'Sector Benchmark'},
                {'Ticker': row['ticker'], 'Category': 'Overall', 'Score': row['company_overall_score'], 'Type': 'Company'},
                {'Ticker': row['ticker'], 'Category': 'Overall', 'Score': row['sector_overall_benchmark'], 'Type': 'Sector Benchmark'}
            ])
        
        comparison_df = pd.DataFrame(company_data)
        
        # Create grouped bar chart
        fig = px.bar(
            comparison_df, 
            x='Category', 
            y='Score', 
            color='Type',
            barmode='group',
            facet_col='Ticker',
            color_discrete_map={
                'Company': '#2196F3',
                'Sector Benchmark': '#9E9E9E'
            },
            labels={'Score': 'ESG Score', 'Category': 'ESG Category'},
            title="Company ESG Scores vs. Sector Benchmarks"
        )
        
        fig.update_layout(
            height=500,
            width=1000
        )
        
        return fig
        
    def create_esg_heatmap(self):
        """Create ESG score heatmap visualization"""
        # Prepare data
        heatmap_data = self.esg_scores[['ticker', 'environmental_score', 'social_score', 'governance_score', 'overall_esg_score']]
        heatmap_data = heatmap_data.set_index('ticker')
        
        # Rename columns for better display
        heatmap_data.columns = ['Environmental', 'Social', 'Governance', 'Overall']
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(
            heatmap_data, 
            annot=True, 
            cmap='Blues', 
            linewidths=.5,
            ax=ax, 
            fmt='.1f',
            vmin=0,
            vmax=100
        )
        
        plt.title('ESG Score Heatmap by Company')
        plt.tight_layout()
        
        return fig
        
    def create_radar_charts(self):
        """Create radar charts for ESG profile visualization"""
        # Create radar charts for each company
        radar_figs = []
        
        for _, company in self.comparisons.iterrows():
            categories = ['Environmental', 'Social', 'Governance']
            company_scores = [
                company['company_env_score'],
                company['company_social_score'],
                company['company_gov_score']
            ]
            
            benchmark_scores = [
                company['sector_env_benchmark'],
                company['sector_social_benchmark'],
                company['sector_gov_benchmark']
            ]
            
            # Create radar chart
            fig = go.Figure()
            
            fig.add_trace(go.Scatterpolar(
                r=company_scores,
                theta=categories,
                fill='toself',
                name=f"{company['ticker']} Scores",
                line_color='#2196F3'
            ))
            
            fig.add_trace(go.Scatterpolar(
                r=benchmark_scores,
                theta=categories,
                fill='toself',
                name=f"{company['sector']} Sector Benchmark",
                line_color='#9E9E9E'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )),
                showlegend=True,
                title=f"ESG Profile: {company['ticker']} vs {company['sector']} Sector"
            )
            
            radar_figs.append(fig)
        
        return radar_figs

# Example usage
if __name__ == "__main__":
    visualizer = ESGVisualizer()
    visualizer.load_data()
    
    # Create visualizations
    portfolio_summary = visualizer.create_portfolio_summary()
    company_comparison = visualizer.create_company_comparison()
    esg_heatmap = visualizer.create_esg_heatmap()
    radar_charts = visualizer.create_radar_charts()
    
    # Display the figures (when not in Streamlit)
    portfolio_summary.show()
    company_comparison.show()
    plt.figure(esg_heatmap.number)
    plt.show()
    for fig in radar_charts:
        fig.show()