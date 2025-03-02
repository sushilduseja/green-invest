import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import os
import sqlite3
import sys
import time
import json
from pathlib import Path

# Add source directory to path
sys.path.append(str(Path(__file__).parent))

# Import project modules
from src.data.company_collector import CompanyDataCollector
from src.data.news_collector import NewsDataCollector
from src.data.report_processor import ReportProcessor
from src.data.data_integrator import DataIntegrator
from src.models.esg_scorer import ESGScorer
from src.models.benchmark_generator import ESGBenchmarkGenerator
from src.visualization.esg_visualizer import ESGVisualizer

# Set page config
st.set_page_config(
    page_title="GreenInvest: ESG Portfolio Analysis",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create database and data folders if they don't exist
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)
DB_PATH = "data/processed/esg_data.db"

# App title and introduction
st.title("🌿 GreenInvest: ESG Portfolio Analysis")
st.markdown("""
Analyze your investment portfolio's Environmental, Social, and Governance (ESG) performance.
This tool helps you understand how your investments align with sustainable and ethical business practices.
""")

# Sidebar
st.sidebar.title("Controls")
app_mode = st.sidebar.selectbox(
    "Choose a mode",
    ["Introduction", "Add Companies to Database", "Create/Edit Portfolio", "ESG Analysis Dashboard"]
)

# Display introduction
if app_mode == "Introduction":
    st.markdown("""
    ## About GreenInvest
    
    GreenInvest is an AI-powered tool that helps investors analyze the ESG (Environmental, Social, and Governance) 
    performance of their investment portfolios. This application uses natural language processing and machine learning 
    to evaluate companies based on their sustainability practices, social impact, and corporate governance.
    
    ### How it works:
    
    1. **Add Companies**: First, add companies to our database by providing their ticker symbols.
    2. **Create Portfolio**: Build your investment portfolio by selecting companies and entering your positions.
    3. **View ESG Analysis**: Get comprehensive ESG analysis with visualizations and comparisons to sector benchmarks.
    
    ### Data Sources:
    
    - Company financial data from Yahoo Finance
    - News data from GDELT Project
    - Corporate reports and sustainability disclosures
    
    ### ESG Scoring Methodology:
    
    Our ESG scoring system evaluates companies across three main dimensions:
    
    - **Environmental**: Climate impact, resource usage, pollution, waste management
    - **Social**: Employee relations, diversity, community engagement, human rights
    - **Governance**: Board structure, executive compensation, ethics, shareholder rights
    
    Each company receives scores from 0-100 in each category, with higher scores indicating better performance.
    
    ### Getting Started:
    
    Use the sidebar to navigate to "Add Companies to Database" to begin building your ESG investment analysis.
    """)
    
    st.info("This is an educational tool and should not be used as the sole basis for investment decisions. Always conduct your own research and consider consulting with a financial advisor.")

# Add companies to database
elif app_mode == "Add Companies to Database":
    st.header("Add Companies to Database")
    
    with st.form("add_companies_form"):
        tickers_input = st.text_input("Enter ticker symbols (comma-separated, e.g., MSFT,AAPL,GOOG)")
        full_collection = st.checkbox("Collect complete data (news, reports)", value=False)
        submitted = st.form_submit_button("Add Companies")
        
        if submitted and tickers_input:
            tickers = [ticker.strip().upper() for ticker in tickers_input.split(",")]
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Process each ticker
            for i, ticker in enumerate(tickers):
                progress = (i) / len(tickers)
                progress_bar.progress(progress)
                status_text.text(f"Processing {ticker}...")
                
                # Collect basic company data
                collector = CompanyDataCollector()
                result = collector.save_company_data(ticker)
                
                if result and full_collection:
                    # Get company info for better news search
                    try:
                        company_info_path = f"data/raw/{ticker}_company_info.json"
                        with open(company_info_path, 'r') as f:
                            company_info = json.load(f)
                        company_name = company_info.get('shortName', ticker)
                    except:
                        company_name = ticker
                
                    # Collect news
                    news_collector = NewsDataCollector()
                    news_collector.save_news_data(company_name, ticker)
                    
                    # Process reports
                    report_processor = ReportProcessor()
                    report_processor.process_company_reports(ticker)
            
            # Integrate all data
            integrator = DataIntegrator()
            for ticker in tickers:
                integrator.integrate_company_data(ticker)
            
            # Update progress
            progress_bar.progress(1.0)
            status_text.text("All companies processed successfully!")
            
            # Show success message with clear button
            st.success(f"Added {len(tickers)} companies to the database. You can now create a portfolio.")
            
            # Show the companies in the database
            conn = sqlite3.connect(DB_PATH)
            companies_df = pd.read_sql("SELECT ticker, name, sector, industry FROM companies", conn)
            conn.close()
            
            st.subheader("Companies in Database")
            st.dataframe(companies_df)

# Create/Edit Portfolio
elif app_mode == "Create/Edit Portfolio":
    st.header("Create or Edit Your Portfolio")
    
    # Load available companies
    conn = sqlite3.connect(DB_PATH)
    try:
        companies_df = pd.read_sql("SELECT ticker, name, sector, industry FROM companies", conn)
        
        # Check if there are companies in the database
        if companies_df.empty:
            st.warning("No companies in the database. Please add companies first.")
            st.stop()
            
        # Load existing portfolio if available
        try:
            portfolio_df = pd.read_sql("SELECT * FROM portfolio", conn)
        except:
            portfolio_df = pd.DataFrame(columns=['ticker', 'shares', 'purchase_price'])
            
        conn.close()
        
        # Display available companies
        with st.expander("Available Companies"):
            st.dataframe(companies_df)
        
        # Portfolio editor
        st.subheader("Edit Portfolio")
        
        # Add new position form
        with st.form("add_position_form"):
            cols = st.columns(3)
            ticker = cols[0].selectbox("Select Ticker", companies_df['ticker'].tolist())
            shares = cols[1].number_input("Number of Shares", min_value=0.01, step=0.01)
            price = cols[2].number_input("Purchase Price ($)", min_value=0.01, step=0.01)
            
            submitted = st.form_submit_button("Add Position")
            
            if submitted:
                # Check if ticker already exists in portfolio
                if ticker in portfolio_df['ticker'].values:
                    # Update existing position
                    portfolio_df.loc[portfolio_df['ticker'] == ticker, 'shares'] = shares
                    portfolio_df.loc[portfolio_df['ticker'] == ticker, 'purchase_price'] = price
                else:
                    # Add new position
                    new_position = pd.DataFrame({'ticker': [ticker], 'shares': [shares], 'purchase_price': [price]})
                    portfolio_df = pd.concat([portfolio_df, new_position], ignore_index=True)
                
                # Save to database
                conn = sqlite3.connect(DB_PATH)
                portfolio_df.to_sql('portfolio', conn, if_exists='replace', index=False)
                conn.close()
                
                st.success(f"Added/updated {ticker} position in your portfolio.")
        
        # Display current portfolio
        if not portfolio_df.empty:
            st.subheader("Current Portfolio")
            
            # Enhance display with company names
            display_portfolio = portfolio_df.merge(companies_df[['ticker', 'name']], on='ticker')
            
            # Calculate total value
            display_portfolio['total_value'] = display_portfolio['shares'] * display_portfolio['purchase_price']
            
            # Format for display
            display_portfolio = display_portfolio[['ticker', 'name', 'shares', 'purchase_price', 'total_value']]
            display_portfolio = display_portfolio.rename(columns={
                'ticker': 'Ticker',
                'name': 'Company',
                'shares': 'Shares',
                'purchase_price': 'Purchase Price ($)',
                'total_value': 'Total Value ($)'
            })
            
            st.dataframe(display_portfolio.style.format({
                'Shares': '{:.2f}',
                'Purchase Price ($)': '${:.2f}',
                'Total Value ($)': '${:.2f}'
            }))
            
            # Portfolio summary
            total_investment = display_portfolio['Total Value ($)'].sum()
            st.metric("Total Portfolio Value", f"${total_investment:.2f}")
            
            # Generate portfolio pie chart
            fig = px.pie(
                display_portfolio, 
                names='Ticker', 
                values='Total Value ($)',
                title="Portfolio Allocation",
                hover_data=['Company'],
                color_discrete_sequence=px.colors.qualitative.Plotly
            )
            st.plotly_chart(fig)
            
            # Option to clear portfolio
            if st.button("Reset Portfolio"):
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS portfolio")
                conn.commit()
                conn.close()
                st.success("Portfolio has been reset. Refresh the page to see changes.")
                
        else:
            st.info("Your portfolio is empty. Add positions using the form above.")
            
    except Exception as e:
        st.error(f"Error: {e}")
        conn.close()
        st.info("If you haven't added companies yet, please go to 'Add Companies to Database' first.")

# ESG Analysis Dashboard
elif app_mode == "ESG Analysis Dashboard":
    st.header("ESG Portfolio Analysis Dashboard")
    
    # Check if portfolio exists
    conn = sqlite3.connect(DB_PATH)
    try:
        portfolio_df = pd.read_sql("SELECT * FROM portfolio", conn)
        conn.close()
        
        if portfolio_df.empty:
            st.warning("Your portfolio is empty. Please create a portfolio first.")
            st.stop()
    except:
        conn.close()
        st.warning("Portfolio not found. Please create a portfolio first.")
        st.stop()
    
    # Run analysis button
    if st.button("Run ESG Analysis"):
        with st.spinner("Analyzing ESG factors for your portfolio..."):
            # Score companies
            scorer = ESGScorer(DB_PATH)
            tickers = portfolio_df['ticker'].tolist()
            scores = scorer.score_portfolio(tickers)
            
            # Generate benchmarks
            benchmark_generator = ESGBenchmarkGenerator(DB_PATH)
            benchmark_generator.load_sector_benchmarks()
            benchmark_generator.generate_company_comparisons()
            
            st.success("Analysis complete!")
        
    # Create visualizations
    try:
        visualizer = ESGVisualizer(DB_PATH)
        visualizer.load_data()
        
        # Portfolio summary
        st.subheader("Portfolio ESG Summary")
        portfolio_summary = visualizer.create_portfolio_summary()
        st.plotly_chart(portfolio_summary)
        
        # ESG Heatmap
        st.subheader("ESG Score Heatmap")
        esg_heatmap = visualizer.create_esg_heatmap()
        st.pyplot(esg_heatmap)
        
        # Company comparisons
        st.subheader("Company ESG Scores vs. Sector Benchmarks")
        company_comparison = visualizer.create_company_comparison()
        st.plotly_chart(company_comparison)
        
        # Radar charts
        st.subheader("ESG Profiles by Company")
        radar_charts = visualizer.create_radar_charts()
        
        # Display radar charts in columns
        cols = st.columns(min(2, len(radar_charts)))
        for i, radar_chart in enumerate(radar_charts):
            with cols[i % 2]:
                st.plotly_chart(radar_chart)
        
        # Add report generation button
        if st.button("Generate ESG Report"):
            st.info("Generating detailed ESG report...")
            
            # Get data for report
            conn = sqlite3.connect(DB_PATH)
            esg_scores = pd.read_sql("SELECT * FROM esg_scores", conn)
            companies = pd.read_sql("SELECT * FROM companies", conn)
            comparisons = pd.read_sql("SELECT * FROM company_benchmark_comparisons", conn)
            portfolio = pd.read_sql("SELECT * FROM portfolio", conn)
            conn.close()
            
            # Create merged data for report
            report_data = portfolio.merge(companies, on='ticker')
            report_data = report_data.merge(esg_scores, on='ticker')
            report_data = report_data.merge(comparisons[['ticker', 'env_difference', 'social_difference', 'gov_difference', 'overall_difference']], on='ticker')
            
            # Calculate portfolio weightings
            report_data['value'] = report_data['shares'] * report_data['purchase_price']
            total_value = report_data['value'].sum()
            report_data['weight'] = report_data['value'] / total_value
            
            # Calculate weighted scores
            report_data['weighted_overall'] = report_data['overall_esg_score'] * report_data['weight']
            portfolio_esg_score = report_data['weighted_overall'].sum()
            
            # Generate report
            st.header("ESG Portfolio Analysis Report")
            st.subheader(f"Overall Portfolio ESG Score: {portfolio_esg_score:.1f}/100")
            
            # ESG Performance Summary
            st.markdown("### ESG Performance Summary")
            summary_cols = st.columns(4)
            with summary_cols[0]:
                st.metric("Environmental", f"{(report_data['environmental_score'] * report_data['weight']).sum():.1f}")
            with summary_cols[1]:
                st.metric("Social", f"{(report_data['social_score'] * report_data['weight']).sum():.1f}")
            with summary_cols[2]:
                st.metric("Governance", f"{(report_data['governance_score'] * report_data['weight']).sum():.1f}")
            with summary_cols[3]:
                st.metric("Overall", f"{portfolio_esg_score:.1f}")
            
            # Top performers
            st.markdown("### Top ESG Performers in Your Portfolio")
            top_performers = report_data.sort_values('overall_esg_score', ascending=False).head(3)
            for i, (_, company) in enumerate(top_performers.iterrows()):
                st.markdown(f"**{i+1}. {company['name']} ({company['ticker']})**")
                st.markdown(f"Overall ESG Score: {company['overall_esg_score']:.1f}/100")
                st.markdown(f"Sector: {company['sector']}")
                st.markdown(f"Performance vs Sector: {'+' if company['overall_difference'] >= 0 else ''}{company['overall_difference']:.1f} points")
            
            # Areas for improvement
            st.markdown("### Areas for Improvement")
            # Find companies with biggest negative differences from benchmarks
            needs_improvement = report_data.sort_values('overall_difference').head(3)
            for i, (_, company) in enumerate(needs_improvement.iterrows()):
                st.markdown(f"**{i+1}. {company['name']} ({company['ticker']})**")
                st.markdown(f"Overall ESG Score: {company['overall_esg_score']:.1f}/100")
                st.markdown(f"Sector: {company['sector']}")
                st.markdown(f"Performance vs Sector: {company['overall_difference']:.1f} points")
            
            st.info("Report generated successfully. You can take screenshots for your records or reference.")
            
    except Exception as e:
        st.error(f"Error creating visualizations: {e}")
        st.info("Try running the ESG Analysis again, or check if your portfolio has been properly created.")

# Add footer
st.markdown("---")
st.markdown("GreenInvest ESG Portfolio Analysis Tool | Created with Streamlit and Hugging Face Transformers")

# Run the Streamlit app (already handled by Streamlit)
