import pandas as pd
import sqlite3
import numpy as np

class ESGBenchmarkGenerator:
    def __init__(self, db_path='data/processed/esg_data.db'):
        self.db_path = db_path
        
    def load_sector_benchmarks(self):
        """
        Load or generate ESG benchmarks by sector
        In a real implementation, this would use actual industry data
        For this example, we'll create simulated benchmarks
        """
        # Simulated sector benchmarks
        sectors = [
            "Technology", "Healthcare", "Financial Services", "Consumer Cyclical",
            "Industrials", "Communication Services", "Energy", "Basic Materials",
            "Consumer Defensive", "Utilities", "Real Estate"
        ]
        
        benchmarks = []
        for sector in sectors:
            # Simulated scores - different sectors have different typical ESG profiles
            if sector in ["Technology", "Healthcare"]:
                env_score = np.random.normal(75, 10)
                social_score = np.random.normal(80, 8)
                gov_score = np.random.normal(75, 12)
            elif sector in ["Energy", "Basic Materials"]:
                env_score = np.random.normal(50, 15)
                social_score = np.random.normal(60, 10)
                gov_score = np.random.normal(65, 10)
            else:
                env_score = np.random.normal(65, 12)
                social_score = np.random.normal(70, 10)
                gov_score = np.random.normal(70, 8)
            
            # Ensure scores are within 0-100 range
            env_score = max(0, min(100, env_score))
            social_score = max(0, min(100, social_score))
            gov_score = max(0, min(100, gov_score))
            
            overall_score = (env_score * 0.4 + social_score * 0.3 + gov_score * 0.3)
            
            benchmarks.append({
                'sector': sector,
                'environmental_benchmark': env_score,
                'social_benchmark': social_score,
                'governance_benchmark': gov_score,
                'overall_benchmark': overall_score
            })
        
        # Create DataFrame and save to database
        benchmarks_df = pd.DataFrame(benchmarks)
        conn = sqlite3.connect(self.db_path)
        benchmarks_df.to_sql('sector_benchmarks', conn, if_exists='replace', index=False)
        conn.close()
        
        return benchmarks_df
        
    def generate_company_comparisons(self):
        """Generate comparison data between companies and their sector benchmarks"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Load ESG scores
            esg_scores = pd.read_sql("SELECT * FROM esg_scores", conn)
            
            # Load companies data
            companies = pd.read_sql("SELECT ticker, sector FROM companies", conn)
            
            # Load sector benchmarks
            try:
                benchmarks = pd.read_sql("SELECT * FROM sector_benchmarks", conn)
            except:
                conn.close()
                benchmarks = self.load_sector_benchmarks()
                conn = sqlite3.connect(self.db_path)
            
            # Merge data
            merged = esg_scores.merge(companies, on='ticker')
            
            # Create comparisons
            comparisons = []
            for _, company in merged.iterrows():
                sector = company['sector']
                sector_benchmark = benchmarks[benchmarks['sector'] == sector]
                
                if sector_benchmark
                # Create comparisons
            comparisons = []
            for _, company in merged.iterrows():
                sector = company['sector']
                sector_benchmark = benchmarks[benchmarks['sector'] == sector]
                
                if sector_benchmark.empty:
                    # If sector not found, use average of all sectors
                    sector_benchmark = benchmarks.mean()
                    benchmark_env = sector_benchmark['environmental_benchmark']
                    benchmark_social = sector_benchmark['social_benchmark']
                    benchmark_gov = sector_benchmark['governance_benchmark']
                    benchmark_overall = sector_benchmark['overall_benchmark']
                else:
                    benchmark_env = sector_benchmark['environmental_benchmark'].iloc[0]
                    benchmark_social = sector_benchmark['social_benchmark'].iloc[0]
                    benchmark_gov = sector_benchmark['governance_benchmark'].iloc[0]
                    benchmark_overall = sector_benchmark['overall_benchmark'].iloc[0]
                
                comparison = {
                    'ticker': company['ticker'],
                    'sector': sector,
                    'company_env_score': company['environmental_score'],
                    'sector_env_benchmark': benchmark_env,
                    'env_difference': company['environmental_score'] - benchmark_env,
                    'company_social_score': company['social_score'],
                    'sector_social_benchmark': benchmark_social,
                    'social_difference': company['social_score'] - benchmark_social,
                    'company_gov_score': company['governance_score'],
                    'sector_gov_benchmark': benchmark_gov,
                    'gov_difference': company['governance_score'] - benchmark_gov,
                    'company_overall_score': company['overall_esg_score'],
                    'sector_overall_benchmark': benchmark_overall,
                    'overall_difference': company['overall_esg_score'] - benchmark_overall
                }
                
                comparisons.append(comparison)
            
            # Create DataFrame and save to database
            comparisons_df = pd.DataFrame(comparisons)
            comparisons_df.to_sql('company_benchmark_comparisons', conn, if_exists='replace', index=False)
            
            conn.close()
            return comparisons_df
        except Exception as e:
            print(f"Error generating company comparisons: {e}")
            return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    benchmark_generator = ESGBenchmarkGenerator()
    benchmark_generator.load_sector_benchmarks()
    comparisons = benchmark_generator.generate_company_comparisons()
    print("\nCompany-Benchmark Comparisons:")
    print(comparisons)