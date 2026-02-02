# ========================================================================
# ADVANCED EXCEL POLICY DATA ANALYZER FOR GOOGLE COLAB
# ========================================================================
# Features:
# - Multiple filtering options with interactive menus
# - Statistical analysis and insights
# - Data visualizations (charts and graphs)
# - Anomaly detection
# - Export to multiple formats
# - Data quality checks
# - Automated reporting
# ========================================================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from google.colab import files
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# ========================================================================
# CLASS: PolicyDataAnalyzer
# ========================================================================

class PolicyDataAnalyzer:
    """
    Advanced analyzer for insurance policy data with filtering,
    analysis, and visualization capabilities.
    """
    
    def __init__(self):
        self.df = None
        self.filtered_df = None
        self.original_count = 0
        self.analysis_results = {}
        
    def load_data(self, filename=None):
        """Load Excel file with data validation"""
        if filename is None:
            print("📁 Please upload your Excel file:")
            uploaded = files.upload()
            filename = list(uploaded.keys())[0]
        
        try:
            self.df = pd.read_excel(filename)
            self.original_count = len(self.df)
            print(f"✅ Successfully loaded {self.original_count} records")
            print(f"📊 Columns: {', '.join(self.df.columns.tolist())}")
            self._validate_data()
            return True
        except Exception as e:
            print(f"❌ Error loading file: {e}")
            return False
    
    def _validate_data(self):
        """Perform data quality checks"""
        print("\n" + "="*70)
        print("🔍 DATA QUALITY REPORT")
        print("="*70)
        
        # Check for missing values
        missing = self.df.isnull().sum()
        missing_pct = (missing / len(self.df)) * 100
        
        print("\n📋 Missing Values Analysis:")
        for col, count in missing.items():
            if count > 0:
                print(f"  - {col}: {count} ({missing_pct[col]:.2f}%)")
        
        # Check for duplicates
        duplicates = self.df.duplicated(subset=['POLICY_NUMBER']).sum()
        print(f"\n🔄 Duplicate Policy Numbers: {duplicates}")
        
        # Data type validation
        print("\n📊 Data Types:")
        for col, dtype in self.df.dtypes.items():
            print(f"  - {col}: {dtype}")
        
        # Statistical summary for numeric columns
        print("\n📈 Numeric Columns Summary:")
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        print(self.df[numeric_cols].describe())
    
    def apply_advanced_filters(self, filter_config):
        """
        Apply multiple filters based on configuration dictionary
        
        filter_config example:
        {
            'insurance_types': ['Home', 'Life'],
            'companies': ['Unity Cover'],
            'policy_age_min': 3,
            'policy_age_max': 10,
            'premium_min': 5000,
            'premium_max': 50000,
            'claim_status': ['Approved'],
            'registration_date_start': '2015-01-01',
            'registration_date_end': '2025-01-01',
            'has_claims': True
        }
        """
        self.filtered_df = self.df.copy()
        filter_count = 0
        
        print("\n" + "="*70)
        print("🔧 APPLYING FILTERS")
        print("="*70)
        
        # Filter by Insurance Type
        if 'insurance_types' in filter_config and filter_config['insurance_types']:
            self.filtered_df = self.filtered_df[
                self.filtered_df['INSURANCE_TYPE'].isin(filter_config['insurance_types'])
            ]
            filter_count += 1
            print(f"✓ Insurance Types: {filter_config['insurance_types']}")
        
        # Filter by Companies
        if 'companies' in filter_config and filter_config['companies']:
            self.filtered_df = self.filtered_df[
                self.filtered_df['INSURANCE_COMPANY_NAME'].isin(filter_config['companies'])
            ]
            filter_count += 1
            print(f"✓ Companies: {filter_config['companies']}")
        
        # Filter by Policy Age Range
        if 'policy_age_min' in filter_config:
            self.filtered_df = self.filtered_df[
                self.filtered_df['PolicyAge'] >= filter_config['policy_age_min']
            ]
            filter_count += 1
            print(f"✓ Minimum Policy Age: {filter_config['policy_age_min']}")
        
        if 'policy_age_max' in filter_config:
            self.filtered_df = self.filtered_df[
                self.filtered_df['PolicyAge'] <= filter_config['policy_age_max']
            ]
            filter_count += 1
            print(f"✓ Maximum Policy Age: {filter_config['policy_age_max']}")
        
        # Filter by Premium Amount Range
        if 'premium_min' in filter_config:
            self.filtered_df = self.filtered_df[
                self.filtered_df['PREMIUM_AMOUNT'] >= filter_config['premium_min']
            ]
            filter_count += 1
            print(f"✓ Minimum Premium: ₹{filter_config['premium_min']:,.2f}")
        
        if 'premium_max' in filter_config:
            self.filtered_df = self.filtered_df[
                self.filtered_df['PREMIUM_AMOUNT'] <= filter_config['premium_max']
            ]
            filter_count += 1
            print(f"✓ Maximum Premium: ₹{filter_config['premium_max']:,.2f}")
        
        # Filter by Claim Status
        if 'claim_status' in filter_config and filter_config['claim_status']:
            self.filtered_df = self.filtered_df[
                self.filtered_df['CLAIM_STATUS'].isin(filter_config['claim_status'])
            ]
            filter_count += 1
            print(f"✓ Claim Status: {filter_config['claim_status']}")
        
        # Filter by Registration Date Range
        if 'registration_date_start' in filter_config:
            self.filtered_df = self.filtered_df[
                self.filtered_df['REGISTRATION_DATE'] >= filter_config['registration_date_start']
            ]
            filter_count += 1
            print(f"✓ Registration From: {filter_config['registration_date_start']}")
        
        if 'registration_date_end' in filter_config:
            self.filtered_df = self.filtered_df[
                self.filtered_df['REGISTRATION_DATE'] <= filter_config['registration_date_end']
            ]
            filter_count += 1
            print(f"✓ Registration Until: {filter_config['registration_date_end']}")
        
        # Filter policies with/without claims
        if 'has_claims' in filter_config:
            if filter_config['has_claims']:
                self.filtered_df = self.filtered_df[
                    self.filtered_df['CLAIM_PAID_AMOUNT'].notna() & 
                    (self.filtered_df['CLAIM_PAID_AMOUNT'] > 0)
                ]
                print(f"✓ Only policies WITH claims")
            else:
                self.filtered_df = self.filtered_df[
                    self.filtered_df['CLAIM_PAID_AMOUNT'].isna() | 
                    (self.filtered_df['CLAIM_PAID_AMOUNT'] == 0)
                ]
                print(f"✓ Only policies WITHOUT claims")
            filter_count += 1
        
        # Filter by Customer Age
        if 'customer_age_min' in filter_config or 'customer_age_max' in filter_config:
            self.filtered_df['CUSTOMER_AGE'] = (
                datetime.now() - pd.to_datetime(self.filtered_df['DOB'])
            ).dt.days / 365.25
            
            if 'customer_age_min' in filter_config:
                self.filtered_df = self.filtered_df[
                    self.filtered_df['CUSTOMER_AGE'] >= filter_config['customer_age_min']
                ]
                filter_count += 1
                print(f"✓ Minimum Customer Age: {filter_config['customer_age_min']}")
            
            if 'customer_age_max' in filter_config:
                self.filtered_df = self.filtered_df[
                    self.filtered_df['CUSTOMER_AGE'] <= filter_config['customer_age_max']
                ]
                filter_count += 1
                print(f"✓ Maximum Customer Age: {filter_config['customer_age_max']}")
        
        print(f"\n📊 Applied {filter_count} filters")
        print(f"📉 Filtered from {self.original_count} to {len(self.filtered_df)} records")
        print(f"📈 Retention rate: {(len(self.filtered_df)/self.original_count)*100:.2f}%")
        
        return self.filtered_df
    
    def calculate_metrics(self):
        """Calculate comprehensive metrics for filtered data"""
        if self.filtered_df is None or len(self.filtered_df) == 0:
            print("⚠️ No filtered data available")
            return None
        
        metrics = {}
        
        # Basic metrics
        metrics['total_policies'] = len(self.filtered_df)
        metrics['total_premium_collected'] = self.filtered_df['PREMIUM_AMOUNT'].sum() * \
                                             self.filtered_df['PolicyAge'].mean()
        metrics['avg_premium_per_policy'] = self.filtered_df['PREMIUM_AMOUNT'].mean()
        metrics['avg_policy_age'] = self.filtered_df['PolicyAge'].mean()
        
        # Claims metrics
        metrics['total_claims'] = self.filtered_df['CLAIM_PAID_AMOUNT'].notna().sum()
        metrics['total_claims_paid'] = self.filtered_df['CLAIM_PAID_AMOUNT'].sum()
        metrics['avg_claim_amount'] = self.filtered_df['CLAIM_PAID_AMOUNT'].mean()
        metrics['claim_ratio'] = (metrics['total_claims'] / metrics['total_policies']) * 100
        
        # Loss ratio (claims paid / premiums collected)
        if metrics['total_premium_collected'] > 0:
            metrics['loss_ratio'] = (metrics['total_claims_paid'] / 
                                    metrics['total_premium_collected']) * 100
        else:
            metrics['loss_ratio'] = 0
        
        # Premium distribution
        metrics['premium_std'] = self.filtered_df['PREMIUM_AMOUNT'].std()
        metrics['premium_min'] = self.filtered_df['PREMIUM_AMOUNT'].min()
        metrics['premium_max'] = self.filtered_df['PREMIUM_AMOUNT'].max()
        metrics['premium_median'] = self.filtered_df['PREMIUM_AMOUNT'].median()
        
        # Policy age distribution
        metrics['policy_age_min'] = self.filtered_df['PolicyAge'].min()
        metrics['policy_age_max'] = self.filtered_df['PolicyAge'].max()
        metrics['policy_age_median'] = self.filtered_df['PolicyAge'].median()
        
        self.analysis_results = metrics
        return metrics
    
    def display_metrics_report(self):
        """Display formatted metrics report"""
        if not self.analysis_results:
            self.calculate_metrics()
        
        print("\n" + "="*70)
        print("📊 COMPREHENSIVE METRICS REPORT")
        print("="*70)
        
        m = self.analysis_results
        
        print("\n📋 POLICY OVERVIEW:")
        print(f"  Total Policies: {m['total_policies']:,}")
        print(f"  Average Policy Age: {m['avg_policy_age']:.2f} years")
        print(f"  Age Range: {m['policy_age_min']:.0f} - {m['policy_age_max']:.0f} years")
        print(f"  Median Policy Age: {m['policy_age_median']:.2f} years")
        
        print("\n💰 PREMIUM ANALYSIS:")
        print(f"  Total Premium Collected: ₹{m['total_premium_collected']:,.2f}")
        print(f"  Average Premium/Policy: ₹{m['avg_premium_per_policy']:,.2f}")
        print(f"  Median Premium: ₹{m['premium_median']:,.2f}")
        print(f"  Premium Range: ₹{m['premium_min']:,.2f} - ₹{m['premium_max']:,.2f}")
        print(f"  Premium Std Dev: ₹{m['premium_std']:,.2f}")
        
        print("\n🏥 CLAIMS ANALYSIS:")
        print(f"  Total Claims Filed: {m['total_claims']:,}")
        print(f"  Total Claims Paid: ₹{m['total_claims_paid']:,.2f}")
        print(f"  Average Claim Amount: ₹{m['avg_claim_amount']:,.2f}")
        print(f"  Claim Frequency: {m['claim_ratio']:.2f}%")
        
        print("\n📈 PERFORMANCE INDICATORS:")
        print(f"  Loss Ratio: {m['loss_ratio']:.2f}%")
        if m['loss_ratio'] < 60:
            print(f"  Status: ✅ Healthy (Good profitability)")
        elif m['loss_ratio'] < 80:
            print(f"  Status: ⚠️ Moderate (Acceptable range)")
        else:
            print(f"  Status: ❌ High (Review needed)")
    
    def detect_anomalies(self):
        """Detect anomalies in the data using statistical methods"""
        if self.filtered_df is None or len(self.filtered_df) == 0:
            return
        
        print("\n" + "="*70)
        print("🔍 ANOMALY DETECTION REPORT")
        print("="*70)
        
        anomalies_found = False
        
        # Anomaly 1: Unusually high premiums (3 standard deviations)
        premium_mean = self.filtered_df['PREMIUM_AMOUNT'].mean()
        premium_std = self.filtered_df['PREMIUM_AMOUNT'].std()
        high_premium_threshold = premium_mean + (3 * premium_std)
        
        high_premiums = self.filtered_df[
            self.filtered_df['PREMIUM_AMOUNT'] > high_premium_threshold
        ]
        
        if len(high_premiums) > 0:
            print(f"\n⚠️ Found {len(high_premiums)} policies with unusually HIGH premiums:")
            print(high_premiums[['POLICY_NUMBER', 'CUSTOMER_NAME', 'PREMIUM_AMOUNT', 
                                 'INSURANCE_TYPE']].head())
            anomalies_found = True
        
        # Anomaly 2: Large claim amounts compared to premiums
        if 'Total_Premim_Paid' in self.filtered_df.columns:
            suspicious_claims = self.filtered_df[
                (self.filtered_df['CLAIM_PAID_AMOUNT'] > 
                 self.filtered_df['Total_Premim_Paid'] * 2) &
                (self.filtered_df['CLAIM_PAID_AMOUNT'].notna())
            ]
            
            if len(suspicious_claims) > 0:
                print(f"\n⚠️ Found {len(suspicious_claims)} claims EXCEEDING 2x total premiums paid:")
                print(suspicious_claims[['POLICY_NUMBER', 'CUSTOMER_NAME', 'CLAIM_PAID_AMOUNT',
                                        'Total_Premim_Paid']].head())
                anomalies_found = True
        
        # Anomaly 3: Very old policies (>20 years)
        old_policies = self.filtered_df[self.filtered_df['PolicyAge'] > 20]
        if len(old_policies) > 0:
            print(f"\n📅 Found {len(old_policies)} very old policies (>20 years):")
            print(old_policies[['POLICY_NUMBER', 'CUSTOMER_NAME', 'PolicyAge',
                               'REGISTRATION_DATE']].head())
            anomalies_found = True
        
        # Anomaly 4: Multiple claims from same customer
        claim_counts = self.filtered_df.groupby('CUSTOMER_NAME')['CLAIM_NO'].count()
        frequent_claimants = claim_counts[claim_counts > 1].sort_values(ascending=False)
        
        if len(frequent_claimants) > 0:
            print(f"\n🔄 Found {len(frequent_claimants)} customers with multiple claims:")
            print(frequent_claimants.head(10))
            anomalies_found = True
        
        if not anomalies_found:
            print("\n✅ No significant anomalies detected in the filtered data.")
    
    def generate_visualizations(self):
        """Generate comprehensive data visualizations"""
        if self.filtered_df is None or len(self.filtered_df) == 0:
            print("⚠️ No data to visualize")
            return
        
        print("\n" + "="*70)
        print("📊 GENERATING VISUALIZATIONS")
        print("="*70)
        
        # Figure 1: Premium Distribution by Insurance Type
        plt.figure(figsize=(14, 10))
        
        plt.subplot(2, 3, 1)
        insurance_premiums = self.filtered_df.groupby('INSURANCE_TYPE')['PREMIUM_AMOUNT'].sum()
        plt.pie(insurance_premiums.values, labels=insurance_premiums.index, autopct='%1.1f%%')
        plt.title('Premium Distribution by Insurance Type')
        
        # Figure 2: Policy Count by Company
        plt.subplot(2, 3, 2)
        company_counts = self.filtered_df['INSURANCE_COMPANY_NAME'].value_counts()
        plt.bar(range(len(company_counts)), company_counts.values)
        plt.xticks(range(len(company_counts)), company_counts.index, rotation=45, ha='right')
        plt.xlabel('Insurance Company')
        plt.ylabel('Number of Policies')
        plt.title('Policy Count by Company')
        plt.tight_layout()
        
        # Figure 3: Claim Status Distribution
        plt.subplot(2, 3, 3)
        claim_status = self.filtered_df['CLAIM_STATUS'].value_counts()
        colors = {'Approved': 'green', 'Rejected': 'red', 'Pending': 'orange'}
        bar_colors = [colors.get(status, 'gray') for status in claim_status.index]
        plt.bar(range(len(claim_status)), claim_status.values, color=bar_colors)
        plt.xticks(range(len(claim_status)), claim_status.index)
        plt.xlabel('Claim Status')
        plt.ylabel('Count')
        plt.title('Claim Status Distribution')
        
        # Figure 4: Premium Amount Distribution (Histogram)
        plt.subplot(2, 3, 4)
        plt.hist(self.filtered_df['PREMIUM_AMOUNT'], bins=30, edgecolor='black', alpha=0.7)
        plt.xlabel('Premium Amount (₹)')
        plt.ylabel('Frequency')
        plt.title('Premium Amount Distribution')
        plt.axvline(self.filtered_df['PREMIUM_AMOUNT'].mean(), color='red', 
                   linestyle='--', label='Mean')
        plt.legend()
        
        # Figure 5: Policy Age Distribution
        plt.subplot(2, 3, 5)
        plt.hist(self.filtered_df['PolicyAge'], bins=20, edgecolor='black', alpha=0.7, color='skyblue')
        plt.xlabel('Policy Age (years)')
        plt.ylabel('Frequency')
        plt.title('Policy Age Distribution')
        
        # Figure 6: Claims Paid vs Premium Amount Scatter
        plt.subplot(2, 3, 6)
        claim_data = self.filtered_df[self.filtered_df['CLAIM_PAID_AMOUNT'].notna()]
        if len(claim_data) > 0:
            plt.scatter(claim_data['PREMIUM_AMOUNT'], claim_data['CLAIM_PAID_AMOUNT'], 
                       alpha=0.6, s=50)
            plt.xlabel('Premium Amount (₹)')
            plt.ylabel('Claim Paid Amount (₹)')
            plt.title('Claims vs Premium Correlation')
            
            # Add trend line
            z = np.polyfit(claim_data['PREMIUM_AMOUNT'], claim_data['CLAIM_PAID_AMOUNT'], 1)
            p = np.poly1d(z)
            plt.plot(claim_data['PREMIUM_AMOUNT'], 
                    p(claim_data['PREMIUM_AMOUNT']), 
                    "r--", alpha=0.8, label='Trend')
            plt.legend()
        
        plt.tight_layout()
        plt.savefig('policy_analysis_charts.png', dpi=300, bbox_inches='tight')
        print("✅ Visualization saved as 'policy_analysis_charts.png'")
        plt.show()
        
        # Additional Time Series Analysis
        if 'REGISTRATION_DATE' in self.filtered_df.columns:
            plt.figure(figsize=(14, 5))
            
            # Policies registered over time
            self.filtered_df['REG_YEAR'] = pd.to_datetime(
                self.filtered_df['REGISTRATION_DATE']
            ).dt.year
            year_counts = self.filtered_df['REG_YEAR'].value_counts().sort_index()
            
            plt.subplot(1, 2, 1)
            plt.plot(year_counts.index, year_counts.values, marker='o', linewidth=2)
            plt.xlabel('Year')
            plt.ylabel('Number of Policies Registered')
            plt.title('Policy Registration Trend Over Time')
            plt.grid(True, alpha=0.3)
            
            # Average premium over time
            plt.subplot(1, 2, 2)
            avg_premium_by_year = self.filtered_df.groupby('REG_YEAR')['PREMIUM_AMOUNT'].mean()
            plt.bar(avg_premium_by_year.index, avg_premium_by_year.values, alpha=0.7)
            plt.xlabel('Year')
            plt.ylabel('Average Premium Amount (₹)')
            plt.title('Average Premium Trend Over Time')
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            plt.savefig('time_series_analysis.png', dpi=300, bbox_inches='tight')
            print("✅ Time series visualization saved as 'time_series_analysis.png'")
            plt.show()
    
    def generate_insights(self):
        """Generate AI-like insights from the data"""
        if self.filtered_df is None or len(self.filtered_df) == 0:
            return
        
        print("\n" + "="*70)
        print("🧠 DATA INSIGHTS & RECOMMENDATIONS")
        print("="*70)
        
        insights = []
        
        # Insight 1: Most profitable insurance type
        type_profit = self.filtered_df.groupby('INSURANCE_TYPE').agg({
            'PREMIUM_AMOUNT': 'sum',
            'CLAIM_PAID_AMOUNT': 'sum'
        })
        type_profit['PROFIT'] = type_profit['PREMIUM_AMOUNT'] - type_profit['CLAIM_PAID_AMOUNT']
        most_profitable = type_profit['PROFIT'].idxmax()
        insights.append(f"💡 '{most_profitable}' insurance is the most profitable type "
                       f"with ₹{type_profit['PROFIT'].max():,.2f} in net profit.")
        
        # Insight 2: High-risk customers
        claim_rate_by_company = self.filtered_df.groupby('INSURANCE_COMPANY_NAME').agg({
            'CLAIM_NO': 'count',
            'POLICY_NUMBER': 'count'
        })
        claim_rate_by_company['CLAIM_RATE'] = (
            claim_rate_by_company['CLAIM_NO'] / claim_rate_by_company['POLICY_NUMBER']
        ) * 100
        highest_claim_rate = claim_rate_by_company['CLAIM_RATE'].idxmax()
        insights.append(f"⚠️ '{highest_claim_rate}' has the highest claim rate at "
                       f"{claim_rate_by_company['CLAIM_RATE'].max():.2f}%.")
        
        # Insight 3: Premium growth potential
        avg_premium = self.filtered_df['PREMIUM_AMOUNT'].mean()
        low_premium_policies = len(self.filtered_df[
            self.filtered_df['PREMIUM_AMOUNT'] < avg_premium * 0.7
        ])
        if low_premium_policies > 0:
            insights.append(f"📈 {low_premium_policies} policies have premiums 30% below average. "
                           f"Potential for premium optimization.")
        
        # Insight 4: Customer retention
        old_policies = len(self.filtered_df[self.filtered_df['PolicyAge'] > 10])
        retention_rate = (old_policies / len(self.filtered_df)) * 100
        if retention_rate > 50:
            insights.append(f"✅ Strong customer retention: {retention_rate:.1f}% of policies "
                           f"are 10+ years old.")
        else:
            insights.append(f"⚠️ Low retention rate: Only {retention_rate:.1f}% of policies "
                           f"are 10+ years old. Consider loyalty programs.")
        
        # Insight 5: Claims processing efficiency
        if 'CLAIM_STATUS' in self.filtered_df.columns:
            pending_claims = len(self.filtered_df[self.filtered_df['CLAIM_STATUS'] == 'Pending'])
            if pending_claims > 0:
                pending_pct = (pending_claims / len(self.filtered_df)) * 100
                insights.append(f"⏳ {pending_claims} claims ({pending_pct:.1f}%) are still pending. "
                               f"Review for process optimization.")
        
        # Display insights
        for i, insight in enumerate(insights, 1):
            print(f"\n{i}. {insight}")
    
    def export_results(self, formats=['xlsx', 'csv', 'json']):
        """Export filtered data to multiple formats"""
        if self.filtered_df is None or len(self.filtered_df) == 0:
            print("⚠️ No data to export")
            return
        
        print("\n" + "="*70)
        print("💾 EXPORTING RESULTS")
        print("="*70)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_filename = f"filtered_policy_data_{timestamp}"
        
        exported_files = []
        
        # Export to Excel
        if 'xlsx' in formats:
            excel_file = f"{base_filename}.xlsx"
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                self.filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)
                
                # Add summary sheet
                summary_df = pd.DataFrame([self.analysis_results]).T
                summary_df.columns = ['Value']
                summary_df.to_excel(writer, sheet_name='Summary Metrics')
                
                # Add pivot tables
                pivot1 = pd.pivot_table(self.filtered_df, 
                                       values='PREMIUM_AMOUNT',
                                       index='INSURANCE_TYPE',
                                       aggfunc=['sum', 'mean', 'count'])
                pivot1.to_excel(writer, sheet_name='By Insurance Type')
                
                pivot2 = pd.pivot_table(self.filtered_df,
                                       values='CLAIM_PAID_AMOUNT',
                                       index='CLAIM_STATUS',
                                       aggfunc=['sum', 'count'])
                pivot2.to_excel(writer, sheet_name='Claims Analysis')
            
            exported_files.append(excel_file)
            print(f"✅ Excel file: {excel_file}")
        
        # Export to CSV
        if 'csv' in formats:
            csv_file = f"{base_filename}.csv"
            self.filtered_df.to_csv(csv_file, index=False)
            exported_files.append(csv_file)
            print(f"✅ CSV file: {csv_file}")
        
        # Export to JSON
        if 'json' in formats:
            json_file = f"{base_filename}.json"
            self.filtered_df.to_json(json_file, orient='records', indent=2)
            exported_files.append(json_file)
            print(f"✅ JSON file: {json_file}")
        
        # Download all files
        print("\n📥 Downloading files...")
        for file in exported_files:
            files.download(file)
        
        print(f"\n✅ Successfully exported {len(exported_files)} files!")
        
        return exported_files


# ========================================================================
# MAIN EXECUTION SCRIPT
# ========================================================================

def main():
    """Main execution function with interactive menu"""
    
    print("="*70)
    print("🏢 ADVANCED INSURANCE POLICY DATA ANALYZER")
    print("="*70)
    print("\nWelcome! This tool provides comprehensive analysis of policy data.")
    print("Features: Filtering, Statistics, Visualizations, Anomaly Detection")
    print("="*70)
    
    # Initialize analyzer
    analyzer = PolicyDataAnalyzer()
    
    # Load data
    if not analyzer.load_data():
        print("❌ Failed to load data. Exiting...")
        return
    
    # Define filter configuration
    # CUSTOMIZE THESE FILTERS BASED ON YOUR NEEDS
    filter_config = {
        'insurance_types': ['Home', 'Life', 'Travel', 'Auto'],
        'companies': ['Unity Cover', 'Skyline Insurance', 'Apex Insurance Co.', 'Global Assurance'],
        'policy_age_min': 3,
        'policy_age_max': 7,
        'claim_status': ['Approved', 'Pending', 'Rejected'],
        # 'premium_min': 5000,
        # 'premium_max': 50000,
        # 'registration_date_start': '2015-01-01',
        # 'has_claims': True,
        # 'customer_age_min': 25,
        # 'customer_age_max': 65
    }
    
    # Apply filters
    analyzer.apply_advanced_filters(filter_config)
    
    # Calculate and display metrics
    analyzer.calculate_metrics()
    analyzer.display_metrics_report()
    
    # Detect anomalies
    analyzer.detect_anomalies()
    
    # Generate insights
    analyzer.generate_insights()
    
    # Generate visualizations
    analyzer.generate_visualizations()
    
    # Export results
    analyzer.export_results(formats=['xlsx', 'csv', 'json'])
    
    print("\n" + "="*70)
    print("✅ ANALYSIS COMPLETE!")
    print("="*70)
    print("\nThank you for using the Advanced Policy Data Analyzer!")


# ========================================================================
# ALTERNATIVE: CUSTOM FILTER EXAMPLES
# ========================================================================

def example_custom_filters():
    """Examples of different filter configurations"""
    
    # Example 1: High-value policies only
    high_value_config = {
        'premium_min': 20000,
        'policy_age_min': 5,
        'has_claims': False
    }
    
    # Example 2: Recently registered policies with claims
    recent_claims_config = {
        'registration_date_start': '2020-01-01',
        'has_claims': True,
        'claim_status': ['Approved']
    }
    
    # Example 3: Life insurance policies from specific company
    life_insurance_config = {
        'insurance_types': ['Life'],
        'companies': ['Unity Cover', 'Shield Mutual'],
        'customer_age_min': 30,
        'customer_age_max': 60
    }
    
    # Example 4: Problem policies (high claims)
    problem_policies_config = {
        'has_claims': True,
        'claim_status': ['Approved'],
        # This would need custom logic in the filter method
    }
    
    return [high_value_config, recent_claims_config, 
            life_insurance_config, problem_policies_config]


# ========================================================================
# RUN THE ANALYSIS
# ========================================================================

if __name__ == "__main__":
    main()
    
    # Uncomment below to see example filter configurations
    # examples = example_custom_filters()
    # print("\n📚 Available filter configuration examples:")
    # for i, config in enumerate(examples, 1):
    #     print(f"\nExample {i}:")
    #     for key, value in config.items():
    #         print(f"  {key}: {value}")
