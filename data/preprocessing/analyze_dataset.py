"""
Comprehensive Dataset Analysis Tool
Analyzes any CSV dataset and provides detailed statistics, insights, and recommendations
"""

import pandas as pd
import numpy as np
import os
import sys
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import argparse

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

class DatasetAnalyzer:
    """Comprehensive dataset analyzer with detailed reporting"""
    
    def __init__(self, file_path: str, sample_size: int = 3000000):
        self.file_path = file_path
        self.sample_size = sample_size
        self.df = None
        self.total_rows = 0
        self.analysis_results = {}
        
    def load_dataset(self) -> bool:
        """Load dataset and basic information"""
        print(f"📁 LOADING DATASET: {self.file_path}")
        print("="*60)
        
        if not os.path.exists(self.file_path):
            print(f"❌ File not found: {self.file_path}")
            return False
        
        try:
            # Get file size
            file_size_mb = os.path.getsize(self.file_path) / (1024 * 1024)
            print(f"💾 File size: {file_size_mb:.1f} MB")
            
            # Count total rows
            print("🔍 Counting total rows...")
            self.total_rows = sum(1 for _ in open(self.file_path, encoding='utf-8')) - 1
            print(f"📏 Total rows: {self.total_rows:,}")
            
            # Load sample
            actual_sample_size = min(self.sample_size, self.total_rows)
            print(f"📊 Loading sample: {actual_sample_size:,} rows")
            
            self.df = pd.read_csv(self.file_path, nrows=actual_sample_size, low_memory=False)
            
            print(f"✅ Dataset loaded successfully")
            print(f"📐 Columns: {len(self.df.columns)}")
            print(f"🔍 Sample size: {len(self.df):,} rows")
            
            return True
            
        except Exception as e:
            print(f"❌ Error loading dataset: {e}")
            return False
    
    def analyze_basic_info(self):
        """Analyze basic dataset information"""
        print(f"\n📋 BASIC DATASET INFORMATION")
        print("="*60)
        
        # Dataset overview
        print(f"📁 File: {os.path.basename(self.file_path)}")
        print(f"📏 Total rows: {self.total_rows:,}")
        print(f"📐 Total columns: {len(self.df.columns)}")
        print(f"🔍 Analysis based on: {len(self.df):,} rows sample")
        
        # Column names
        print(f"\n📝 COLUMN NAMES:")
        for i, col in enumerate(self.df.columns, 1):
            print(f"  {i:2d}. {col}")
        
        # Data types summary
        print(f"\n📊 DATA TYPES SUMMARY:")
        dtype_counts = self.df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            percentage = (count / len(self.df.columns)) * 100
            print(f"  {str(dtype):15s}: {count:2d} columns ({percentage:5.1f}%)")
        
        # Memory usage
        memory_mb = self.df.memory_usage(deep=True).sum() / (1024 * 1024)
        estimated_full_memory = memory_mb * (self.total_rows / len(self.df))
        print(f"\n💾 MEMORY USAGE:")
        print(f"  Sample memory: {memory_mb:.2f} MB")
        print(f"  Estimated full dataset: {estimated_full_memory:.2f} MB")
        
        self.analysis_results['basic_info'] = {
            'total_rows': self.total_rows,
            'total_columns': len(self.df.columns),
            'sample_memory_mb': memory_mb,
            'estimated_memory_mb': estimated_full_memory,
            'dtype_counts': dtype_counts.to_dict()
        }
    
    def analyze_missing_values(self):
        """Analyze missing values in detail"""
        print(f"\n❓ MISSING VALUES ANALYSIS")
        print("="*60)
        
        missing_counts = self.df.isnull().sum()
        missing_percentages = (missing_counts / len(self.df)) * 100
        
        # Overall missing data
        total_missing = missing_counts.sum()
        total_cells = len(self.df) * len(self.df.columns)
        overall_missing_pct = (total_missing / total_cells) * 100
        
        print(f"📊 OVERALL MISSING DATA:")
        print(f"  Total missing values: {total_missing:,}")
        print(f"  Total cells: {total_cells:,}")
        print(f"  Overall missing percentage: {overall_missing_pct:.2f}%")
        
        # Missing values by column
        missing_data = pd.DataFrame({
            'Column': missing_counts.index,
            'Missing_Count': missing_counts.values,
            'Missing_Percentage': missing_percentages.values
        }).sort_values('Missing_Percentage', ascending=False)
        
        print(f"\n📋 MISSING VALUES BY COLUMN (Top 15):")
        print("  {:25s} {:>12s} {:>12s}".format("Column", "Missing", "Percentage"))
        print("  " + "-" * 50)
        
        for _, row in missing_data.head(15).iterrows():
            if row['Missing_Count'] > 0:
                print(f"  {row['Column']:25s} {row['Missing_Count']:>12,} {row['Missing_Percentage']:>11.2f}%")
        
        # Missing value categories
        no_missing = (missing_percentages == 0).sum()
        low_missing = ((missing_percentages > 0) & (missing_percentages <= 5)).sum()
        medium_missing = ((missing_percentages > 5) & (missing_percentages <= 25)).sum()
        high_missing = ((missing_percentages > 25) & (missing_percentages <= 75)).sum()
        very_high_missing = (missing_percentages > 75).sum()
        
        print(f"\n📊 MISSING VALUE CATEGORIES:")
        print(f"  No missing (0%):           {no_missing:3d} columns")
        print(f"  Low missing (0-5%):        {low_missing:3d} columns")
        print(f"  Medium missing (5-25%):    {medium_missing:3d} columns")
        print(f"  High missing (25-75%):     {high_missing:3d} columns")
        print(f"  Very high missing (>75%):  {very_high_missing:3d} columns")
        
        self.analysis_results['missing_values'] = {
            'total_missing': int(total_missing),
            'overall_missing_pct': overall_missing_pct,
            'missing_by_column': missing_data.to_dict('records'),
            'categories': {
                'no_missing': int(no_missing),
                'low_missing': int(low_missing),
                'medium_missing': int(medium_missing),
                'high_missing': int(high_missing),
                'very_high_missing': int(very_high_missing)
            }
        }
    
    def analyze_numeric_columns(self):
        """Analyze numeric columns in detail"""
        print(f"\n🔢 NUMERIC COLUMNS ANALYSIS")
        print("="*60)
        
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) == 0:
            print("  ℹ️  No numeric columns found")
            return
        
        print(f"📊 Found {len(numeric_cols)} numeric columns")
        
        print(f"\n📈 NUMERIC STATISTICS:")
        print("  {:20s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s}".format(
            "Column", "Min", "Max", "Mean", "Median", "Std"))
        print("  " + "-" * 75)
        
        numeric_stats = []
        for col in numeric_cols:
            if self.df[col].notna().sum() > 0:  # Only if column has non-null values
                stats = {
                    'column': col,
                    'min': self.df[col].min(),
                    'max': self.df[col].max(),
                    'mean': self.df[col].mean(),
                    'median': self.df[col].median(),
                    'std': self.df[col].std()
                }
                numeric_stats.append(stats)
                
                print(f"  {col:20s} {stats['min']:>10.2f} {stats['max']:>10.2f} "
                      f"{stats['mean']:>10.2f} {stats['median']:>10.2f} {stats['std']:>10.2f}")
        
        # Data type optimization opportunities
        print(f"\n🔧 DATA TYPE OPTIMIZATION OPPORTUNITIES:")
        
        int_optimizations = []
        float_optimizations = []
        
        for col in numeric_cols:
            if self.df[col].dtype == 'int64':
                min_val = self.df[col].min()
                max_val = self.df[col].max()
                
                if pd.notna(min_val) and pd.notna(max_val):
                    if min_val >= 0 and max_val <= 255:
                        suggestion = "uint8 (0-255)"
                    elif min_val >= -128 and max_val <= 127:
                        suggestion = "int8 (-128 to 127)"
                    elif min_val >= 0 and max_val <= 65535:
                        suggestion = "uint16 (0-65535)"
                    elif min_val >= -32768 and max_val <= 32767:
                        suggestion = "int16 (-32768 to 32767)"
                    elif min_val >= -2147483648 and max_val <= 2147483647:
                        suggestion = "int32 (32-bit)"
                    else:
                        suggestion = "int64 (current)"
                    
                    if suggestion != "int64 (current)":
                        int_optimizations.append(f"  {col:25s} → {suggestion}")
            
            elif self.df[col].dtype == 'float64':
                float_optimizations.append(f"  {col:25s} → float32 (50% memory reduction)")
        
        if int_optimizations:
            print("  Integer optimizations:")
            for opt in int_optimizations[:10]:  # Show top 10
                print(opt)
        
        if float_optimizations:
            print("  Float optimizations:")
            for opt in float_optimizations[:10]:  # Show top 10
                print(opt)
        
        if not int_optimizations and not float_optimizations:
            print("  ✅ All numeric types appear to be optimally sized")
        
        self.analysis_results['numeric_analysis'] = {
            'numeric_columns_count': len(numeric_cols),
            'statistics': numeric_stats,
            'optimization_opportunities': {
                'integer': len(int_optimizations),
                'float': len(float_optimizations)
            }
        }
    
    def analyze_categorical_columns(self):
        """Analyze categorical/text columns"""
        print(f"\n🏷️ CATEGORICAL COLUMNS ANALYSIS")
        print("="*60)
        
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        
        if len(categorical_cols) == 0:
            print("  ℹ️  No categorical columns found")
            return
        
        print(f"📊 Found {len(categorical_cols)} categorical columns")
        
        print(f"\n📝 CATEGORICAL STATISTICS:")
        print("  {:25s} {:>12s} {:>12s} {:>12s}".format(
            "Column", "Unique", "Cardinality", "Most Frequent"))
        print("  " + "-" * 65)
        
        categorical_stats = []
        
        for col in categorical_cols:
            unique_count = self.df[col].nunique()
            cardinality_ratio = unique_count / len(self.df)
            
            # Get most frequent value
            if self.df[col].notna().sum() > 0:
                most_frequent = self.df[col].value_counts().index[0]
                most_frequent_str = str(most_frequent)[:15] + "..." if len(str(most_frequent)) > 15 else str(most_frequent)
            else:
                most_frequent_str = "N/A"
            
            stats = {
                'column': col,
                'unique_count': unique_count,
                'cardinality_ratio': cardinality_ratio,
                'most_frequent': most_frequent_str
            }
            categorical_stats.append(stats)
            
            print(f"  {col:25s} {unique_count:>12,} {cardinality_ratio:>11.3f} {most_frequent_str:>12s}")
        
        # Categorization recommendations
        print(f"\n💡 CATEGORIZATION RECOMMENDATIONS:")
        
        low_cardinality = []
        high_cardinality = []
        
        for stats in categorical_stats:
            if stats['cardinality_ratio'] < 0.1:  # Less than 10% unique values
                memory_savings = "70-90% memory reduction"
                low_cardinality.append(f"  {stats['column']:25s} → Category type ({memory_savings})")
            elif stats['cardinality_ratio'] > 0.8:  # More than 80% unique values
                high_cardinality.append(f"  {stats['column']:25s} → Consider dropping or encoding")
        
        if low_cardinality:
            print("  Good candidates for category type:")
            for rec in low_cardinality[:10]:
                print(rec)
        
        if high_cardinality:
            print("  High cardinality columns (consider special handling):")
            for rec in high_cardinality[:5]:
                print(rec)
        
        if not low_cardinality and not high_cardinality:
            print("  ✅ Categorical columns have reasonable cardinality")
        
        self.analysis_results['categorical_analysis'] = {
            'categorical_columns_count': len(categorical_cols),
            'statistics': categorical_stats,
            'recommendations': {
                'low_cardinality_count': len(low_cardinality),
                'high_cardinality_count': len(high_cardinality)
            }
        }
    
    def analyze_data_quality(self):
        """Analyze data quality issues"""
        print(f"\n✅ DATA QUALITY ANALYSIS")
        print("="*60)
        
        quality_issues = []
        
        # 1. Check for duplicates (if we have reasonable key columns)
        print("🔍 DUPLICATE ANALYSIS:")
        
        # Try to identify key columns
        potential_keys = []
        for col in self.df.columns:
            if any(keyword in col.upper() for keyword in ['LAT', 'LNG', 'TIME', 'DATE', 'ID']):
                potential_keys.append(col)
        
        if len(potential_keys) >= 2:
            try:
                # Check for duplicates on potential key columns
                duplicates = self.df.duplicated(subset=potential_keys[:4]).sum()  # Use first 4 key columns
                duplicate_pct = (duplicates / len(self.df)) * 100
                print(f"  Potential duplicates: {duplicates:,} ({duplicate_pct:.2f}%)")
                print(f"  Key columns used: {potential_keys[:4]}")
                quality_issues.append(f"Duplicates: {duplicates:,} rows")
            except:
                print("  Could not analyze duplicates with available columns")
        else:
            print("  Could not identify key columns for duplicate analysis")
        
        # 2. Check for outliers in numeric columns
        print(f"\n📊 OUTLIER ANALYSIS:")
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        outlier_columns = []
        for col in numeric_cols[:10]:  # Check first 10 numeric columns
            if self.df[col].notna().sum() > 0:
                Q1 = self.df[col].quantile(0.25)
                Q3 = self.df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                if IQR > 0:  # Avoid division by zero
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    outliers = ((self.df[col] < lower_bound) | (self.df[col] > upper_bound)).sum()
                    outlier_pct = (outliers / len(self.df)) * 100
                    
                    if outlier_pct > 5:  # More than 5% outliers
                        outlier_columns.append((col, outliers, outlier_pct))
                        print(f"  {col:25s}: {outliers:6,} outliers ({outlier_pct:5.1f}%)")
        
        if not outlier_columns:
            print("  ✅ No significant outliers detected in numeric columns")
        
        # 3. Check for data consistency
        print(f"\n🔗 DATA CONSISTENCY CHECKS:")
        
        # Check for negative values where they shouldn't be
        negative_issues = []
        for col in numeric_cols:
            if any(keyword in col.upper() for keyword in ['COUNT', 'SIZE', 'DISTANCE', 'SPEED', 'AGE']):
                if (self.df[col] < 0).any():
                    negative_count = (self.df[col] < 0).sum()
                    negative_issues.append(f"  {col}: {negative_count:,} negative values")
        
        if negative_issues:
            print("  ⚠️  Unexpected negative values found:")
            for issue in negative_issues:
                print(issue)
        else:
            print("  ✅ No unexpected negative values found")
        
        # 4. Check for extreme values
        print(f"\n⚡ EXTREME VALUES:")
        extreme_found = False
        
        for col in numeric_cols[:5]:  # Check first 5 numeric columns
            if self.df[col].notna().sum() > 0:
                min_val = self.df[col].min()
                max_val = self.df[col].max()
                mean_val = self.df[col].mean()
                
                # Check if max is way larger than mean
                if abs(max_val) > abs(mean_val) * 100:  # Max is 100x larger than mean
                    print(f"  {col:25s}: Extreme max value {max_val:,.2f} vs mean {mean_val:.2f}")
                    extreme_found = True
        
        if not extreme_found:
            print("  ✅ No extreme values detected")
        
        self.analysis_results['data_quality'] = {
            'potential_duplicates': duplicates if 'duplicates' in locals() else 0,
            'outlier_columns': len(outlier_columns),
            'consistency_issues': len(negative_issues),
            'quality_score': self._calculate_quality_score()
        }
    
    def _calculate_quality_score(self) -> float:
        """Calculate overall data quality score (0-100)"""
        score = 100.0
        
        # Deduct points for missing values
        if 'missing_values' in self.analysis_results:
            missing_pct = self.analysis_results['missing_values']['overall_missing_pct']
            score -= min(missing_pct, 30)  # Max 30 point deduction for missing values
        
        # Deduct points for high cardinality columns
        if 'categorical_analysis' in self.analysis_results:
            high_card_count = self.analysis_results['categorical_analysis']['recommendations']['high_cardinality_count']
            score -= high_card_count * 5  # 5 points per high cardinality column
        
        return max(score, 0)
    
    def generate_recommendations(self):
        """Generate optimization and improvement recommendations"""
        print(f"\n💡 OPTIMIZATION RECOMMENDATIONS")
        print("="*60)
        
        recommendations = []
        
        # Memory optimization recommendations
        if 'numeric_analysis' in self.analysis_results:
            num_stats = self.analysis_results['numeric_analysis']
            if num_stats['optimization_opportunities']['integer'] > 0:
                recommendations.append(f"🔧 Optimize {num_stats['optimization_opportunities']['integer']} integer columns to smaller types")
            
            if num_stats['optimization_opportunities']['float'] > 0:
                recommendations.append(f"🔧 Convert {num_stats['optimization_opportunities']['float']} float64 columns to float32")
        
        # Categorical optimization recommendations
        if 'categorical_analysis' in self.analysis_results:
            cat_stats = self.analysis_results['categorical_analysis']
            if cat_stats['recommendations']['low_cardinality_count'] > 0:
                recommendations.append(f"🏷️  Convert {cat_stats['recommendations']['low_cardinality_count']} columns to category type")
            
            if cat_stats['recommendations']['high_cardinality_count'] > 0:
                recommendations.append(f"⚠️  Review {cat_stats['recommendations']['high_cardinality_count']} high-cardinality columns for encoding or removal")
        
        # Missing value recommendations
        if 'missing_values' in self.analysis_results:
            missing_stats = self.analysis_results['missing_values']
            if missing_stats['categories']['very_high_missing'] > 0:
                recommendations.append(f"❓ Consider dropping {missing_stats['categories']['very_high_missing']} columns with >75% missing values")
            
            if missing_stats['categories']['medium_missing'] > 0:
                recommendations.append(f"🔍 Review imputation strategies for {missing_stats['categories']['medium_missing']} columns with 5-25% missing values")
        
        # Data quality recommendations
        if 'data_quality' in self.analysis_results:
            quality_stats = self.analysis_results['data_quality']
            if quality_stats['potential_duplicates'] > 0:
                recommendations.append(f"🔍 Remove {quality_stats['potential_duplicates']:,} potential duplicate rows")
            
            if quality_stats['outlier_columns'] > 0:
                recommendations.append(f"📊 Review {quality_stats['outlier_columns']} columns with significant outliers")
        
        # Display recommendations
        if recommendations:
            print("📋 PRIORITY RECOMMENDATIONS:")
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")
            
            # Estimated benefits
            print(f"\n📈 ESTIMATED BENEFITS:")
            if 'basic_info' in self.analysis_results:
                current_memory = self.analysis_results['basic_info']['estimated_memory_mb']
                estimated_reduction = min(50, len(recommendations) * 8)  # Rough estimate
                new_memory = current_memory * (1 - estimated_reduction/100)
                print(f"  Memory usage: {current_memory:.1f}MB → {new_memory:.1f}MB ({estimated_reduction}% reduction)")
                
                # File size estimation
                current_file_mb = os.path.getsize(self.file_path) / (1024 * 1024)
                new_file_mb = current_file_mb * (1 - estimated_reduction/100)
                print(f"  File size: {current_file_mb:.1f}MB → {new_file_mb:.1f}MB")
        
        else:
            print("✅ Dataset appears to be well-optimized!")
            print("  No major optimization opportunities detected.")
        
        self.analysis_results['recommendations'] = recommendations
    
    def print_summary(self):
        """Print executive summary"""
        print(f"\n📊 EXECUTIVE SUMMARY")
        print("="*60)
        
        print(f"📁 Dataset: {os.path.basename(self.file_path)}")
        print(f"📏 Size: {self.total_rows:,} rows × {len(self.df.columns)} columns")
        
        file_size_mb = os.path.getsize(self.file_path) / (1024 * 1024)
        print(f"💾 File size: {file_size_mb:.1f} MB")
        
        if 'missing_values' in self.analysis_results:
            missing_pct = self.analysis_results['missing_values']['overall_missing_pct']
            print(f"❓ Missing data: {missing_pct:.1f}%")
        
        if 'data_quality' in self.analysis_results:
            quality_score = self.analysis_results['data_quality']['quality_score']
            print(f"✅ Quality score: {quality_score:.1f}/100")
        
        # Quick stats
        if 'numeric_analysis' in self.analysis_results:
            num_cols = self.analysis_results['numeric_analysis']['numeric_columns_count']
            print(f"🔢 Numeric columns: {num_cols}")
        
        if 'categorical_analysis' in self.analysis_results:
            cat_cols = self.analysis_results['categorical_analysis']['categorical_columns_count']
            print(f"🏷️  Categorical columns: {cat_cols}")
        
        print(f"⏰ Analysis completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def run_full_analysis(self) -> bool:
        """Run complete analysis"""
        if not self.load_dataset():
            return False
        
        print(f"\n🚀 STARTING COMPREHENSIVE ANALYSIS")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Run all analysis components
        self.analyze_basic_info()
        self.analyze_missing_values()
        self.analyze_numeric_columns()
        self.analyze_categorical_columns()
        self.analyze_data_quality()
        self.generate_recommendations()
        self.print_summary()
        
        return True

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='Comprehensive Dataset Analysis Tool')
    parser.add_argument('file_path', help='Path to the CSV file to analyze')
    parser.add_argument('--sample-size', type=int, default=3000000, 
                       help='Sample size for analysis (default: 3000000)')
    
    # If no arguments provided, use interactive mode
    if len(sys.argv) == 1:
        print("🔍 DATASET ANALYZER - Interactive Mode")
        print("="*50)
        
        # Show available datasets in current directory
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        if csv_files:
            print("📁 Available CSV files in current directory:")
            for i, file in enumerate(csv_files, 1):
                size_mb = os.path.getsize(file) / (1024 * 1024)
                print(f"  {i}. {file} ({size_mb:.1f} MB)")
            
            try:
                choice = input(f"\nEnter file number (1-{len(csv_files)}) or file path: ").strip()
                if choice.isdigit() and 1 <= int(choice) <= len(csv_files):
                    file_path = csv_files[int(choice) - 1]
                else:
                    file_path = choice
            except KeyboardInterrupt:
                print("\n👋 Analysis cancelled")
                return
        else:
            file_path = input("Enter path to CSV file: ").strip()
        
        sample_size = 3000000
        try:
            sample_input = input(f"Sample size for analysis (default {sample_size}): ").strip()
            if sample_input:
                sample_size = int(sample_input)
        except ValueError:
            print("Using default sample size")
    
    else:
        args = parser.parse_args()
        file_path = args.file_path
        sample_size = args.sample_size
    
    # Run analysis
    analyzer = DatasetAnalyzer(file_path, sample_size)
    success = analyzer.run_full_analysis()
    
    if success:
        print(f"\n✅ Analysis completed successfully!")
    else:
        print(f"\n❌ Analysis failed!")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())