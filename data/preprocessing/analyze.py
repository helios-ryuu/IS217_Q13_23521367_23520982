#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dataset Analysis Tool - Complete Analysis
Phân tích toàn bộ dataset và tạo báo cáo chi tiết
"""

import pandas as pd
import numpy as np
from datetime import datetime
import argparse
import os
import sys
from typing import Dict, List, Optional

# ==========================================
# Type Conversion Functions
# ==========================================

def get_sql_server_type(pandas_dtype: str, column_name: str, max_length: int = 0) -> str:
    """Chuyển đổi kiểu pandas sang SQL Server"""
    
    # Tọa độ
    if any(coord in column_name.upper() for coord in ['LATITUDE', 'LONGITUDE', 'LAT', 'LNG']):
        return 'DECIMAL(9,6)'
    
    # Cột đường phố - chỉ cột có tên chính xác là "Street" (không phân biệt chữ hoa thường)
    if column_name.upper() == 'STREET':
        if max_length > 100:
            return 'NVARCHAR(4000)'
        else:
            return 'NVARCHAR(100)'
    
    # Các cột Boolean (BIT) - bao gồm cả IS_WEEKEND và environment columns
    boolean_columns = [
        'IS_WEEKEND', 'AMENITY', 'BUMP', 'CROSSING', 'GIVE_WAY', 'JUNCTION', 
        'NO_EXIT', 'RAILWAY', 'ROUNDABOUT', 'STATION', 'STOP', 
        'TRAFFIC_CALMING', 'TRAFFIC_SIGNAL', 'TURNING_LOOP'
    ]
    if column_name.upper() in boolean_columns:
        return 'BIT'
    
    # Kiểu số
    if pandas_dtype in ['int8']:
        return 'TINYINT'
    elif pandas_dtype in ['int16']:
        return 'SMALLINT'
    elif pandas_dtype in ['int32', 'int64']:
        return 'INT'
    elif pandas_dtype in ['bool']:
        return 'BIT'
    elif pandas_dtype in ['float32', 'float64']:
        return 'DECIMAL(8,4)'
    
    # Kiểu chuỗi - nvarchar(100) cho tất cả trừ STREET
    elif pandas_dtype in ['object', 'string']:
        return 'NVARCHAR(100)'
    elif pandas_dtype == 'category':
        return 'NVARCHAR(100)'
    
    # Kiểu thời gian
    elif 'datetime' in pandas_dtype:
        return 'DATETIME2'
    elif 'timedelta' in pandas_dtype:
        return 'TIME'
    
    # Mặc định
    else:
        return 'NVARCHAR(100)'

def analyze_column_characteristics(df: pd.DataFrame, column: str) -> Dict:
    """Phân tích đặc điểm cột"""
    col_data = df[column]
    
    analysis = {
        'dtype': str(col_data.dtype),
        'non_null_count': col_data.notna().sum(),
        'null_count': col_data.isna().sum(),
        'null_percentage': (col_data.isna().sum() / len(col_data)) * 100,
        'unique_count': col_data.nunique(),
        'memory_usage_mb': col_data.memory_usage(deep=True) / (1024 * 1024),
        'max_length': 0,
        'min_value': None,
        'max_value': None
    }
    
    # Đặc điểm chuỗi
    if col_data.dtype == 'object':
        string_lengths = col_data.dropna().astype(str).str.len()
        if len(string_lengths) > 0:
            analysis['max_length'] = string_lengths.max()
            analysis['avg_length'] = string_lengths.mean()
            analysis['min_length'] = string_lengths.min()
    
    # Đặc điểm số
    elif pd.api.types.is_numeric_dtype(col_data):
        if col_data.notna().sum() > 0:
            analysis['min_value'] = col_data.min()
            analysis['max_value'] = col_data.max()
            analysis['mean_value'] = col_data.mean()
            analysis['std_value'] = col_data.std()
    
    return analysis

def create_sql_create_table(df: pd.DataFrame, table_name: str = 'US_Accidents') -> str:
    """Tạo câu lệnh CREATE TABLE SQL Server"""
    
    sql_lines = [f"CREATE TABLE {table_name} ("]
    
    for column in df.columns:
        char = analyze_column_characteristics(df, column)
        sql_type = get_sql_server_type(char['dtype'], column, char['max_length'])
        
        # Xác định NULL/NOT NULL
        null_constraint = "NOT NULL" if char['null_percentage'] == 0 else "NULL"
        
        sql_lines.append(f"    [{column}] {sql_type} {null_constraint},")
    
    # Loại bỏ dấu phẩy cuối và đóng ngoặc
    sql_lines[-1] = sql_lines[-1].rstrip(',')
    sql_lines.append(");")
    
    return '\n'.join(sql_lines)

class DatasetAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None
        self.analysis_results = {}
        
        # Tạo tên file output dựa trên tên dataset
        dataset_name = os.path.splitext(os.path.basename(file_path))[0]
        self.output_file = os.path.join(os.path.dirname(file_path), f"{dataset_name}-analyze_report.txt")
        self.output_content = []

    def print_and_save(self, text=""):
        """Print text và lưu vào output content"""
        print(text)
        self.output_content.append(text)
    
    def save_report(self):
        """Lưu báo cáo phân tích ra file"""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(self.output_content))
            self.print_and_save(f"\n📄 Báo cáo phân tích đã được lưu tại: {self.output_file}")
            return True
        except Exception as e:
            print(f"❌ Lỗi khi lưu báo cáo: {str(e)}")
            return False

    def load_dataset(self):
        """Load the complete dataset"""
        self.print_and_save(f"🔄 Loading dataset from: {self.file_path}")
        
        try:
            if self.file_path.endswith('.csv'):
                self.df = pd.read_csv(self.file_path)
            elif self.file_path.endswith(('.xlsx', '.xls')):
                self.df = pd.read_excel(self.file_path)
            else:
                raise ValueError("Unsupported file format. Only CSV and Excel files are supported.")
            
            self.print_and_save(f"✅ Successfully loaded {len(self.df):,} rows and {len(self.df.columns)} columns")
            return True
            
        except Exception as e:
            self.print_and_save(f"❌ Error loading dataset: {str(e)}")
            return False

    def analyze_basic_info(self):
        """Analyze basic dataset information"""
        self.print_and_save(f"\n📊 BASIC DATASET INFORMATION")
        self.print_and_save("="*60)
        
        total_rows = len(self.df)
        total_columns = len(self.df.columns)
        memory_usage = self.df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        
        self.print_and_save(f"📈 Dataset Shape: {total_rows:,} rows × {total_columns} columns")
        self.print_and_save(f"💾 Memory Usage: {memory_usage:.2f} MB")
        self.print_and_save(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Column types
        dtype_counts = self.df.dtypes.value_counts()
        self.print_and_save(f"\n🏷️ DATA TYPES:")
        for dtype, count in dtype_counts.items():
            self.print_and_save(f"  {str(dtype):15s}: {count:3d} columns")
        
        # Missing values overview
        missing_data = self.df.isnull().sum()
        missing_columns = missing_data[missing_data > 0]
        
        self.print_and_save(f"\n❓ MISSING VALUES OVERVIEW:")
        if len(missing_columns) > 0:
            total_missing = missing_data.sum()
            missing_percentage = (total_missing / (total_rows * total_columns)) * 100
            self.print_and_save(f"  Total missing values: {total_missing:,} ({missing_percentage:.2f}%)")
            self.print_and_save(f"  Columns with missing data: {len(missing_columns)}")
        else:
            self.print_and_save("  ✅ No missing values found")
        
        self.analysis_results['basic_info'] = {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'total_memory_mb': memory_usage,
            'dtype_counts': dtype_counts.to_dict(),
            'missing_columns_count': len(missing_columns)
        }

    def analyze_missing_values(self):
        """Detailed missing values analysis"""
        self.print_and_save(f"\n❓ MISSING VALUES DETAILED ANALYSIS")
        self.print_and_save("="*60)
        
        missing_data = self.df.isnull().sum()
        missing_percentage = (missing_data / len(self.df)) * 100
        
        missing_df = pd.DataFrame({
            'Column': missing_data.index,
            'Missing_Count': missing_data.values,
            'Missing_Percentage': missing_percentage.values
        }).sort_values('Missing_Percentage', ascending=False)
        
        # Filter columns with missing values
        missing_df = missing_df[missing_df['Missing_Count'] > 0]
        
        if len(missing_df) > 0:
            self.print_and_save(f"📋 TOP COLUMNS WITH MISSING VALUES:")
            self.print_and_save("  {:30s} {:>12s} {:>15s}".format("Column", "Missing", "Percentage"))
            self.print_and_save("  " + "-" * 60)
            
            for _, row in missing_df.head(20).iterrows():
                self.print_and_save(f"  {row['Column']:30s} {row['Missing_Count']:>12,} {row['Missing_Percentage']:>14.2f}%")
        
        # Categorize columns by missing percentage
        categories = {
            'low_missing': len(missing_df[(missing_df['Missing_Percentage'] > 0) & (missing_df['Missing_Percentage'] <= 25)]),
            'medium_missing': len(missing_df[(missing_df['Missing_Percentage'] > 25) & (missing_df['Missing_Percentage'] <= 50)]),
            'high_missing': len(missing_df[(missing_df['Missing_Percentage'] > 50) & (missing_df['Missing_Percentage'] <= 75)]),
            'very_high_missing': len(missing_df[missing_df['Missing_Percentage'] > 75])
        }
        
        overall_missing_pct = (missing_data.sum() / (len(self.df) * len(self.df.columns))) * 100
        
        self.print_and_save(f"\n📊 MISSING DATA CATEGORIES:")
        self.print_and_save(f"  Low missing (≤25%):      {categories['low_missing']} columns")
        self.print_and_save(f"  Medium missing (25-50%):  {categories['medium_missing']} columns")
        self.print_and_save(f"  High missing (50-75%):    {categories['high_missing']} columns")
        self.print_and_save(f"  Very high missing (>75%): {categories['very_high_missing']} columns")
        
        self.analysis_results['missing_values'] = {
            'overall_missing_pct': overall_missing_pct,
            'categories': categories,
            'detailed_missing': missing_df.to_dict('records') if len(missing_df) > 0 else []
        }

    def analyze_numeric_columns(self):
        """Analyze numeric columns"""
        self.print_and_save(f"\n🔢 NUMERIC COLUMNS ANALYSIS")
        self.print_and_save("="*60)
        
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) == 0:
            self.print_and_save("  ℹ️  No numeric columns found")
            return
        
        self.print_and_save(f"📊 Found {len(numeric_cols)} numeric columns")
        
        self.print_and_save(f"\n📈 NUMERIC STATISTICS:")
        self.print_and_save("  {:25s} {:>12s} {:>12s} {:>12s} {:>12s}".format(
            "Column", "Mean", "Std", "Min", "Max"))
        self.print_and_save("  " + "-" * 75)
        
        numeric_stats = []
        
        for col in numeric_cols:
            if self.df[col].notna().sum() > 0:
                stats = {
                    'column': col,
                    'mean': self.df[col].mean(),
                    'std': self.df[col].std(),
                    'min': self.df[col].min(),
                    'max': self.df[col].max(),
                    'median': self.df[col].median(),
                    'q25': self.df[col].quantile(0.25),
                    'q75': self.df[col].quantile(0.75)
                }
                numeric_stats.append(stats)
                
                self.print_and_save(f"  {col:25s} {stats['mean']:>12.2f} {stats['std']:>12.2f} {stats['min']:>12.2f} {stats['max']:>12.2f}")
        
        self.analysis_results['numeric_analysis'] = {
            'numeric_columns_count': len(numeric_cols),
            'statistics': numeric_stats
        }

    def analyze_categorical_columns(self):
        """Analyze categorical/text columns"""
        self.print_and_save(f"\n🏷️ CATEGORICAL COLUMNS ANALYSIS")
        self.print_and_save("="*60)
        
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        
        if len(categorical_cols) == 0:
            self.print_and_save("  ℹ️  No categorical columns found")
            return
        
        self.print_and_save(f"📊 Found {len(categorical_cols)} categorical columns")
        
        self.print_and_save(f"\n📝 CATEGORICAL STATISTICS:")
        self.print_and_save("  {:25s} {:>12s} {:>12s} {:>12s}".format(
            "Column", "Unique", "Cardinality", "Most Frequent"))
        self.print_and_save("  " + "-" * 65)
        
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
            
            self.print_and_save(f"  {col:25s} {unique_count:>12,} {cardinality_ratio:>11.3f} {most_frequent_str:>12s}")
        
        self.analysis_results['categorical_analysis'] = {
            'categorical_columns_count': len(categorical_cols),
            'statistics': categorical_stats
        }

    def analyze_data_quality(self):
        """Analyze overall data quality"""
        self.print_and_save(f"\n🔍 DATA QUALITY ASSESSMENT")
        self.print_and_save("="*60)
        
        # Calculate quality score
        quality_score = self._calculate_quality_score()
        
        # Quality indicators
        self.print_and_save(f"📊 OVERALL QUALITY SCORE: {quality_score:.1f}/100")
        
        if quality_score >= 90:
            quality_level = "Excellent"
            quality_emoji = "🌟"
        elif quality_score >= 75:
            quality_level = "Good"
            quality_emoji = "✅"
        elif quality_score >= 60:
            quality_level = "Fair"
            quality_emoji = "⚠️"
        else:
            quality_level = "Poor"
            quality_emoji = "❌"
        
        self.print_and_save(f"{quality_emoji} Quality Level: {quality_level}")
        
        self.analysis_results['data_quality'] = {
            'quality_score': quality_score,
            'quality_level': quality_level
        }

    def _calculate_quality_score(self):
        """Calculate data quality score"""
        score = 100
        
        # Deduct points for missing values
        if 'missing_values' in self.analysis_results:
            missing_pct = self.analysis_results['missing_values']['overall_missing_pct']
            score -= min(missing_pct, 30)  # Max 30 point deduction for missing values
        
        # Deduct points for high cardinality columns
        if 'categorical_analysis' in self.analysis_results:
            # Count high cardinality columns (cardinality ratio > 0.8)
            high_card_count = 0
            if 'statistics' in self.analysis_results['categorical_analysis']:
                for stats in self.analysis_results['categorical_analysis']['statistics']:
                    if stats.get('cardinality_ratio', 0) > 0.8:
                        high_card_count += 1
            score -= high_card_count * 5  # 5 points per high cardinality column
        
        return max(score, 0)

    def _print_analysis_summary(self):
        """Print analysis summary"""
        self.print_and_save(f"\n📋 ANALYSIS SUMMARY")
        self.print_and_save("="*60)
        
        # Basic stats
        if 'basic_info' in self.analysis_results:
            basic = self.analysis_results['basic_info']
            self.print_and_save(f"📊 Dataset: {basic['total_rows']:,} rows × {basic['total_columns']} columns")
            self.print_and_save(f"💾 Memory: {basic['total_memory_mb']:.1f} MB")
        
        # Missing data summary
        if 'missing_values' in self.analysis_results:
            missing_stats = self.analysis_results['missing_values']
            self.print_and_save(f"❓ Missing data: {missing_stats['overall_missing_pct']:.2f}%")
        
        # Data quality
        if 'data_quality' in self.analysis_results:
            quality = self.analysis_results['data_quality']
            self.print_and_save(f"🏆 Quality score: {quality['quality_score']:.1f}/100 ({quality['quality_level']})")
        
        # Type conversion
        if 'type_conversion' in self.analysis_results:
            type_conv = self.analysis_results['type_conversion']
            self.print_and_save(f"🔄 SQL Server mapping: {len(type_conv['sql_type_counts'])} different types")

    def analyze_type_conversion(self):
        """Analyze type conversion for SQL Server"""
        self.print_and_save(f"\n🔄 SQL SERVER TYPE CONVERSION ANALYSIS")
        self.print_and_save("="*60)
        
        # Tạo bảng chuyển đổi kiểu dữ liệu
        conversion_data = []
        
        for column in self.df.columns:
            char = analyze_column_characteristics(self.df, column)
            sql_type = get_sql_server_type(char['dtype'], column, char['max_length'])
            
            conversion_data.append({
                'column': column,
                'python_type': char['dtype'],
                'sql_type': sql_type,
                'non_null_count': char['non_null_count'],
                'null_count': char['null_count'],
                'null_percentage': char['null_percentage'],
                'unique_count': char['unique_count'],
                'memory_usage_mb': char['memory_usage_mb'],
                'max_length': char['max_length'],
                'min_value': char['min_value'],
                'max_value': char['max_value']
            })
        
        # Hiển thị bảng chuyển đổi
        self.print_and_save(f"\n📋 TYPE CONVERSION TABLE:")
        self.print_and_save("  {:25s} {:15s} {:15s} {:>10s} {:>8s}".format(
            "Column", "Python Type", "SQL Type", "Null %", "Memory MB"))
        self.print_and_save("  " + "-" * 80)
        
        for data in conversion_data:
            self.print_and_save(f"  {data['column']:25s} {data['python_type']:15s} {data['sql_type']:15s} {data['null_percentage']:>9.1f}% {data['memory_usage_mb']:>7.2f}")
        
        # Thống kê SQL Server types
        sql_type_counts = {}
        for data in conversion_data:
            sql_type = data['sql_type']
            sql_type_counts[sql_type] = sql_type_counts.get(sql_type, 0) + 1
        
        self.print_and_save(f"\n📊 SQL SERVER TYPE DISTRIBUTION:")
        for sql_type, count in sorted(sql_type_counts.items()):
            self.print_and_save(f"  {sql_type:20s}: {count:3d} columns")
        
        # Tạo CREATE TABLE script
        self.print_and_save(f"\n💾 SQL SERVER CREATE TABLE SCRIPT:")
        self.print_and_save("-" * 60)
        create_table_sql = create_sql_create_table(self.df)
        
        # Hiển thị script (giới hạn dòng để không quá dài)
        sql_lines = create_table_sql.split('\n')
        for i, line in enumerate(sql_lines):
            if i < 20:  # Hiển thị 20 dòng đầu
                self.print_and_save(line)
            elif i == 20 and len(sql_lines) > 25:
                self.print_and_save(f"  ... ({len(sql_lines) - 20} more lines) ...")
                break
        
        if len(sql_lines) <= 25:  # Hiển thị hết nếu không quá dài
            for line in sql_lines[20:]:
                self.print_and_save(line)
        
        # Khuyến nghị tối ưu hóa
        self.print_and_save(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")
        
        # Cột có nhiều null
        high_null_cols = [data for data in conversion_data if data['null_percentage'] > 50]
        if high_null_cols:
            self.print_and_save(f"  🔍 {len(high_null_cols)} columns have >50% null values")
        
        # Cột chuỗi dài
        long_string_cols = [data for data in conversion_data if data['sql_type'] == 'NVARCHAR(4000)']
        if long_string_cols:
            self.print_and_save(f"  📏 {len(long_string_cols)} columns with long strings (NVARCHAR(4000))")
        
        # Tổng bộ nhớ
        total_memory = sum(data['memory_usage_mb'] for data in conversion_data)
        self.print_and_save(f"  💾 Total estimated memory: {total_memory:.1f} MB")
        
        # Lưu thông tin vào analysis_results
        self.analysis_results['type_conversion'] = {
            'conversion_table': conversion_data,
            'sql_type_counts': sql_type_counts,
            'create_table_script': create_table_sql,
            'high_null_columns': len(high_null_cols),
            'long_string_columns': len(long_string_cols),
            'total_memory_mb': total_memory
        }

    def run_full_analysis(self):
        """Run complete dataset analysis"""
        self.print_and_save("🚀 STARTING COMPREHENSIVE DATASET ANALYSIS")
        self.print_and_save("="*80)
        self.print_and_save(f"📁 File: {self.file_path}")
        self.print_and_save(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Load dataset
            if not self.load_dataset():
                return False
            
            # Run all analyses
            self.analyze_basic_info()
            self.analyze_missing_values()
            self.analyze_numeric_columns()
            self.analyze_categorical_columns()
            self.analyze_type_conversion()
            self.analyze_data_quality()
            
            # Final summary
            self._print_analysis_summary()
            
            # Save report
            self.save_report()
            
            self.print_and_save(f"\n✅ Analysis completed successfully!")
            self.print_and_save(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return True
            
        except Exception as e:
            self.print_and_save(f"\n❌ Analysis failed: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Analyze complete dataset and generate detailed report')
    parser.add_argument('dataset_path', help='Path to the dataset file (CSV or Excel)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.dataset_path):
        print(f"❌ Error: File not found: {args.dataset_path}")
        return 1
    
    print("🔍 DATASET ANALYZER - COMPLETE ANALYSIS")
    print("="*50)
    
    analyzer = DatasetAnalyzer(args.dataset_path)
    success = analyzer.run_full_analysis()
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())