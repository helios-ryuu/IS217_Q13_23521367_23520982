import pandas as pd
import numpy as np
import warnings
import os
import gc
import re
from datetime import datetime
from typing import List, Optional

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

# =========================================================
# Configuration
# =========================================================
INPUT_FILE = "../US_Accidents_March23.csv"
OUTPUT_FILE = "../US_Accidents_March23-cleaned.csv"
CHUNK_SIZE = 2500000  # Process 2M rows at a time to prevent memory overflow
DATE_CUTOFF = "2018-01-01"  # Keep records from this date onwards

# Columns to delete
COLUMNS_TO_DELETE = ['ID', 'Description', 'End_Lat', 'End_Lng', 'End_Time']
# Note: Start_Time will be deleted after time feature extraction

def get_file_info(file_path):
    """Get file size and basic info"""
    if os.path.exists(file_path):
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        return f"{size_mb:.1f} MB"
    return "File not found"

def print_dataset_info_before():
    """Print comprehensive dataset information before processing"""
    print("📊 ORIGINAL DATASET ANALYSIS")
    print("="*60)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return None
    
    # Get file size
    file_size_mb = os.path.getsize(INPUT_FILE) / (1024 * 1024)
    print(f"📁 File: {INPUT_FILE}")
    print(f"💾 Size: {file_size_mb:.1f} MB")
    
    # Read sample for analysis
    try:
        print("🔍 Loading sample data for analysis...")
        df_sample = pd.read_csv(INPUT_FILE, nrows=50000, low_memory=False)
        
        # Get total row count
        print("🔍 Counting total rows...")
        total_rows = sum(1 for _ in open(INPUT_FILE, encoding='utf-8')) - 1
        
        print(f"📏 Total rows: {total_rows:,}")
        print(f"📐 Total columns: {len(df_sample.columns)}")
        print(f"🔍 Sample analyzed: {len(df_sample):,} rows")
        
        # Column info
        print(f"\n📋 COLUMN INFORMATION:")
        print(f"Columns: {list(df_sample.columns)}")
        
        # Data types
        print(f"\n📊 DATA TYPES:")
        dtype_counts = df_sample.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {dtype}: {count} columns")
        
        # Missing values analysis
        print(f"\n🔍 MISSING VALUES (Top 10):")
        missing = df_sample.isnull().sum().sort_values(ascending=False)
        missing_pct = (missing / len(df_sample) * 100).round(2)
        
        for col in missing.head(10).index:
            if missing[col] > 0:
                print(f"  {col}: {missing[col]:,} ({missing_pct[col]}%)")
        
        if missing.sum() == 0:
            print("  ✅ No missing values found!")
        
        # Numeric columns summary
        numeric_cols = df_sample.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            print(f"\n📈 NUMERIC COLUMNS SUMMARY:")
            print(f"  Found {len(numeric_cols)} numeric columns")
            for col in numeric_cols[:5]:  # Show first 5
                print(f"  {col}: min={df_sample[col].min():.2f}, max={df_sample[col].max():.2f}, mean={df_sample[col].mean():.2f}")
        
        # Categorical columns
        categorical_cols = df_sample.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            print(f"\n📝 CATEGORICAL COLUMNS (Top 5):")
            for col in categorical_cols[:5]:
                unique_count = df_sample[col].nunique()
                print(f"  {col}: {unique_count} unique values")
                if unique_count <= 10:
                    print(f"    Values: {df_sample[col].value_counts().head(3).index.tolist()}")
        
        # Memory usage
        memory_mb = df_sample.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"\n💾 MEMORY USAGE:")
        print(f"  Sample memory: {memory_mb:.1f} MB")
        estimated_full_memory = memory_mb * (total_rows / len(df_sample))
        print(f"  Estimated full dataset: {estimated_full_memory:.1f} MB")
        
        # DataFrame info
        print(f"\n📋 DATAFRAME INFO (ORIGINAL):")
        df_sample.info(memory_usage='deep', show_counts=True)
        
        return df_sample
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None

def delete_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Delete specified columns from dataframe"""
    columns_to_drop = [col for col in COLUMNS_TO_DELETE if col in df.columns]
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
    return df

def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create time-based features from Start_Time"""
    if 'Start_Time' not in df.columns:
        return df
    
    # Convert to datetime
    df['Start_Time'] = pd.to_datetime(df['Start_Time'], errors='coerce')
    
    # Create time features
    df['FULL_DATE'] = df['Start_Time'].dt.strftime('%d %B %Y')
    df['YEAR'] = df['Start_Time'].dt.year
    df['QUARTER'] = df['Start_Time'].dt.quarter
    df['MONTH'] = df['Start_Time'].dt.month
    df['DAY'] = df['Start_Time'].dt.day
    df['HOUR'] = df['Start_Time'].dt.hour
    df['MINUTE'] = df['Start_Time'].dt.minute
    df['SECOND'] = df['Start_Time'].dt.second
    df['DAY_OF_WEEK'] = df['Start_Time'].dt.dayofweek + 1  # 1=Monday, 7=Sunday
    df['IS_WEEKEND'] = df['Start_Time'].dt.dayofweek.isin([5, 6]).astype(int)  # 1 for weekend, 0 for weekday
    
    # Delete Start_Time column after creating all time features
    df = df.drop(columns=['Start_Time'])
    
    return df

def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names: uppercase and remove parentheses"""
    # Create mapping of old to new column names
    column_mapping = {}
    for col in df.columns:
        # Convert to uppercase and remove parentheses and content within them
        new_col = col.upper()
        # Remove parentheses and everything inside them
        new_col = re.sub(r'\([^)]*\)', '', new_col)
        # Clean up any extra spaces or underscores
        new_col = re.sub(r'\s+', '_', new_col.strip())
        new_col = re.sub(r'_+', '_', new_col)
        new_col = new_col.strip('_')
        
        column_mapping[col] = new_col
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    return df

def filter_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """Filter records from 2018-01-01 onwards"""
    if 'Start_Time' not in df.columns:
        return df
    
    # Ensure Start_Time is datetime
    if df['Start_Time'].dtype != 'datetime64[ns]':
        df['Start_Time'] = pd.to_datetime(df['Start_Time'], errors='coerce')
    
    # Filter from 2018 onwards
    cutoff_date = pd.to_datetime(DATE_CUTOFF)
    mask = df['Start_Time'] >= cutoff_date
    return df[mask]

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert data types to SQL Server compatible formats"""
    # Convert float columns to NUMERIC(18,2) precision
    float_columns = df.select_dtypes(include=['float64', 'float32']).columns
    for col in float_columns:
        if col.lower() in ['latitude', 'longitude', 'start_lat', 'start_lng']:
            # DECIMAL(9,6) for coordinates
            df[col] = df[col].round(6)
        elif col.lower() in ['distance', 'distance_mi']:
            # DECIMAL(8,3) for distance
            df[col] = df[col].round(3)
        else:
            # NUMERIC(18,2) for other float columns
            df[col] = df[col].round(2)
    
    # Convert large integers to avoid overflow
    int_columns = df.select_dtypes(include=['int64']).columns
    for col in int_columns:
        if col not in ['YEAR', 'QUARTER', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'DAY_OF_WEEK', 'IS_WEEKEND']:
            # Check if values fit in int32 range
            if df[col].min() >= -2147483648 and df[col].max() <= 2147483647:
                df[col] = df[col].astype('int32')
    
    return df

def process_dataset_in_chunks():
    """Process the dataset in chunks to prevent memory overflow"""
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Input file not found: {INPUT_FILE}")
        return None
    
    # Remove output file if it exists
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    chunk_number = 0
    total_rows_processed = 0
    total_rows_kept = 0
    first_chunk = True
    
    # Statistics tracking
    columns_deleted_count = 0
    time_features_added = 0
    
    try:
        # Process in chunks
        for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, low_memory=False):
            chunk_number += 1
            initial_rows = len(chunk)
            initial_cols = len(chunk.columns)
            
            # Step 1: Delete unwanted columns
            chunk = delete_unwanted_columns(chunk)
            if chunk_number == 1:
                columns_deleted_count = initial_cols - len(chunk.columns)
            
            # Step 2: Filter by date (must be done before time feature creation)
            chunk = filter_by_date(chunk)
            
            if len(chunk) == 0:
                continue
            
            # Step 3: Create time features
            chunk = create_time_features(chunk)
            if chunk_number == 1:
                time_features_added = 10  # We know we add 10 time features
            
            # Step 4: Convert data types
            chunk = convert_data_types(chunk)
            
            # Step 5: Standardize column names
            chunk = standardize_column_names(chunk)
            
            # Step 6: Save chunk to output file
            if first_chunk:
                # Write header for first chunk
                chunk.to_csv(OUTPUT_FILE, index=False, mode='w')
                first_chunk = False
            else:
                # Append subsequent chunks without header
                chunk.to_csv(OUTPUT_FILE, index=False, mode='a', header=False)
            
            total_rows_processed += initial_rows
            total_rows_kept += len(chunk)
            
            # Force garbage collection
            del chunk
            gc.collect()
        
        return {
            'chunks_processed': chunk_number,
            'total_rows_processed': total_rows_processed,
            'total_rows_kept': total_rows_kept,
            'columns_deleted': columns_deleted_count,
            'time_features_added': time_features_added
        }
        
    except Exception as e:
        print(f"❌ Error during chunk processing: {e}")
        import traceback
        traceback.print_exc()
        return None

def analyze_processed_dataset():
    """Analyze the processed dataset"""
    print(f"\n📊 PROCESSED DATASET ANALYSIS")
    print("="*60)
    
    if not os.path.exists(OUTPUT_FILE):
        print(f"❌ Output file not found: {OUTPUT_FILE}")
        return None
    
    try:
        # Read sample of processed data
        df_sample = pd.read_csv(OUTPUT_FILE, nrows=50000, low_memory=False)
        
        # Get total row count
        total_rows = sum(1 for _ in open(OUTPUT_FILE, encoding='utf-8')) - 1
        
        print(f"📁 File: {OUTPUT_FILE}")
        print(f"💾 Size: {get_file_info(OUTPUT_FILE)}")
        print(f"📏 Total rows: {total_rows:,}")
        print(f"📐 Total columns: {len(df_sample.columns)}")
        print(f"🔍 Sample analyzed: {len(df_sample):,} rows")
        
        # Column info
        print(f"\n📋 PROCESSED COLUMN INFORMATION:")
        print(f"Columns: {list(df_sample.columns)}")
        
        # Data types
        print(f"\n📊 DATA TYPES:")
        dtype_counts = df_sample.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {dtype}: {count} columns")
        
        # Missing values analysis
        print(f"\n🔍 MISSING VALUES (Top 10):")
        missing = df_sample.isnull().sum().sort_values(ascending=False)
        missing_pct = (missing / len(df_sample) * 100).round(2)
        
        for col in missing.head(10).index:
            if missing[col] > 0:
                print(f"  {col}: {missing[col]:,} ({missing_pct[col]}%)")
        
        if missing.sum() == 0:
            print("  ✅ No missing values found!")
        
        # Time features analysis
        time_cols = ['FULL_DATE', 'YEAR', 'QUARTER', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'DAY_OF_WEEK', 'IS_WEEKEND']
        existing_time_cols = [col for col in time_cols if col in df_sample.columns]
        
        if existing_time_cols:
            print(f"\n⏰ TIME FEATURES ANALYSIS:")
            for col in existing_time_cols:
                if col == 'FULL_DATE':
                    print(f"  {col}: Sample values: {df_sample[col].head(3).tolist()}")
                elif col in ['YEAR', 'QUARTER', 'MONTH']:
                    print(f"  {col}: Range: {df_sample[col].min()} - {df_sample[col].max()}")
                elif col == 'IS_WEEKEND':
                    weekend_pct = (df_sample[col].sum() / len(df_sample) * 100).round(1)
                    print(f"  {col}: {weekend_pct}% weekend records")
        
        # Memory usage
        memory_mb = df_sample.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"\n💾 MEMORY USAGE:")
        print(f"  Sample memory: {memory_mb:.1f} MB")
        estimated_full_memory = memory_mb * (total_rows / len(df_sample))
        print(f"  Estimated full dataset: {estimated_full_memory:.1f} MB")
        
        # DataFrame info
        print(f"\n📋 DATAFRAME INFO (PROCESSED):")
        df_sample.info(memory_usage='deep', show_counts=True)
        
        return df_sample
        
    except Exception as e:
        print(f"❌ Error analyzing processed file: {e}")
        return None

def compare_datasets(original_stats, processed_stats, original_sample, processed_sample):
    """Compare original vs processed datasets"""
    print(f"\n🔄 DATASET COMPARISON")
    print("="*60)
    
    if not original_stats or not processed_stats:
        print("❌ Cannot compare - missing statistics")
        return
    
    # Row comparison
    print(f"📏 ROW COUNT:")
    print(f"  Original: {original_stats['total_rows_processed']:,} rows")
    print(f"  Processed: {processed_stats['total_rows_kept']:,} rows")
    row_diff = original_stats['total_rows_processed'] - processed_stats['total_rows_kept']
    reduction_pct = (row_diff / original_stats['total_rows_processed'] * 100)
    print(f"  Difference: {row_diff:,} rows removed ({reduction_pct:.1f}% reduction)")
    
    # Column comparison
    if original_sample is not None and processed_sample is not None:
        print(f"\n📐 COLUMN COUNT:")
        print(f"  Original: {len(original_sample.columns)} columns")
        print(f"  Processed: {len(processed_sample.columns)} columns")
        col_diff = len(processed_sample.columns) - len(original_sample.columns)
        print(f"  Net change: {col_diff:+d} columns")
        
        # Show specific changes
        print(f"  Columns deleted: {processed_stats['columns_deleted']}")
        print(f"  Time features added: {processed_stats['time_features_added']}")
        print(f"  Start_Time deleted after processing: 1")
    
    # File size comparison
    original_size = get_file_info(INPUT_FILE)
    processed_size = get_file_info(OUTPUT_FILE)
    print(f"\n💾 FILE SIZE:")
    print(f"  Original: {original_size}")
    print(f"  Processed: {processed_size}")
    
    # Processing performance
    print(f"\n🚀 PROCESSING PERFORMANCE:")
    print(f"  Chunks processed: {processed_stats['chunks_processed']}")
    print(f"  Chunk size: {CHUNK_SIZE:,} rows")
    print(f"  Date cutoff applied: {DATE_CUTOFF}")

def main():
    """Main preprocessing function"""
    print("🚀 SILENT DATASET PREPROCESSING")
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    try:
        # Step 1: Analyze original dataset
        original_sample = print_dataset_info_before()
        
        # Step 2: Process data in chunks (silent)
        print(f"\n🔄 PROCESSING DATASET...")
        print("Processing in silent mode...")
        processing_stats = process_dataset_in_chunks()
        
        if processing_stats is None:
            print("❌ Processing failed")
            return False
        
        # Step 3: Analyze processed dataset
        processed_sample = analyze_processed_dataset()
        
        # Step 4: Compare datasets
        original_stats = {
            'total_rows_processed': processing_stats['total_rows_processed']
        }
        compare_datasets(original_stats, processing_stats, original_sample, processed_sample)
        
        print(f"\n" + "="*60)
        print("✅ PREPROCESSING COMPLETED SUCCESSFULLY!")
        print(f"📁 Output saved to: {OUTPUT_FILE}")
        print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        print(f"\n❌ Error during preprocessing: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)