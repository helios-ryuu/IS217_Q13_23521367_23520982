"""
Báo cáo chuyển đổi kiểu dữ liệu Python → SQL Server
Tác giả: Data type conversion reporter
Ngày: 2024
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional

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

def create_conversion_table(df: pd.DataFrame) -> pd.DataFrame:
    """Tạo bảng chuyển đổi kiểu dữ liệu"""
    
    conversion_data = []
    
    for column in df.columns:
        char = analyze_column_characteristics(df, column)
        sql_type = get_sql_server_type(char['dtype'], column, char['max_length'])
        
        conversion_data.append({
            'Tên Cột': column,
            'Kiểu Python': char['dtype'],
            'Kiểu SQL Server': sql_type,
            'Không Null': f"{char['non_null_count']:,}",
            'Null': f"{char['null_count']:,}",
            'Null %': f"{char['null_percentage']:.1f}%",
            'Giá Trị Duy Nhất': f"{char['unique_count']:,}",
            'Bộ Nhớ (MB)': f"{char['memory_usage_mb']:.2f}",
            'Độ Dài Tối Đa': char['max_length'] if char['max_length'] > 0 else 'N/A',
            'Giá Trị Min': char['min_value'] if char['min_value'] is not None else 'N/A',
            'Giá Trị Max': char['max_value'] if char['max_value'] is not None else 'N/A'
        })
    
    return pd.DataFrame(conversion_data)

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

def generate_type_conversion_report(csv_file: str, df_sample: Optional[pd.DataFrame] = None) -> None:
    """Tạo báo cáo chuyển đổi kiểu dữ liệu chi tiết"""
    
    try:
        # Đọc mẫu dữ liệu nếu không có
        if df_sample is None:
            print("📖 Đọc mẫu dữ liệu...")
            df_sample = pd.read_csv(csv_file, nrows=10000, low_memory=False)
        
        print("="*80)
        print("📋 BÁO CÁO CHUYỂN ĐỔI KIỂU DỮ LIỆU PYTHON → SQL SERVER")
        print("="*80)
        
        # Thống kê tổng quan
        print(f"📊 TỔNG QUAN:")
        print(f"  Tổng số cột: {len(df_sample.columns)}")
        print(f"  Dòng mẫu: {len(df_sample):,}")
        
        # Phân loại kiểu dữ liệu
        dtype_summary = df_sample.dtypes.value_counts()
        print(f"\n📈 PHÂN LOẠI KIỂU DỮ LIỆU PYTHON:")
        for dtype, count in dtype_summary.items():
            print(f"  {dtype}: {count} cột")
        
        # Bảng chuyển đổi chi tiết
        print(f"\n📋 BẢNG CHUYỂN ĐỔI CHI TIẾT:")
        conversion_table = create_conversion_table(df_sample)
        
        # In bảng với format đẹp
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 20)
        
        print(conversion_table.to_string(index=False))
        
        # Thống kê SQL Server
        sql_type_counts = conversion_table['Kiểu SQL Server'].value_counts()
        print(f"\n📊 PHÂN LOẠI KIỂU SQL SERVER:")
        for sql_type, count in sql_type_counts.items():
            print(f"  {sql_type}: {count} cột")
        
        # Tạo CREATE TABLE script
        print(f"\n💾 SCRIPT TẠO BẢNG SQL SERVER:")
        print("-" * 60)
        create_table_sql = create_sql_create_table(df_sample)
        print(create_table_sql)
        
        # Lưu báo cáo vào file
        report_file = csv_file.replace('.csv', '_type_conversion_report.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("BÁO CÁO CHUYỂN ĐỔI KIỂU DỮ LIỆU PYTHON → SQL SERVER\n")
            f.write("="*80 + "\n\n")
            f.write(f"File: {csv_file}\n")
            f.write(f"Ngày tạo: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("BẢNG CHUYỂN ĐỔI:\n")
            f.write(conversion_table.to_string(index=False))
            f.write("\n\nSCRIPT TẠO BẢNG SQL SERVER:\n")
            f.write("-" * 60 + "\n")
            f.write(create_table_sql)
        
        print(f"\n💾 ĐÃ LUU BÁO CÁO:")
        print(f"  Báo cáo text: {report_file}")
        
        # Khuyến nghị tối ưu hóa
        print(f"\n💡 KHUYẾN NGHỊ TỐI ƯU HÓA:")
        
        # Cột có nhiều null
        high_null_cols = conversion_table[conversion_table['Null %'].str.replace('%', '').astype(float) > 50]
        if len(high_null_cols) > 0:
            print(f"  🔍 {len(high_null_cols)} cột có >50% giá trị null - cân nhắc xóa hoặc tối ưu")
        
        # Cột chuỗi dài
        long_string_cols = conversion_table[
            (conversion_table['Kiểu SQL Server'] == 'NVARCHAR(4000)') |
            (conversion_table['Độ Dài Tối Đa'] != 'N/A')
        ]
        if len(long_string_cols) > 0:
            print(f"  📏 {len(long_string_cols)} cột chuỗi dài - kiểm tra độ dài thực tế")
        
        # Bộ nhớ lớn
        total_memory = conversion_table['Bộ Nhớ (MB)'].str.replace(' MB', '').astype(float).sum()
        print(f"  💾 Tổng bộ nhớ ước tính: {total_memory:.1f} MB")
        
        print(f"\n✅ Hoàn thành báo cáo chuyển đổi kiểu dữ liệu!")
        
    except Exception as e:
        print(f"❌ Lỗi tạo báo cáo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Test với file mẫu
    import sys
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        generate_type_conversion_report(csv_file)
    else:
        print("Sử dụng: python type_conversion.py <file.csv>")