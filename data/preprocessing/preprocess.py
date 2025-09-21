"""
Hệ thống tiền xử lý dữ liệu cuối cùng - Tối ưu hóa cho SQL Server
Tác giả: Final preprocessing system
Ngày: 2024
"""

import pandas as pd
import numpy as np
import warnings
import os
import gc
import re
from datetime import datetime
from typing import List, Optional, Dict
from tqdm import tqdm

# Tắt cảnh báo
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

# ==========================================
# Các Pha Tiền Xử Lý Thuần Túy
# ==========================================

def phase_delete_columns(df: pd.DataFrame, columns_to_delete: List[str]) -> pd.DataFrame:
    """Pha 1: Xóa cột không cần thiết"""
    columns_to_drop = [col for col in columns_to_delete if col in df.columns]
    return df.drop(columns=columns_to_drop) if columns_to_drop else df

def phase_filter_date(df: pd.DataFrame, time_column: str = 'Start_Time', 
                     date_cutoff: str = "2018-01-01") -> pd.DataFrame:
    """Pha 2: Lọc dữ liệu theo ngày"""
    if time_column not in df.columns:
        return df
    
    if df[time_column].dtype != 'datetime64[ns]':
        df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
    
    cutoff_date = pd.to_datetime(date_cutoff)
    return df[df[time_column] >= cutoff_date]

def phase_create_time_features(df: pd.DataFrame, time_column: str = 'Start_Time') -> pd.DataFrame:
    """Pha 3: Tạo đặc trưng thời gian"""
    if time_column not in df.columns:
        return df
    
    df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
    
    # Tạo đặc trưng thời gian cơ bản
    df['YEAR'] = df[time_column].dt.year.astype('int16')
    df['QUARTER'] = df[time_column].dt.quarter.astype('int8')
    df['MONTH'] = df[time_column].dt.month.astype('int8')
    df['DAY'] = df[time_column].dt.day.astype('int8')
    df['HOUR'] = df[time_column].dt.hour.astype('int8')
    df['MINUTE'] = df[time_column].dt.minute.astype('int8')
    df['SECOND'] = df[time_column].dt.second.astype('int8')
    df['IS_WEEKEND'] = df[time_column].dt.dayofweek.isin([5, 6]).astype('bool')
    
    return df.drop(columns=[time_column])

def phase_sql_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 4: Chuyển đổi kiểu dữ liệu SQL Server"""
    
    # Tọa độ: decimal(9,6)
    coord_cols = ['Start_Lat', 'Start_Lng', 'LATITUDE', 'LONGITUDE']
    for col in coord_cols:
        if col in df.columns:
            df[col] = df[col].round(6).astype('float64')
    
    # Số thực khác: decimal(8,4) 
    float_cols = df.select_dtypes(include=['float64', 'float32']).columns
    for col in float_cols:
        if col not in coord_cols:
            df[col] = df[col].round(4).astype('float64')
    
    # Số nguyên: int/smallint/tinyint/bit
    int_cols = df.select_dtypes(include=['int64', 'int32']).columns
    for col in int_cols:
        if col in ['YEAR']:
            df[col] = df[col].astype('int16')  # smallint
        elif col in ['QUARTER', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND']:
            df[col] = df[col].astype('int8')   # tinyint
        else:
            df[col] = df[col].astype('int32')  # int
    
    # Boolean -> bit
    bool_cols = df.select_dtypes(include=['bool']).columns
    for col in bool_cols:
        df[col] = df[col].astype('int8')  # bit trong SQL
    
    # Chuỗi: nvarchar(4000) cho STREET, nvarchar(100) cho các cột khác
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype('string')  # Sử dụng string type để mapping SQL
    
    return df

def phase_standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 5: Chuẩn hóa tên cột"""
    column_mapping = {}
    for col in df.columns:
        new_col = col.upper()
        new_col = re.sub(r'\([^)]*\)', '', new_col)
        new_col = re.sub(r'\s+', '_', new_col.strip())
        new_col = re.sub(r'_+', '_', new_col).strip('_')
        column_mapping[col] = new_col
    
    df = df.rename(columns=column_mapping)
    
    # Đổi tên tọa độ cụ thể
    coordinate_mapping = {'START_LAT': 'LATITUDE', 'START_LNG': 'LONGITUDE'}
    for old_name, new_name in coordinate_mapping.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    return df



def phase_validate_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 6: Xác thực và làm sạch dữ liệu"""
    
    def find_column(patterns: List[str]) -> Optional[str]:
        for pattern in patterns:
            for col in df.columns:
                if pattern.upper() in col.upper():
                    return col
        return None
    
    # Xác thực mức độ nghiêm trọng
    severity_col = find_column(['SEVERITY'])
    if severity_col and severity_col in df.columns:
        df = df[df[severity_col].isin([1, 2, 3, 4])]
    
    # Xác thực và loại bỏ bản sao
    lat_col = find_column(['LATITUDE', 'START_LAT'])
    lng_col = find_column(['LONGITUDE', 'START_LNG'])
    
    if lat_col and lng_col and lat_col in df.columns and lng_col in df.columns:
        time_cols = [find_column([col]) for col in ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE']]
        time_cols = [col for col in time_cols if col and col in df.columns]
        
        if len(time_cols) >= 4:
            # Làm tròn tọa độ để phát hiện bản sao
            df_temp = df.copy()
            df_temp[f'{lat_col}_rounded'] = df_temp[lat_col].round(4)
            df_temp[f'{lng_col}_rounded'] = df_temp[lng_col].round(4)
            
            duplicate_cols = [f'{lat_col}_rounded', f'{lng_col}_rounded'] + time_cols
            duplicate_mask = df_temp.duplicated(subset=duplicate_cols, keep='first')
            df = df[~duplicate_mask]
    
    return df

# ==========================================
# Hàm Xử Lý Chính
# ==========================================

def get_file_info(file_path: str) -> Dict:
    """Lấy thông tin chi tiết file"""
    if not os.path.exists(file_path):
        return {"size_mb": 0, "exists": False}
    
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    size_gb = size_mb / 1024
    
    return {
        "size_bytes": size_bytes,
        "size_mb": size_mb,
        "size_gb": size_gb,
        "exists": True,
        "formatted": f"{size_mb:.1f} MB" if size_mb < 1024 else f"{size_gb:.2f} GB"
    }

def process_chunks(input_file: str, output_file: str, chunk_size: int = 2600000,
                  columns_to_delete: List[str] = None, date_cutoff: str = "2018-01-01") -> Optional[Dict]:
    """Xử lý dữ liệu theo khối"""
    
    if columns_to_delete is None:
        columns_to_delete = ['ID', 'Description', 'End_Lat', 'End_Lng', 'End_Time', 'Weather_Timestamp']
    
    if not os.path.exists(input_file):
        print(f"❌ File không tồn tại: {input_file}")
        return None
    
    # Xóa file đầu ra nếu có
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Thống kê xử lý
    stats = {
        'chunks_processed': 0,
        'total_rows_input': 0,
        'total_rows_output': 0,
        'columns_deleted': 0,
        'time_features_added': 7,
        'phase_stats': {}
    }
    
    first_chunk = True
    
    try:
        # Đếm tổng dòng
        print("🔍 Đang đếm tổng số dòng...")
        total_lines = sum(1 for _ in open(input_file, encoding='utf-8')) - 1
        total_chunks = (total_lines + chunk_size - 1) // chunk_size
        print(f"📊 Tổng {total_lines:,} dòng, {total_chunks} khối")
        
        # Xử lý từng khối
        chunk_reader = pd.read_csv(input_file, chunksize=chunk_size, low_memory=False)
        
        with tqdm(total=total_chunks, desc="Xử lý khối", unit="khối") as pbar:
            for chunk_num, chunk in enumerate(chunk_reader, 1):
                initial_rows = len(chunk)
                initial_cols = len(chunk.columns)
                
                # Áp dụng các pha xử lý
                try:
                    chunk = phase_delete_columns(chunk, columns_to_delete)
                    if chunk_num == 1:
                        stats['columns_deleted'] = initial_cols - len(chunk.columns)
                    
                    chunk = phase_filter_date(chunk, date_cutoff=date_cutoff)
                    if len(chunk) == 0:
                        pbar.update(1)
                        continue
                    
                    chunk = phase_create_time_features(chunk)
                    chunk = phase_sql_data_types(chunk)
                    chunk = phase_standardize_columns(chunk)
                    chunk = phase_validate_clean(chunk)
                    
                except Exception as e:
                    print(f"\n❌ Lỗi xử lý khối {chunk_num}: {e}")
                    return None
                
                # Lưu khối
                if first_chunk:
                    chunk.to_csv(output_file, index=False, mode='w')
                    first_chunk = False
                else:
                    chunk.to_csv(output_file, index=False, mode='a', header=False)
                
                # Cập nhật thống kê
                stats['chunks_processed'] = chunk_num
                stats['total_rows_input'] += initial_rows
                stats['total_rows_output'] += len(chunk)
                
                # Cập nhật progress bar
                pbar.set_postfix({
                    'Dòng đầu vào': f"{initial_rows:,}",
                    'Dòng đầu ra': f"{len(chunk):,}",
                    'Tổng': f"{stats['total_rows_output']:,}"
                })
                pbar.update(1)
                
                # Dọn dẹp bộ nhớ
                del chunk
                gc.collect()
        
        return stats
        
    except Exception as e:
        print(f"❌ Lỗi xử lý: {e}")
        return None

def analyze_dataset_detailed(file_path: str, sample_size: int = 50000) -> Optional[pd.DataFrame]:
    """Phân tích chi tiết bộ dữ liệu"""
    if not os.path.exists(file_path):
        print(f"❌ File không tồn tại: {file_path}")
        return None
    
    try:
        df_sample = pd.read_csv(file_path, nrows=sample_size, low_memory=False)
        total_rows = sum(1 for _ in open(file_path, encoding='utf-8')) - 1
        file_info = get_file_info(file_path)
        
        print(f"📁 File: {os.path.basename(file_path)}")
        print(f"💾 Kích thước: {file_info['formatted']}")
        print(f"📏 Tổng dòng: {total_rows:,}")
        print(f"📐 Tổng cột: {len(df_sample.columns)}")
        
        # Phân tích kiểu dữ liệu
        print(f"\n📊 KIỂU DỮ LIỆU:")
        dtype_counts = df_sample.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {dtype}: {count} cột")
        
        # Giá trị thiếu
        missing = df_sample.isnull().sum()
        if missing.sum() > 0:
            print(f"\n🔍 GIÁ TRỊ THIẾU:")
            missing_pct = (missing / len(df_sample) * 100).round(2)
            for col in missing[missing > 0].head(5).index:
                print(f"  {col}: {missing[col]:,} ({missing_pct[col]}%)")
        else:
            print(f"\n✅ Không có giá trị thiếu!")
        
        return df_sample
        
    except Exception as e:
        print(f"❌ Lỗi phân tích: {e}")
        return None

def compare_datasets_detailed(original_file: str, processed_file: str, processing_stats: Dict) -> None:
    """So sánh chi tiết hai bộ dữ liệu"""
    print(f"\n" + "="*70)
    print("🔄 SO SÁNH CHI TIẾT BỘ DỮ LIỆU")
    print("="*70)
    
    # Thông tin file
    orig_info = get_file_info(original_file)
    proc_info = get_file_info(processed_file)
    
    print(f"📁 FILE:")
    print(f"  Gốc: {orig_info['formatted']}")
    print(f"  Xử lý: {proc_info['formatted']}")
    
    if orig_info['exists'] and proc_info['exists']:
        reduction = ((orig_info['size_mb'] - proc_info['size_mb']) / orig_info['size_mb']) * 100
        print(f"  Giảm dung lượng: {reduction:.1f}%")
    
    # Thống kê dòng
    print(f"\n📏 DỮ LIỆU:")
    print(f"  Dòng đầu vào: {processing_stats['total_rows_input']:,}")
    print(f"  Dòng đầu ra: {processing_stats['total_rows_output']:,}")
    row_reduction = ((processing_stats['total_rows_input'] - processing_stats['total_rows_output']) / processing_stats['total_rows_input']) * 100
    print(f"  Giảm dòng: {row_reduction:.1f}%")
    print(f"  Cột đã xóa: {processing_stats['columns_deleted']}")
    print(f"  Đặc trưng thời gian thêm: {processing_stats['time_features_added']}")
    print(f"  Khối đã xử lý: {processing_stats['chunks_processed']}")

def main(input_file: str = "../US_Accidents_March23.csv",
         output_file: str = "../US_Accidents_March23-final.csv",
         chunk_size: int = 2600000,
         date_cutoff: str = "2018-01-01",
         columns_to_delete: List[str] = None) -> bool:
    """Hàm chính"""
    
    if columns_to_delete is None:
        columns_to_delete = ['ID', 'Description', 'End_Lat', 'End_Lng', 'End_Time', 'Weather_Timestamp']
    
    print("🚀 HỆ THỐNG TIỀN XỬ LÝ DỮ LIỆU CUỐI CÙNG")
    print(f"⏰ Bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Cấu hình
    print("⚙️ CẤU HÌNH:")
    print(f"  File đầu vào: {input_file}")
    print(f"  File đầu ra: {output_file}")
    print(f"  Kích thước khối: {chunk_size:,} dòng")
    print(f"  Ngày cắt: {date_cutoff}")
    print(f"  Cột xóa: {len(columns_to_delete)} cột")
    
    try:
        # Phân tích dữ liệu gốc
        print(f"\n📊 PHÂN TÍCH DỮ LIỆU GỐC:")
        print("-" * 40)
        original_sample = analyze_dataset_detailed(input_file)
        
        # Xử lý dữ liệu
        print(f"\n🔄 XỬ LÝ DỮ LIỆU:")
        processing_stats = process_chunks(
            input_file=input_file,
            output_file=output_file,
            chunk_size=chunk_size,
            columns_to_delete=columns_to_delete,
            date_cutoff=date_cutoff
        )
        
        if processing_stats is None:
            print("❌ Xử lý thất bại")
            return False
        
        # Phân tích dữ liệu đã xử lý
        print(f"\n📊 PHÂN TÍCH DỮ LIỆU ĐÃ XỬ LÝ:")
        print("-" * 40)
        processed_sample = analyze_dataset_detailed(output_file)
        
        # So sánh chi tiết
        compare_datasets_detailed(input_file, output_file, processing_stats)
        
        # Tạo báo cáo chuyển đổi kiểu dữ liệu
        print(f"\n📋 TẠO BÁO CÁO CHUYỂN ĐỔI KIỂU DỮ LIỆU...")
        try:
            from type_conversion import generate_type_conversion_report
            generate_type_conversion_report(output_file, processed_sample)
        except ImportError:
            print("⚠️ Không tìm thấy type_conversion.py - bỏ qua báo cáo chuyển đổi")
        
        print(f"\n" + "="*70)
        print("✅ TIỀN XỬ LÝ HOÀN THÀNH!")
        print(f"📁 Kết quả: {output_file}")
        print(f"⏰ Hoàn thành: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)