import os
import re
import gc
import warnings
import argparse
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional, Dict
from tqdm import tqdm

# Tắt cảnh báo
warnings.filterwarnings('ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

# ==========================================
# Preprocessing Reporter
# ==========================================

class PreprocessingReporter:
    def __init__(self, input_file: str, output_file: str):
        """Khởi tạo reporter"""
        dataset_name = os.path.splitext(os.path.basename(input_file))[0]
        self.input_file = input_file
        self.output_file = output_file
        self.report_file = os.path.join(os.path.dirname(output_file), f"{dataset_name}-preprocess_report.txt")
        self.report_content = []
        
    def add_to_report(self, content: str):
        """Thêm nội dung vào báo cáo"""
        self.report_content.append(content)
        
    def save_report(self) -> bool:
        """Lưu báo cáo ra file"""
        try:
            with open(self.report_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(self.report_content))
            return True
        except Exception as e:
            return False
    
    def generate_header(self):
        """Tạo header cho báo cáo"""
        self.add_to_report("="*80)
        self.add_to_report("🔄 BÁO CÁO TIỀN XỬ LÝ DỮ LIỆU")
        self.add_to_report("="*80)
        self.add_to_report(f"📅 Ngày tạo: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.add_to_report("")
    
    def analyze_original_dataset(self) -> Optional[Dict]:
        """Phân tích dataset gốc"""
        if not os.path.exists(self.input_file):
            return None
            
        try:
            sample_df = pd.read_csv(self.input_file, nrows=10000, low_memory=False)
            total_rows = sum(1 for _ in open(self.input_file, encoding='utf-8')) - 1
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(sample_df.columns),
                'file_size': get_file_info(self.input_file),
                'memory_usage': sample_df.memory_usage(deep=True).sum() / 1024 / 1024,
                'data_types': sample_df.dtypes.value_counts().to_dict(),
                'missing_values': sample_df.isnull().sum().sum(),
                'missing_percentage': (sample_df.isnull().sum().sum() / (len(sample_df) * len(sample_df.columns))) * 100,
                'duplicates': sample_df.duplicated().sum(),
                'column_names': list(sample_df.columns)
            }
            
            return analysis
            
        except Exception as e:
            return None
    
    def analyze_processed_dataset(self) -> Optional[Dict]:
        """Phân tích dataset đã xử lý"""
        if not os.path.exists(self.output_file):
            return None
            
        try:
            sample_df = pd.read_csv(self.output_file, nrows=10000, low_memory=False)
            total_rows = sum(1 for _ in open(self.output_file, encoding='utf-8')) - 1
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(sample_df.columns),
                'file_size': get_file_info(self.output_file),
                'memory_usage': sample_df.memory_usage(deep=True).sum() / 1024 / 1024,
                'data_types': sample_df.dtypes.value_counts().to_dict(),
                'missing_values': sample_df.isnull().sum().sum(),
                'missing_percentage': (sample_df.isnull().sum().sum() / (len(sample_df) * len(sample_df.columns))) * 100,
                'duplicates': sample_df.duplicated().sum(),
                'column_names': list(sample_df.columns)
            }
            
            return analysis
            
        except Exception as e:
            return None
    
    def generate_comparison_report(self, original_analysis: Dict, processed_analysis: Dict, processing_stats: Dict):
        """Tạo báo cáo so sánh chi tiết"""
        
        # Thêm thông tin cấu hình
        if 'file_info' in processing_stats:
            config = processing_stats['file_info']
            self.add_to_report("⚙️ THÔNG TIN CẤU HÌNH")
            self.add_to_report("-" * 50)
            self.add_to_report(f"File đầu vào: {config['input_file']}")
            self.add_to_report(f"File đầu ra: {config['output_file']}")
            self.add_to_report(f"Kích thước khối: {config['chunk_size']:,} dòng")
            self.add_to_report(f"Ngày cắt: {config['date_cutoff']}")
            self.add_to_report(f"Cột xóa: {len(config['columns_to_delete'])} cột")
            self.add_to_report("")
        
        # Thêm processing log nếu có
        if 'processing_log' in processing_stats and processing_stats['processing_log']:
            self.add_to_report("📝 CHI TIẾT QUÁ TRÌNH XỬ LÝ")
            self.add_to_report("-" * 50)
            for log_entry in processing_stats['processing_log']:
                self.add_to_report(log_entry)
            self.add_to_report("")
        
        # Thêm comparison log nếu có
        if 'comparison_log' in processing_stats and processing_stats['comparison_log']:
            for log_entry in processing_stats['comparison_log']:
                self.add_to_report(log_entry)
            self.add_to_report("")
        
        # Thêm analysis gốc nếu có
        if 'original_analysis' in processing_stats and processing_stats['original_analysis']:
            orig = processing_stats['original_analysis']
            self.add_to_report("📊 PHÂN TÍCH DỮ LIỆU GỐC")
            self.add_to_report("-" * 50)
            for summary in orig['analysis_summary']:
                self.add_to_report(summary)
            
            if orig['dtype_counts']:
                self.add_to_report("\nKiểu dữ liệu:")
                for dtype, count in orig['dtype_counts'].items():
                    self.add_to_report(f"  {dtype}: {count} cột")
            
            if orig['missing_info']:
                self.add_to_report("\nGiá trị thiếu:")
                for missing in orig['missing_info']:
                    self.add_to_report(f"  {missing}")
            else:
                self.add_to_report("\n✅ Không có giá trị thiếu!")
            self.add_to_report("")
        
        # Thêm analysis đã xử lý nếu có
        if 'processed_analysis' in processing_stats and processing_stats['processed_analysis']:
            proc = processing_stats['processed_analysis']
            self.add_to_report("📊 PHÂN TÍCH DỮ LIỆU ĐÃ XỬ LÝ")
            self.add_to_report("-" * 50)
            for summary in proc['analysis_summary']:
                self.add_to_report(summary)
            
            if proc['dtype_counts']:
                self.add_to_report("\nKiểu dữ liệu:")
                for dtype, count in proc['dtype_counts'].items():
                    self.add_to_report(f"  {dtype}: {count} cột")
            
            if proc['missing_info']:
                self.add_to_report("\nGiá trị thiếu:")
                for missing in proc['missing_info']:
                    self.add_to_report(f"  {missing}")
            else:
                self.add_to_report("\n✅ Không có giá trị thiếu!")
            self.add_to_report("")
        
        self.add_to_report("🔍 THÔNG TIN TỔNG QUAN")
        self.add_to_report("-" * 50)
        
        # Thông tin cơ bản
        self.add_to_report(f"Dataset gốc:")
        self.add_to_report(f"  - Số dòng: {original_analysis['total_rows']:,}")
        self.add_to_report(f"  - Số cột: {original_analysis['total_columns']}")
        self.add_to_report(f"  - Kích thước file: {original_analysis['file_size']['formatted']}")
        self.add_to_report(f"  - Bộ nhớ: {original_analysis['memory_usage']:.1f} MB")
        
        self.add_to_report(f"\nDataset đã xử lý:")
        self.add_to_report(f"  - Số dòng: {processed_analysis['total_rows']:,}")
        self.add_to_report(f"  - Số cột: {processed_analysis['total_columns']}")
        self.add_to_report(f"  - Kích thước file: {processed_analysis['file_size']['formatted']}")
        self.add_to_report(f"  - Bộ nhớ: {processed_analysis['memory_usage']:.1f} MB")
        
        # Thống kê thay đổi
        self.add_to_report(f"\n📊 THAY ĐỔI SAU TIỀN XỬ LÝ")
        self.add_to_report("-" * 50)
        
        # Thay đổi số dòng
        row_change = processed_analysis['total_rows'] - original_analysis['total_rows']
        row_change_pct = (row_change / original_analysis['total_rows']) * 100
        self.add_to_report(f"Số dòng: {row_change:+,} ({row_change_pct:+.1f}%)")
        
        # Thay đổi số cột
        col_change = processed_analysis['total_columns'] - original_analysis['total_columns']
        self.add_to_report(f"Số cột: {col_change:+} cột")
        
        # Thay đổi kích thước file
        size_change_mb = processed_analysis['file_size']['size_mb'] - original_analysis['file_size']['size_mb']
        size_change_pct = (size_change_mb / original_analysis['file_size']['size_mb']) * 100
        self.add_to_report(f"Kích thước file: {size_change_mb:+.1f} MB ({size_change_pct:+.1f}%)")
        
        # Thay đổi bộ nhớ
        memory_change = processed_analysis['memory_usage'] - original_analysis['memory_usage']
        memory_change_pct = (memory_change / original_analysis['memory_usage']) * 100
        self.add_to_report(f"Bộ nhớ: {memory_change:+.1f} MB ({memory_change_pct:+.1f}%)")
        
        # Chi tiết các pha xử lý
        self.add_to_report(f"\n🔄 CHI TIẾT QUÁ TRÌNH XỬ LÝ")
        self.add_to_report("-" * 50)
        self.add_to_report(f"Tổng số khối đã xử lý: {processing_stats['chunks_processed']}")
        self.add_to_report(f"Cột đã xóa: {processing_stats['columns_deleted']}")
        self.add_to_report(f"Đặc trưng thời gian thêm: {processing_stats['time_features_added']}")
        
        # Cột đã xóa
        original_cols = set(original_analysis['column_names'])
        processed_cols = set(processed_analysis['column_names'])
        deleted_cols = original_cols - processed_cols
        added_cols = processed_cols - original_cols
        
        if deleted_cols:
            self.add_to_report(f"\nCột đã xóa ({len(deleted_cols)}):")
            for col in sorted(deleted_cols):
                self.add_to_report(f"  - {col}")
        
        if added_cols:
            self.add_to_report(f"\nCột đã thêm ({len(added_cols)}):")
            for col in sorted(added_cols):
                self.add_to_report(f"  - {col}")
    
    def generate_processing_phases_detail(self):
        """Chi tiết các pha xử lý"""
        self.add_to_report(f"\n🔄 CHI TIẾT CÁC PHA TIỀN XỬ LÝ")
        self.add_to_report("-" * 50)
        
        phases = [
            ("Pha 1", "Tính DURATION, xóa cột và lọc chuỗi dài", "Tính thời lượng, loại bỏ cột không cần, lọc records có chuỗi > 50 ký tự"),
            ("Pha 2", "Lọc dữ liệu theo ngày", "Chỉ giữ lại dữ liệu từ 2018 trở lên"),
            ("Pha 3", "Tạo đặc trưng thời gian", "Thêm DATE, YEAR, QUARTER, MONTH, DAY, HOUR, IS_WEEKEND"),
            ("Pha 4", "Chuyển đổi kiểu dữ liệu SQL", "Chuẩn hóa chuỗi (trim, Unknown), tối ưu kiểu dữ liệu"),
            ("Pha 5", "Chuẩn hóa tên cột", "Chuyển tên cột thành chữ hoa và chuẩn hóa"),
            ("Pha 6", "Sắp xếp thứ tự cột", "Sắp xếp cột theo thứ tự DDL SQL Server")
        ]
        
        for phase_num, phase_name, description in phases:
            self.add_to_report(f"{phase_num}: {phase_name}")
            self.add_to_report(f"   {description}")
            self.add_to_report("")
    
    def generate_full_report(self, processing_stats: Dict) -> bool:
        """Tạo báo cáo đầy đủ"""
        # Header
        self.generate_header()
        
        # Phân tích dataset gốc và đã xử lý
        original_analysis = self.analyze_original_dataset()
        processed_analysis = self.analyze_processed_dataset()
        
        if not original_analysis or not processed_analysis:
            self.add_to_report("❌ Không thể phân tích được các dataset")
            return self.save_report()
        
        # So sánh chi tiết
        self.generate_comparison_report(original_analysis, processed_analysis, processing_stats)
        
        # Chi tiết các pha xử lý
        self.generate_processing_phases_detail()
        
        # Kết luận
        self.add_to_report("🎆 KẾT QUẢ")
        self.add_to_report("-" * 50)
        self.add_to_report("✅ Tiền xử lý hoàn thành thành công!")
        self.add_to_report(f"Dataset đã được tối ưu hóa cho SQL Server")
        self.add_to_report(f"Sẵn sàng cho việc import vào cơ sở dữ liệu")
        
        return self.save_report()

# ==========================================
# Các Pha Tiền Xử Lý Thuần Túy
# ==========================================

def phase_delete_columns(df: pd.DataFrame, columns_to_delete: List[str]) -> pd.DataFrame:
    """Pha 1: Xóa cột không cần thiết và tính DURATION"""
    # Tính toán DURATION trước khi xóa End_Time
    if 'Start_Time' in df.columns and 'End_Time' in df.columns:
        # Chuyển đổi về datetime nếu chưa phải
        df['Start_Time'] = pd.to_datetime(df['Start_Time'], errors='coerce')
        df['End_Time'] = pd.to_datetime(df['End_Time'], errors='coerce')
        
        # Tính DURATION bằng giây (làm tròn thành số nguyên)
        df['DURATION'] = (df['End_Time'] - df['Start_Time']).dt.total_seconds()
        df['DURATION'] = df['DURATION'].fillna(0).round().astype('int64')
        
        # Xử lý giá trị âm (đặt về 0)
        df.loc[df['DURATION'] < 0, 'DURATION'] = 0
    
    # Xóa các cột không cần thiết
    columns_to_drop = [col for col in columns_to_delete if col in df.columns]
    df = df.drop(columns=columns_to_drop) if columns_to_drop else df
    
    # Lọc records có chuỗi dài > 50 ký tự (silent)
    string_cols = df.select_dtypes(include=['object']).columns
    if len(string_cols) > 0:
        mask = df[string_cols].apply(lambda x: x.astype(str).str.len() <= 50).all(axis=1)
        df = df[mask]
    
    return df

def phase_filter_date(df: pd.DataFrame, time_column: str = 'Start_Time', 
                     date_cutoff: str = "2018-01-01") -> pd.DataFrame:
    """Pha 2: Lọc dữ liệu theo ngày"""
    if time_column not in df.columns:
        return df
    
    # Convert to datetime if needed
    if df[time_column].dtype != 'datetime64[ns]':
        df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
    
    cutoff_date = pd.to_datetime(date_cutoff)
    return df[df[time_column] >= cutoff_date]

def phase_create_time_features(df: pd.DataFrame, time_column: str = 'Start_Time') -> pd.DataFrame:
    """Pha 3: Tạo đặc trưng thời gian"""
    if time_column not in df.columns:
        return df
    
    # Chuyển đổi sang datetime nếu cần
    df[time_column] = pd.to_datetime(df[time_column], errors='coerce')
    
    # Tạo đặc trưng thời gian cơ bản (bao gồm DATE)
    df['DATE'] = df[time_column].dt.date
    df['YEAR'] = df[time_column].dt.year.astype('int16')
    df['QUARTER'] = df[time_column].dt.quarter.astype('int8')
    df['MONTH'] = df[time_column].dt.month.astype('int8')
    df['DAY'] = df[time_column].dt.day.astype('int8')
    df['HOUR'] = df[time_column].dt.hour.astype('int8')
    df['IS_WEEKEND'] = df[time_column].dt.dayofweek.isin([5, 6]).astype('bool')
    
    # Loại bỏ cột thời gian gốc để tránh dư thừa
    return df.drop(columns=[time_column])

def phase_sql_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 4: Chuyển đổi kiểu dữ liệu SQL Server"""
    
    # Chuẩn hóa chuỗi: trim khoảng trắng, thay null bằng "Unknown"
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(['nan', 'None', ''], 'Unknown')
    
    # Chuyển DATE sang string để lưu vào CSV (sẽ là DATE trong SQL)
    if 'DATE' in df.columns:
        df['DATE'] = df['DATE'].astype(str)
    
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
        elif col in ['QUARTER', 'MONTH', 'DAY', 'HOUR']:
            df[col] = df[col].astype('int8')   # tinyint
        elif col == 'DURATION':
            df[col] = df[col].astype('int64')  # bigint cho DURATION (giây)
        else:
            df[col] = df[col].astype('int32')  # int32
    
    # Danh sách các cột environment và IS_WEEKEND luôn là Boolean (BIT)
    environment_boolean_cols = [
        'IS_WEEKEND', 'AMENITY', 'BUMP', 'CROSSING', 'GIVE_WAY', 'JUNCTION', 
        'NO_EXIT', 'RAILWAY', 'ROUNDABOUT', 'STATION', 'STOP', 
        'TRAFFIC_CALMING', 'TRAFFIC_SIGNAL', 'TURNING_LOOP'
    ]
    
    # Đảm bảo các cột environment được convert về bool nếu chúng là numeric
    for col in environment_boolean_cols:
        if col in df.columns:
            if df[col].dtype in ['int8', 'int16', 'int32', 'int64', 'float32', 'float64']:
                # Convert về bool (0 -> False, non-zero -> True)
                df[col] = df[col].astype('bool')

    # Chuỗi: chuyển về string type
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].astype('string')
    
    return df

def phase_standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 5: Chuẩn hóa tên cột"""
    column_mapping = {}
    for col in df.columns:
        # Chuyển tên cột thành chữ hoa và chuẩn hóa
        new_col = col.upper()

        # Loại bỏ ký tự đặc biệt và thay thế khoảng trắng
        new_col = re.sub(r'\([^)]*\)', '', new_col)

        # Thay thế khoảng trắng bằng dấu gạch dưới
        new_col = re.sub(r'\s+', '_', new_col.strip())

        # Loại bỏ các ký tự không phải chữ cái, số, hoặc dấu gạch dưới
        new_col = re.sub(r'_+', '_', new_col).strip('_')
        column_mapping[col] = new_col
    
    # Đổi tên cột
    df = df.rename(columns=column_mapping)
    
    # Đổi tên tọa độ cụ thể
    coordinate_mapping = {'START_LAT': 'LATITUDE', 'START_LNG': 'LONGITUDE'}
    for old_name, new_name in coordinate_mapping.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    return df

def phase_reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 6: Sắp xếp lại thứ tự cột theo DDL SQL Server"""
    
    # Định nghĩa thứ tự cột theo DDL - fact attributes lên đầu
    fact_columns = ['SEVERITY', 'DISTANCE', 'DURATION']
    
    # Các nhóm cột dimension theo thứ tự DDL (loại bỏ SOURCE)
    time_columns = ['DATE', 'YEAR', 'QUARTER', 'MONTH', 'DAY', 'HOUR', 'IS_WEEKEND']
    
    location_columns = ['COUNTRY', 'STATE', 'COUNTY', 'CITY', 'STREET', 'ZIPCODE', 'LATITUDE', 'LONGITUDE']
    
    weather_columns = ['TEMPERATURE', 'WIND_CHILL', 'HUMIDITY', 'PRESSURE', 'VISIBILITY', 
                      'WIND_DIRECTION', 'WIND_SPEED', 'PRECIPITATION', 'WEATHER_CONDITION',
                      'SUNRISE_SUNSET']
    
    environment_columns = ['AMENITY', 'BUMP', 'CROSSING', 'GIVE_WAY', 'JUNCTION', 'NO_EXIT',
                          'RAILWAY', 'ROUNDABOUT', 'STATION', 'STOP', 'TRAFFIC_CALMING',
                          'TRAFFIC_SIGNAL', 'TURNING_LOOP']
    
    # Tạo danh sách cột theo thứ tự mong muốn
    desired_order = []
    
    # Thêm fact columns trước (những cột có trong DataFrame)
    for col in fact_columns:
        if col in df.columns:
            desired_order.append(col)
    
    # Thêm các dimension columns theo thứ tự DDL
    for group in [time_columns, location_columns, weather_columns, environment_columns]:
        for col in group:
            if col in df.columns:
                desired_order.append(col)
    
    # Thêm các cột còn lại (nếu có)
    remaining_cols = [col for col in df.columns if col not in desired_order]
    desired_order.extend(remaining_cols)
    
    # Sắp xếp lại DataFrame theo thứ tự mong muốn
    return df[desired_order]

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
    """Xử lý dữ liệu theo khối - Tối ưu hóa tốc độ (Minimal printing)"""
    
    if columns_to_delete is None:
        columns_to_delete = [
            'ID', 'Description', 'End_Lat', 'End_Lng', 'End_Time', 'Weather_Timestamp',
            'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight',
            'Airport_Code', 'Timezone', 'Source'
        ]
    
    if not os.path.exists(input_file):
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
        'time_features_added': 7,  # DATE, YEAR, QUARTER, MONTH, DAY, HOUR, IS_WEEKEND
        'phase_stats': {},
        'processing_log': [],
        'file_info': {
            'input_file': input_file,
            'output_file': output_file,
            'chunk_size': chunk_size,
            'date_cutoff': date_cutoff,
            'columns_to_delete': columns_to_delete
        }
    }
    
    first_chunk = True
    
    try:
        # Đếm tổng dòng
        total_lines = sum(1 for _ in open(input_file, encoding='utf-8')) - 1
        total_chunks = (total_lines + chunk_size - 1) // chunk_size
        stats['processing_log'].append(f"📊 Tổng {total_lines:,} dòng, {total_chunks} khối")
        stats['total_lines'] = total_lines
        stats['total_chunks'] = total_chunks
        
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
                    chunk = phase_reorder_columns(chunk)
                    
                except Exception as e:
                    stats['processing_log'].append(f"❌ Lỗi xử lý khối {chunk_num}: {e}")
                    return None
                
                # Lưu khối
                try:
                    if first_chunk:
                        chunk.to_csv(output_file, index=False, mode='w')
                        first_chunk = False
                        stats['processing_log'].append(f"📝 Tạo file đầu ra với {len(chunk.columns)} cột")
                    else:
                        chunk.to_csv(output_file, index=False, mode='a', header=False)
                except Exception as e:
                    stats['processing_log'].append(f"❌ Lỗi lưu khối {chunk_num}: {e}")
                    return None
                
                # Cập nhật thống kê
                stats['chunks_processed'] = chunk_num
                stats['total_rows_input'] += initial_rows
                stats['total_rows_output'] += len(chunk)
                
                # Cập nhật progress bar với minimal info
                pbar.set_postfix({
                    'Processed': f"{stats['total_rows_output']:,}"
                })
                pbar.update(1)
                
                # Dọn dẹp bộ nhớ
                del chunk
                gc.collect()
        
        stats['processing_log'].append("✅ Hoàn thành xử lý tất cả khối")
        return stats
        
    except Exception as e:
        if 'processing_log' in stats:
            stats['processing_log'].append(f"❌ Lỗi xử lý: {e}")
        return None

def analyze_dataset_detailed(file_path: str, sample_size: int = 50000) -> Optional[Dict]:
    """Phân tích chi tiết bộ dữ liệu và trả về thông tin (No printing)"""
    if not os.path.exists(file_path):
        return None
    
    try:
        df_sample = pd.read_csv(file_path, nrows=sample_size, low_memory=False)
        total_rows = sum(1 for _ in open(file_path, encoding='utf-8')) - 1
        file_info = get_file_info(file_path)
        
        # Phân tích kiểu dữ liệu
        dtype_counts = df_sample.dtypes.value_counts()
        
        # Giá trị thiếu
        missing = df_sample.isnull().sum()
        missing_info = []
        if missing.sum() > 0:
            missing_pct = (missing / len(df_sample) * 100).round(2)
            for col in missing[missing > 0].head(5).index:
                missing_info.append(f"{col}: {missing[col]:,} ({missing_pct[col]}%)")
        
        return {
            'sample_df': df_sample,
            'total_rows': total_rows,
            'file_info': file_info,
            'dtype_counts': dtype_counts.to_dict(),
            'missing_info': missing_info,
            'analysis_summary': [
                f"📁 File: {os.path.basename(file_path)}",
                f"💾 Kích thước: {file_info['formatted']}",
                f"📏 Tổng dòng: {total_rows:,}",
                f"📐 Tổng cột: {len(df_sample.columns)}"
            ]
        }
        
    except Exception as e:
        return None

def compare_datasets_detailed(original_file: str, processed_file: str, processing_stats: Dict) -> List[str]:
    """So sánh chi tiết hai bộ dữ liệu và trả về thông tin (No printing)"""
    comparison_log = []
    
    # Thông tin file
    orig_info = get_file_info(original_file)
    proc_info = get_file_info(processed_file)
    
    comparison_log.append("🔄 SO SÁNH CHI TIẾT BỘ DỮ LIỆU")
    comparison_log.append("="*70)
    comparison_log.append(f"📁 FILE:")
    comparison_log.append(f"  Gốc: {orig_info['formatted']}")
    comparison_log.append(f"  Xử lý: {proc_info['formatted']}")
    
    if orig_info['exists'] and proc_info['exists']:
        reduction = ((orig_info['size_mb'] - proc_info['size_mb']) / orig_info['size_mb']) * 100
        comparison_log.append(f"  Giảm dung lượng: {reduction:.1f}%")
    
    # Thống kê dòng
    comparison_log.append(f"📏 DỮ LIỆU:")
    comparison_log.append(f"  Dòng đầu vào: {processing_stats['total_rows_input']:,}")
    comparison_log.append(f"  Dòng đầu ra: {processing_stats['total_rows_output']:,}")
    row_reduction = ((processing_stats['total_rows_input'] - processing_stats['total_rows_output']) / processing_stats['total_rows_input']) * 100
    comparison_log.append(f"  Giảm dòng: {row_reduction:.1f}%")
    comparison_log.append(f"  Cột đã xóa: {processing_stats['columns_deleted']}")
    comparison_log.append(f"  Đặc trưng thời gian thêm: {processing_stats['time_features_added']}")
    comparison_log.append(f"  Khối đã xử lý: {processing_stats['chunks_processed']}")
    
    return comparison_log

def parse_arguments():
    """Phân tích tham số dòng lệnh"""
    parser = argparse.ArgumentParser(
        description='Hệ thống tiền xử lý dữ liệu - Tối ưu hóa cho SQL Server (Minimal Output)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Ví dụ sử dụng:
  python preprocess.py ../US_Accidents_March23.csv
  python preprocess.py input.csv -o output.csv -c 2600000
  python preprocess.py data.csv --date-cutoff 2020-01-01 --chunk-size 500000
  python preprocess.py data.csv --delete-columns ID,Country,Description
        """
    )
    
    # Tham số bắt buộc
    parser.add_argument(
        'input_file', 
        help='File CSV đầu vào (bắt buộc)'
    )
    
    # Tham số tùy chọn
    parser.add_argument(
        '-o', '--output', 
        dest='output_file',
        help='File CSV đầu ra (mặc định: tự động tạo từ tên file đầu vào)'
    )
    
    parser.add_argument(
        '-c', '--chunk-size',
        type=int,
        default=2600000,
        help='Kích thước khối xử lý (mặc định: 1,000,000 dòng)'
    )
    
    parser.add_argument(
        '-d', '--date-cutoff',
        default="2018-01-01",
        help='Ngày cắt lọc dữ liệu (mặc định: 2018-01-01)'
    )
    
    parser.add_argument(
        '--delete-columns',
        help='Danh sách cột cần xóa, cách nhau bằng dấu phẩy'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Hiển thị thông tin chi tiết'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Preprocess System v2.1 - Minimal Output'
    )
    
    return parser.parse_args()

def validate_arguments(args):
    """Xác thực tham số đầu vào (Minimal output)"""
    errors = []
    
    # Kiểm tra file đầu vào
    if not os.path.exists(args.input_file):
        errors.append(f"❌ File đầu vào không tồn tại: {args.input_file}")
    elif not args.input_file.lower().endswith('.csv'):
        errors.append(f"⚠️ File đầu vào không phải CSV: {args.input_file}")
    
    # Kiểm tra chunk size
    if args.chunk_size <= 0:
        errors.append(f"❌ Kích thước khối phải > 0: {args.chunk_size}")
    elif args.chunk_size < 1000:
        errors.append(f"⚠️ Kích thước khối quá nhỏ (< 1000): {args.chunk_size}")
    
    # Kiểm tra định dạng ngày
    try:
        pd.to_datetime(args.date_cutoff)
    except:
        errors.append(f"❌ Định dạng ngày không hợp lệ: {args.date_cutoff}")
    
    # Kiểm tra thư mục đầu ra (Silent create)
    output_dir = os.path.dirname(os.path.abspath(args.output_file))
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
        except:
            errors.append(f"❌ Không thể tạo thư mục đầu ra: {output_dir}")
    
    return errors

def main(input_file: str = "../US_Accidents_March23.csv",
         output_file: str = "../US_Accidents_March23-preprocessed.csv",
         chunk_size: int = 2600000,
         date_cutoff: str = "2018-01-01",
         columns_to_delete: List[str] = None) -> bool:
    """Hàm chính (Minimal output)"""
    
    if columns_to_delete is None:
        columns_to_delete = [
            'ID', 'Description', 'End_Lat', 'End_Lng', 'End_Time', 'Weather_Timestamp',
            'Civil_Twilight', 'Nautical_Twilight', 'Astronomical_Twilight',
            'Airport_Code', 'Timezone', 'Source'
        ]
    
    # Tạo reporter
    reporter = PreprocessingReporter(input_file, output_file)
    
    try:
        # Phân tích dữ liệu gốc (Silent)
        original_analysis = analyze_dataset_detailed(input_file)
        if not original_analysis:
            return False
        
        # Xử lý dữ liệu
        processing_stats = process_chunks(
            input_file=input_file,
            output_file=output_file,
            chunk_size=chunk_size,
            columns_to_delete=columns_to_delete,
            date_cutoff=date_cutoff
        )
        
        if processing_stats is None:
            return False
        
        # Phân tích dữ liệu đã xử lý (Silent)
        processed_analysis = analyze_dataset_detailed(output_file)
        
        # So sánh chi tiết (Silent)
        comparison_log = compare_datasets_detailed(input_file, output_file, processing_stats)
        
        # Thêm thông tin vào processing stats để report sử dụng
        processing_stats['original_analysis'] = original_analysis
        processing_stats['processed_analysis'] = processed_analysis
        processing_stats['comparison_log'] = comparison_log
        
        # Tạo báo cáo tiền xử lý (Silent)
        reporter.generate_full_report(processing_stats)
        
        # Chỉ print minimal thông tin cuối
        print(f"✅ TIỀN XỬ LÝ HOÀN THÀNH!")
        print(f"📁 Kết quả: {output_file}")
        print(f"📄 Báo cáo: {reporter.report_file}")
        print(f"\n{'='*80}")
        
        return True
        
    except Exception as e:
        print(f"❌ LỖI: {e}")
        return False

if __name__ == "__main__":
    try:
        # Phân tích tham số dòng lệnh
        args = parse_arguments()
        
        # Tạo tên file đầu ra nếu không được cung cấp
        if args.output_file is None:
            base_name = os.path.splitext(args.input_file)[0]
            args.output_file = f"{base_name}-preprocessed.csv"
        
        # Xác thực tham số (Minimal output)
        validation_errors = validate_arguments(args)
        if validation_errors:
            for error in validation_errors:
                print(f"   {error}")
            input("\n❌ Có lỗi xảy ra. Nhấn Enter để đóng...")
            exit(1)
        
        # Xử lý danh sách cột xóa
        if args.delete_columns:
            columns_to_delete = [col.strip() for col in args.delete_columns.split(',')]
        else:
            columns_to_delete = None
        
        # Gọi hàm main với các tham số từ dòng lệnh
        success = main(
            input_file=args.input_file,
            output_file=args.output_file,
            chunk_size=args.chunk_size,
            date_cutoff=args.date_cutoff,
            columns_to_delete=columns_to_delete
        )
        
        if not success:
            input("\n❌ Xử lý thất bại. Nhấn Enter để đóng...")
            exit(1)
        else:
            input("\n✅ Hoàn thành! Nhấn Enter để đóng...")
            
    except KeyboardInterrupt:
        print("\n⚠️ Đã hủy bởi người dùng")
        input("\nNhấn Enter để đóng...")
        exit(1)
    except Exception as e:
        print(f"\n❌ Lỗi không mong đợi: {e}")
        input("\nNhấn Enter để đóng...")
        exit(1)