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
# Preprocessing Reporter
# ==========================================

class PreprocessingReporter:
    """Tạo báo cáo quá trình tiền xử lý"""
    
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        # Tạo tên file report
        dataset_name = os.path.splitext(os.path.basename(input_file))[0]
        self.report_file = os.path.join(os.path.dirname(output_file), f"{dataset_name}-preprocess_report.txt")
        self.report_content = []
        
    def add_to_report(self, text: str):
        """Thêm nội dung vào báo cáo"""
        self.report_content.append(text)
        
    def save_report(self) -> bool:
        """Lưu báo cáo ra file"""
        try:
            with open(self.report_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(self.report_content))
            print(f"\n📄 Báo cáo tiền xử lý đã được lưu tại: {self.report_file}")
            return True
        except Exception as e:
            print(f"❌ Lỗi khi lưu báo cáo: {str(e)}")
            return False
    
    def generate_header(self):
        """Tạo header cho báo cáo"""
        self.add_to_report("📄 BÁO CÁO QUÁ TRÌNH TIỀN XỬ LÝ DỮ LIỆU")
        self.add_to_report("="*80)
        self.add_to_report(f"Ngày tạo: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.add_to_report(f"File đầu vào: {self.input_file}")
        self.add_to_report(f"File đầu ra: {self.output_file}")
        self.add_to_report("")
    
    def analyze_original_dataset(self) -> Optional[Dict]:
        """Phân tích dataset gốc"""
        if not os.path.exists(self.input_file):
            return None
        
        try:
            # Đọc mẫu dữ liệu để phân tích
            sample_df = pd.read_csv(self.input_file, nrows=10000, low_memory=False)
            total_rows = sum(1 for _ in open(self.input_file, encoding='utf-8')) - 1
            
            file_info = get_file_info(self.input_file)
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(sample_df.columns),
                'file_size': file_info,
                'column_names': list(sample_df.columns),
                'data_types': sample_df.dtypes.value_counts().to_dict(),
                'missing_values': sample_df.isnull().sum().sum(),
                'missing_percentage': (sample_df.isnull().sum().sum() / (len(sample_df) * len(sample_df.columns))) * 100,
                'duplicates': sample_df.duplicated().sum(),
                'memory_usage': sample_df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
            }
            
            return analysis
            
        except Exception as e:
            self.add_to_report(f"❌ Lỗi phân tích dataset gốc: {e}")
            return None
    
    def analyze_processed_dataset(self) -> Optional[Dict]:
        """Phân tích dataset đã xử lý"""
        if not os.path.exists(self.output_file):
            return None
        
        try:
            # Đọc mẫu dữ liệu để phân tích
            sample_df = pd.read_csv(self.output_file, nrows=10000, low_memory=False)
            total_rows = sum(1 for _ in open(self.output_file, encoding='utf-8')) - 1
            
            file_info = get_file_info(self.output_file)
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(sample_df.columns),
                'file_size': file_info,
                'column_names': list(sample_df.columns),
                'data_types': sample_df.dtypes.value_counts().to_dict(),
                'missing_values': sample_df.isnull().sum().sum(),
                'missing_percentage': (sample_df.isnull().sum().sum() / (len(sample_df) * len(sample_df.columns))) * 100,
                'duplicates': sample_df.duplicated().sum(),
                'memory_usage': sample_df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
            }
            
            return analysis
            
        except Exception as e:
            self.add_to_report(f"❌ Lỗi phân tích dataset đã xử lý: {e}")
            return None
    
    def generate_comparison_report(self, original_analysis: Dict, processed_analysis: Dict, processing_stats: Dict):
        """Tạo báo cáo so sánh chi tiết"""
        
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
                self.add_to_report(f"  + {col}")
    
    def generate_data_types_comparison(self, original_analysis: Dict, processed_analysis: Dict):
        """So sánh kiểu dữ liệu"""
        self.add_to_report(f"\n🏷️ SO SÁNH KIỂU DỮ LIỆU")
        self.add_to_report("-" * 50)
        
        # Kiểu dữ liệu gốc
        self.add_to_report("Dataset gốc:")
        for dtype, count in original_analysis['data_types'].items():
            self.add_to_report(f"  {str(dtype):15s}: {count:3d} cột")
        
        # Kiểu dữ liệu sau xử lý
        self.add_to_report("\nDataset đã xử lý:")
        for dtype, count in processed_analysis['data_types'].items():
            self.add_to_report(f"  {str(dtype):15s}: {count:3d} cột")
    
    def generate_data_quality_comparison(self, original_analysis: Dict, processed_analysis: Dict):
        """So sánh chất lượng dữ liệu"""
        self.add_to_report(f"\n🔍 SO SÁNH CHẤT LƯỢNG DỮ LIỆU")
        self.add_to_report("-" * 50)
        
        # Giá trị thiếu
        self.add_to_report("Giá trị thiếu:")
        self.add_to_report(f"  Gốc: {original_analysis['missing_values']:,} ({original_analysis['missing_percentage']:.2f}%)")
        self.add_to_report(f"  Đã xử lý: {processed_analysis['missing_values']:,} ({processed_analysis['missing_percentage']:.2f}%)")
        
        # Bản sao
        self.add_to_report(f"\nBản sao (trong mẫu):")
        self.add_to_report(f"  Gốc: {original_analysis['duplicates']:,}")
        self.add_to_report(f"  Đã xử lý: {processed_analysis['duplicates']:,}")
    
    def generate_processing_phases_detail(self):
        """Chi tiết các pha xử lý"""
        self.add_to_report(f"\n🔄 CHI TIẾT CÁC PHA TIỀN XỬ LÝ")
        self.add_to_report("-" * 50)
        
        phases = [
            ("Pha 1", "Xóa cột không cần thiết", "Loại bỏ các cột ID, Description, End_Time, v.v."),
            ("Pha 2", "Lọc dữ liệu theo ngày", "Chỉ giữ lại dữ liệu từ 2018 trở lên"),
            ("Pha 3", "Tạo đặc trưng thời gian", "Thêm các cột YEAR, MONTH, DAY, HOUR, v.v."),
            ("Pha 4", "Chuyển đổi kiểu dữ liệu SQL", "Tối ưu hóa kiểu dữ liệu cho SQL Server"),
            ("Pha 5", "Chuẩn hóa tên cột", "Chuyển tên cột thành chữ hoa và chuẩn hóa"),
            ("Pha 6", "Xác thực và làm sạch", "Loại bỏ bản sao và dữ liệu không hợp lệ")
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
        
        # So sánh kiểu dữ liệu
        self.generate_data_types_comparison(original_analysis, processed_analysis)
        
        # So sánh chất lượng
        self.generate_data_quality_comparison(original_analysis, processed_analysis)
        
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
# Preprocessing Reporter
# ==========================================

class PreprocessingReporter:
    """Tạo báo cáo quá trình tiền xử lý"""
    
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        # Tạo tên file report
        dataset_name = os.path.splitext(os.path.basename(input_file))[0]
        self.report_file = os.path.join(os.path.dirname(output_file), f"{dataset_name}-preprocess_report.txt")
        self.report_content = []
        
    def add_to_report(self, text: str):
        """Thêm nội dung vào báo cáo"""
        self.report_content.append(text)
        
    def save_report(self) -> bool:
        """Lưu báo cáo ra file"""
        try:
            with open(self.report_file, 'w', encoding='utf-8') as f:
                f.write("\n".join(self.report_content))
            print(f"\n📄 Báo cáo tiền xử lý đã được lưu tại: {self.report_file}")
            return True
        except Exception as e:
            print(f"❌ Lỗi khi lưu báo cáo: {str(e)}")
            return False
    
    def generate_header(self):
        """Tạo header cho báo cáo"""
        self.add_to_report("📄 BÁO CÁO QUÁ TRÌNH TIỀN XỪLÝ DỮ LIỆU")
        self.add_to_report("="*80)
        self.add_to_report(f"Ngày tạo: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.add_to_report(f"File đầu vào: {self.input_file}")
        self.add_to_report(f"File đầu ra: {self.output_file}")
        self.add_to_report("")
    
    def analyze_original_dataset(self) -> Optional[Dict]:
        """Phân tích dataset gốc"""
        if not os.path.exists(self.input_file):
            return None
        
        try:
            # Đọc mẫu dữ liệu để phân tích
            sample_df = pd.read_csv(self.input_file, nrows=10000, low_memory=False)
            total_rows = sum(1 for _ in open(self.input_file, encoding='utf-8')) - 1
            
            file_info = get_file_info(self.input_file)
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(sample_df.columns),
                'file_size': file_info,
                'column_names': list(sample_df.columns),
                'data_types': sample_df.dtypes.value_counts().to_dict(),
                'missing_values': sample_df.isnull().sum().sum(),
                'missing_percentage': (sample_df.isnull().sum().sum() / (len(sample_df) * len(sample_df.columns))) * 100,
                'duplicates': sample_df.duplicated().sum(),
                'memory_usage': sample_df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
            }
            
            return analysis
            
        except Exception as e:
            self.add_to_report(f"❌ Lỗi phân tích dataset gốc: {e}")
            return None
    
    def analyze_processed_dataset(self) -> Optional[Dict]:
        """Phân tích dataset đã xử lý"""
        if not os.path.exists(self.output_file):
            return None
        
        try:
            # Đọc mẫu dữ liệu để phân tích
            sample_df = pd.read_csv(self.output_file, nrows=10000, low_memory=False)
            total_rows = sum(1 for _ in open(self.output_file, encoding='utf-8')) - 1
            
            file_info = get_file_info(self.output_file)
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(sample_df.columns),
                'file_size': file_info,
                'column_names': list(sample_df.columns),
                'data_types': sample_df.dtypes.value_counts().to_dict(),
                'missing_values': sample_df.isnull().sum().sum(),
                'missing_percentage': (sample_df.isnull().sum().sum() / (len(sample_df) * len(sample_df.columns))) * 100,
                'duplicates': sample_df.duplicated().sum(),
                'memory_usage': sample_df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
            }
            
            return analysis
            
        except Exception as e:
            self.add_to_report(f"❌ Lỗi phân tích dataset đã xử lý: {e}")
            return None
    
    def generate_comparison_report(self, original_analysis: Dict, processed_analysis: Dict, processing_stats: Dict):
        """Tạo báo cáo so sánh chi tiết"""
        
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
        self.add_to_report(f"\n📊 THAY ĐỔI SAU TIỀN XỪLÝ")
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
        self.add_to_report(f"\n🔄 CHI TIẾT QUÁ TRÌNH XỪ LÝ")
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
                self.add_to_report(f"  + {col}")
    
    def generate_data_types_comparison(self, original_analysis: Dict, processed_analysis: Dict):
        """So sánh kiểu dữ liệu"""
        self.add_to_report(f"\n🏷️ SO SÁNH KIỂU DỮ LIỆU")
        self.add_to_report("-" * 50)
        
        # Kiểu dữ liệu gốc
        self.add_to_report("Dataset gốc:")
        for dtype, count in original_analysis['data_types'].items():
            self.add_to_report(f"  {str(dtype):15s}: {count:3d} cột")
        
        # Kiểu dữ liệu sau xử lý
        self.add_to_report("\nDataset đã xử lý:")
        for dtype, count in processed_analysis['data_types'].items():
            self.add_to_report(f"  {str(dtype):15s}: {count:3d} cột")
    
    def generate_data_quality_comparison(self, original_analysis: Dict, processed_analysis: Dict):
        """So sánh chất lượng dữ liệu"""
        self.add_to_report(f"\n🔍 SO SÁNH CHẤT LƯỢNG DỮ LIỆU")
        self.add_to_report("-" * 50)
        
        # Giá trị thiếu
        self.add_to_report("Giá trị thiếu:")
        self.add_to_report(f"  Gốc: {original_analysis['missing_values']:,} ({original_analysis['missing_percentage']:.2f}%)")
        self.add_to_report(f"  Đã xử lý: {processed_analysis['missing_values']:,} ({processed_analysis['missing_percentage']:.2f}%)")
        
        # Bản sao
        self.add_to_report(f"\nBản sao (trong mẫu):")
        self.add_to_report(f"  Gốc: {original_analysis['duplicates']:,}")
        self.add_to_report(f"  Đã xử lý: {processed_analysis['duplicates']:,}")
    
    def generate_processing_phases_detail(self):
        """Chi tiết các pha xử lý"""
        self.add_to_report(f"\n🔄 CHI TIẾT CÁC PHA TIỀN XỪLÝ")
        self.add_to_report("-" * 50)
        
        phases = [
            ("Pha 1", "Xóa cột không cần thiết", "Loại bỏ các cột ID, Description, End_Time, v.v."),
            ("Pha 2", "Lọc dữ liệu theo ngày", "Chỉ giữ lại dữ liệu từ 2018 trở lên"),
            ("Pha 3", "Tạo đặc trưng thời gian", "Thêm các cột YEAR, MONTH, DAY, HOUR, v.v."),
            ("Pha 4", "Chuyển đổi kiểu dữ liệu SQL", "Tối ưu hóa kiểu dữ liệu cho SQL Server"),
            ("Pha 5", "Chuẩn hóa tên cột", "Chuyển tên cột thành chữ hoa và chuẩn hóa"),
            ("Pha 6", "Xác thực và làm sạch", "Loại bỏ bản sao và dữ liệu không hợp lệ")
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
        
        # So sánh kiểu dữ liệu
        self.generate_data_types_comparison(original_analysis, processed_analysis)
        
        # So sánh chất lượng
        self.generate_data_quality_comparison(original_analysis, processed_analysis)
        
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
    
    # Chuyển đổi sang datetime nếu cần
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
    
    # Loại bỏ cột thời gian gốc để tránh dư thừa
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

def phase_validate_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Pha 6: Xác thực và làm sạch dữ liệu"""
    
    # Hàm tìm cột theo mẫu
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
         output_file: str = "../US_Accidents_March23-preprocessed.csv",
         chunk_size: int = 2600000,
         date_cutoff: str = "2018-01-01",
         columns_to_delete: List[str] = None) -> bool:
    """Hàm chính"""
    
    if columns_to_delete is None:
        columns_to_delete = ['ID', 'Description', 'End_Lat', 'End_Lng', 'End_Time', 'Weather_Timestamp']
    
    print("🚀 HỆ THỐNG TIỀN XỬ LÝ DỮ LIỆU CUỐI CÙNG")
    print(f"⏰ Bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Tạo reporter
    reporter = PreprocessingReporter(input_file, output_file)
    
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
        
        # Tạo báo cáo tiền xử lý
        print(f"\n📄 TẠO BÁO CÁO TIỀN XỬ LÝ...")
        reporter.generate_full_report(processing_stats)
        
        print(f"\n" + "="*70)
        print("✅ TIỀN XỬ LÝ HOÀN THÀNH!")
        print(f"📁 Kết quả: {output_file}")
        print(f"⏰ Hoàn thành: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"💡 Để phân tích chi tiết và tạo báo cáo SQL type conversion:")
        print(f"   python analyze_dataset.py \"{output_file}\"")
        
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