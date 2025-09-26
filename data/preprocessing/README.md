# 🚀 Hướng Dẫn Sử Dụng Preprocess.py

## 📋 Mô Tả
Script tiền xử lý dữ liệu tự động cho SQL Server Data Warehouse, hỗ trợ xử lý file CSV lớn theo khối (chunked processing). Hệ thống được tối ưu hóa để xử lý dataset lớn (hàng triệu bản ghi) một cách hiệu quả với báo cáo chi tiết về quá trình xử lý.

## 🔧 Cài Đặt Dependencies
```bash
pip install -r requirements.txt
```

**Dependencies chính:**
- `pandas` - Xử lý dữ liệu
- `numpy` - Tính toán số học
- `tqdm` - Thanh tiến trình
- Các thư viện hỗ trợ khác trong `requirements.txt`

## 💻 Cách Sử Dụng

### ✅ Cách sử dụng cơ bản
```bash
python preprocess.py ../US_Accidents_March23.csv
```

### ⚙️ Các tùy chọn nâng cao

#### 📁 Chỉ định file đầu ra
```bash
python preprocess.py input.csv -o output_processed.csv
```

#### 🔢 Tùy chỉnh kích thước khối xử lý
```bash
python preprocess.py data.csv -c 1000000
# Khối 1 triệu dòng (thay vì 2.6 triệu mặc định)
```

#### 📅 Thay đổi ngày cắt lọc
```bash
python preprocess.py data.csv -d 2020-01-01
# Chỉ giữ dữ liệu từ 2020 trở lên (mặc định: 2018-01-01)
```

#### 🗑️ Tùy chỉnh cột cần xóa
```bash
python preprocess.py data.csv --delete-columns "ID,Country,Description,Custom_Column"
```

#### 📊 Chế độ verbose (chi tiết)
```bash
python preprocess.py data.csv -v
```

### 🔗 Kết hợp nhiều tùy chọn
```bash
python preprocess.py large_data.csv \
  -o processed_data.csv \
  -c 500000 \
  -d 2019-01-01 \
  --delete-columns "ID,Country,Weather_Timestamp" \
  -v
```

## 📊 Các Pha Xử Lý Chi Tiết

### **Pha 1: Xóa cột không cần thiết** 🗑️
- **Mục đích**: Loại bỏ các cột không cần thiết cho Data Warehouse
- **Cột mặc định xóa**: `ID`, `Description`, `End_Lat`, `End_Lng`, `End_Time`, `Weather_Timestamp`, `Country`
- **Lý do**: Giảm kích thước dữ liệu, loại bỏ thông tin dư thừa hoặc không phù hợp cho phân tích
- **Tùy chỉnh**: Sử dụng `--delete-columns` để chỉ định danh sách cột khác

### **Pha 2: Lọc dữ liệu theo ngày** 📅
- **Mục đích**: Lọc dữ liệu theo khoảng thời gian mong muốn
- **Mặc định**: Chỉ giữ dữ liệu từ 2018-01-01 trở lên
- **Cột áp dụng**: `Start_Time` 
- **Lý do**: Tập trung vào dữ liệu gần đây, loại bỏ dữ liệu cũ có thể không đầy đủ
- **Tùy chỉnh**: Sử dụng `-d` hoặc `--date-cutoff` để thay đổi ngày cắt

### **Pha 3: Tạo đặc trưng thời gian** ⏰
- **Mục đích**: Tạo các dimension thời gian cho Data Warehouse
- **Các cột được tạo**:
  - `YEAR` (int16) - Năm
  - `QUARTER` (int8) - Quý (1-4)
  - `MONTH` (int8) - Tháng (1-12)
  - `DAY` (int8) - Ngày (1-31)
  - `HOUR` (int8) - Giờ (0-23)
  - `MINUTE` (int8) - Phút (0-59)
  - `SECOND` (int8) - Giây (0-59)
  - `IS_WEEKEND` (bool) - True nếu là cuối tuần
- **Lợi ích**: Hỗ trợ phân tích theo thời gian, tạo dashboard theo các khoảng thời gian khác nhau

### **Pha 4: Chuyển đổi kiểu dữ liệu SQL Server** 🔄
- **Mục đích**: Tối ưu hóa kiểu dữ liệu cho SQL Server
- **Mapping chi tiết**:
  - **Tọa độ** (`Start_Lat`, `Start_Lng`, `LATITUDE`, `LONGITUDE`) → `decimal(9,6)`
  - **Số thực khác** → `decimal(8,4)`
  - **Năm** → `smallint`
  - **Tháng, ngày, giờ, phút, giây, quý** → `tinyint`
  - **Số nguyên lớn** → `int`
  - **Boolean** → `bit` (qua bool type)
  - **Chuỗi** → `nvarchar` (tối ưu kích thước)
- **Lợi ích**: Giảm dung lượng database, tăng hiệu suất query, đảm bảo tương thích SQL Server

### **Pha 5: Chuẩn hóa tên cột** 📝
- **Mục đích**: Chuẩn hóa tên cột theo convention SQL Server
- **Quy tắc**:
  - Chuyển tất cả về CHỮ HOA
  - Thay khoảng trắng bằng dấu gạch dưới (`_`)
  - Loại bỏ ký tự đặc biệt, dấu ngoặc
  - Mapping đặc biệt: `START_LAT` → `LATITUDE`, `START_LNG` → `LONGITUDE`
- **Lợi ích**: Nhất quán naming convention, dễ sử dụng trong SQL queries

### **Pha 6: Sắp xếp thứ tự cột theo DDL** 📋
- **Mục đích**: Sắp xếp cột theo thứ tự logic trong Data Warehouse schema
- **Thứ tự ưu tiên**:
  1. **Fact Attributes**: `SEVERITY`, `DISTANCE`
  2. **Source Dimension**: `SOURCE`
  3. **Time Dimension**: `YEAR`, `QUARTER`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `IS_WEEKEND`
  4. **Location Dimension**: `STATE`, `COUNTY`, `CITY`, `STREET`, `ZIPCODE`, `AIRPORT_CODE`, `TIMEZONE`, `LATITUDE`, `LONGITUDE`
  5. **Weather Dimension**: `TEMPERATURE`, `WIND_CHILL`, `HUMIDITY`, `PRESSURE`, `VISIBILITY`, `WIND_DIRECTION`, `WIND_SPEED`, `PRECIPITATION`, `WEATHER_CONDITION`, `SUNRISE_SUNSET`, `CIVIL_TWILIGHT`, `NAUTICAL_TWILIGHT`, `ASTRONOMICAL_TWILIGHT`
  6. **Environment Dimension**: `AMENITY`, `BUMP`, `CROSSING`, `GIVE_WAY`, `JUNCTION`, `NO_EXIT`, `RAILWAY`, `ROUNDABOUT`, `STATION`, `STOP`, `TRAFFIC_CALMING`, `TRAFFIC_SIGNAL`, `TURNING_LOOP`
- **Lợi ích**: Dễ import vào SQL Server, khớp với DDL schema, dễ maintain

## 📈 Kết Quả & Báo Cáo

### 📄 File đầu ra
- **File CSV tối ưu hóa**: Dataset đã được xử lý, sẵn sàng cho SQL Server
- **Báo cáo chi tiết** (`.txt`): Phân tích đầy đủ quá trình xử lý

### 📊 Thống kê điển hình (US Accidents Dataset)
```
📁 FILE:
  Gốc: 2.85 GB → Xử lý: 1.42 GB (giảm 50.2%)
📏 DỮ LIỆU:
  Dòng: 7,728,394 → 5,857,117 (giảm 24.2%)
  Cột: 46 → 46 (7 cột xóa + 8 cột thời gian thêm)
  Khối xử lý: 3 khối (2.6M dòng/khối)
```

### 📋 Nội dung báo cáo
- **Thông tin cấu hình**: Tham số đầu vào, file paths
- **So sánh before/after**: Kích thước, số dòng, kiểu dữ liệu
- **Chi tiết từng pha**: Kết quả xử lý từng bước
- **Thống kê tối ưu hóa**: Tỷ lệ giảm dung lượng, hiệu suất

## 🎯 Đặc Điểm Nổi Bật

### ⚡ Hiệu Suất Cao
- **Chunked Processing**: Xử lý theo khối để tiết kiệm RAM
- **Kích thước khối tùy chỉnh**: Mặc định 2.6M dòng, có thể điều chỉnh
- **Progress Bar**: Theo dõi tiến trình real-time với `tqdm`
- **Minimal Output**: Giảm spam console, tập trung vào kết quả

### 🛠️ Tối Ưu Hóa SQL Server
- **Type Optimization**: Mapping chính xác kiểu dữ liệu SQL Server
- **Column Order**: Thứ tự cột khớp với DDL schema
- **Data Validation**: Kiểm tra và làm sạch dữ liệu
- **Size Reduction**: Giảm 50%+ dung lượng sau xử lý

### 📊 Báo Cáo Chi Tiết
- **Preprocessing Report**: Phân tích đầy đủ quá trình
- **Before/After Comparison**: So sánh chi tiết dataset
- **Performance Metrics**: Thống kê hiệu suất, tối ưu hóa
- **Error Handling**: Ghi log lỗi và cảnh báo

## ❓ Trợ Giúp & Troubleshooting

### 💡 Xem trợ giúp
```bash
python preprocess.py --help
```

### 🔍 Kiểm tra phiên bản
```bash
python preprocess.py --version
```

### 🐛 Các lỗi thường gặp

#### ❌ "File không tồn tại"
```bash
# Đảm bảo đường dẫn file chính xác
python preprocess.py "path/to/your/file.csv"
```

#### ❌ "Memory Error"
```bash
# Giảm kích thước chunk
python preprocess.py data.csv -c 1000000
```

#### ❌ "Date parsing error"
```bash
# Kiểm tra format ngày (YYYY-MM-DD)
python preprocess.py data.csv -d "2020-01-01"
```

### 📋 Requirements
- **Python**: 3.7+
- **RAM**: Tối thiểu 4GB (khuyến nghị 8GB+)
- **Disk Space**: 2-3x kích thước file gốc
- **Dependencies**: Xem `requirements.txt`

## ⚠️ Lưu Ý Quan Trọng

### 🔒 An Toàn Dữ Liệu
- **Backup dữ liệu gốc** trước khi xử lý
- Kiểm tra **đủ dung lượng disk** cho file đầu ra
- **Không ghi đè** file gốc (sử dụng tên khác)

### 🚀 Hiệu Suất
- File đầu vào **phải là CSV** với encoding UTF-8
- **Chunk size** phụ thuộc vào RAM available
- **Thời gian xử lý** tỷ lệ với kích thước dataset
- Sử dụng **SSD** để tăng tốc I/O

### 🎛️ Tùy Chỉnh
- Tất cả **tham số đều có thể tùy chỉnh** qua command line
- **Logging level** có thể điều chỉnh với `-v`
- **Cột xóa** có thể chỉ định custom list
- **Date filtering** linh hoạt theo nhu cầu

---

## 🏆 Thành Tựu Đạt Được

✅ **Xử lý thành công** dataset 7.7M+ records  
✅ **Giảm 50%+ dung lượng** file  
✅ **Tối ưu hóa** cho SQL Server Data Warehouse  
✅ **Báo cáo chi tiết** quá trình xử lý  
✅ **Error handling** robust  
✅ **Performance optimization** cao  

---

*💻 Phát triển bởi: IS217 - Data Warehouse Team*  
*📅 Cập nhật: September 2025*