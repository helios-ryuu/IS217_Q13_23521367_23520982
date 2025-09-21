# MÔ TẢ QUY TRÌNH TIỀN XỬ LÝ DỮ LIỆU TAI NẠN GIAO THÔNG HOA KỲ

## 📋 Thông tin Dataset

- **📁 Dataset đầu vào:** `US_Accidents_March23.csv`
- **📁 Dataset đầu ra:** `US_Accidents_March23-final.csv`
- **📊 Kích thước:** 7,728,394 dòng × 46 cột (2.9 GB)
- **🎯 Mục tiêu:** Tối ưu hóa dữ liệu cho SQL Server với 6 pha xử lý

---

## 🔄 6 PHA XỬ LÝ CHI TIẾT

### PHA 1: XÓA CỘT KHÔNG CẦN THIẾT
**Loại bỏ:** `ID`, `Description`, `End_Lat`, `End_Lng`, `End_Time`, `Weather_Timestamp`
- → Giảm từ 53 xuống 47 cột, tiết kiệm bộ nhớ

### PHA 2: LỌC DỮ LIỆU THEO THỜI GIAN
Chỉ giữ dữ liệu từ **2018-2023**, loại bỏ dữ liệu cũ không nhất quán để đảm bảo chất lượng và tính cập nhật.

### PHA 3: TẠO ĐẶC TRƯNG THỜI GIAN
Tạo **7 cột thời gian** từ `Start_Time`:
- `YEAR` (2018-2023) → **SMALLINT**
- `QUARTER` (1-4) → **TINYINT**
- `MONTH` (1-12) → **TINYINT**
- `DAY` (1-31) → **TINYINT**
- `HOUR` (0-23) → **TINYINT**
- `MINUTE` (0-59) → **TINYINT**
- `SECOND` (0-59) → **TINYINT**
- `IS_WEEKEND` (0/1) → **BIT**

### PHA 4: CHUYỂN ĐỔI KIỂU DỮ LIỆU SQL SERVER

#### 🔢 NUMERIC (32 cột)
- **Tọa độ:** `DECIMAL(9,6)` - LATITUDE, LONGITUDE
- **Số thực:** `DECIMAL(8,4)` - Temperature, Distance, etc.
- **Boolean:** `BIT` - 13 cột Amenity, Bump, Crossing, etc.
- **Thời gian:** `INT/TINYINT` theo phạm vi giá trị

#### 🏷️ CATEGORICAL (15 cột)
- **Tất cả** → `NVARCHAR(100)` với NULL/NOT NULL tự động

### PHA 5: CHUẨN HÓA TÊN CỘT
- Chuyển sang **CHỮ HOA:** `source` → `SOURCE`
- Loại bỏ ký tự đặc biệt và khoảng trắng
- Chuẩn hóa tọa độ: `Start_Lat` → `LATITUDE`

### PHA 6: XÁC THỰC VÀ LÀM SẠCH
- ✅ Xác thực Severity (1-4)
- ✅ Phát hiện bản sao dựa trên tọa độ và thời gian

---

## 🎯 KẾT QUẢ VÀ LỢI ÍCH

### 📈 HIỆU SUẤT
- ✅ Giảm 62.7% kích thước file và bộ nhớ:    
    - Kích thước file gốc: 2.85 GB
    - Kích thước file sau xử lý: 1.06 GB
    - Số dòng đầu vào: 7,728,394
    - Số dòng đầu ra: 5,495,185
    - Số cột đã xóa: 6
    - Số cột đã thêm: 7
    - Tổng số cột 47
- ✅ Tối ưu hóa cho truy vấn trong SQL Server

### 📊 CHẤT LƯỢNG
- ✅ Chuẩn hóa 100% tên cột và kiểu dữ liệu
