# 🚀 Hướng Dẫn Sử Dụng Preprocess.py

## 📋 Mô Tả
Script tiền xử lý dữ liệu tự động cho SQL Server Data Warehouse, hỗ trợ xử lý file CSV lớn theo khối.

## 🔧 Cài Đặt Dependencies
```bash
pip install -r requirements.txt
```

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
# Chỉ giữ dữ liệu từ 2020 trở lên
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

## 📊 Các Pha Xử Lý

1. **Pha 1**: Xóa cột không cần thiết
2. **Pha 2**: Lọc dữ liệu theo ngày
3. **Pha 3**: Tạo đặc trưng thời gian
4. **Pha 4**: Chuyển đổi kiểu dữ liệu SQL
5. **Pha 5**: Chuẩn hóa tên cột
6. **Pha 6**: Sắp xếp thứ tự cột theo DDL

## 📈 Kết Quả

- File CSV đã được tối ưu hóa cho SQL Server
- Báo cáo chi tiết quá trình xử lý (`.txt`)
- Thứ tự cột khớp với DDL schema
- Kiểu dữ liệu tương thích SQL Server

## ❓ Trợ Giúp
```bash
python preprocess.py --help
```

## 🔍 Phiên Bản
```bash
python preprocess.py --version
```

## ⚠️ Lưu Ý

- File đầu vào phải là CSV
- Cần đủ RAM để xử lý chunk
- Thời gian xử lý phụ thuộc vào kích thước file
- Backup dữ liệu gốc trước khi xử lý