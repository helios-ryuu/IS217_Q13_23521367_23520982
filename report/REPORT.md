## Thiết kế Dashboard cho Đồ án Phân tích Tai nạn Giao thông Hoa Kỳ

Dựa trên kiến trúc dữ liệu của bạn (Star Schema với SSAS và BigQuery), dưới đây là đề xuất cụ thể cho **3 trang báo cáo Power BI** và **3 trang báo cáo Looker Studio**, mỗi trang có 4 loại biểu đồ.

***

### **POWER BI DASHBOARDS (sử dụng SSAS Database)**

#### **Trang 1: Tổng quan Tai nạn Giao thông (Accident Overview)**

**Mục đích:** Cung cấp cái nhìn tổng thể về số liệu tai nạn, mức độ nghiêm trọng và xu hướng theo thời gian

**4 loại biểu đồ:**

1. **Card Visuals (KPI Cards)** - Hiển thị các chỉ số chính[1][2][3]
   - Total Accidents (Tổng số vụ tai nạn)
   - Average Severity (Mức độ nghiêm trọng trung bình)
   - YoY Growth % (Tăng trưởng so với năm trước)

2. **Line Chart** - Xu hướng tai nạn theo thời gian[3][4]
   - Trục X: Thời gian (Năm-Quý-Tháng)
   - Trục Y: Số lượng tai nạn
   - Màu sắc: Phân theo mức độ nghiêm trọng (Severity 1-4)
   - *MDX tham khảo: Câu 4 của bạn*

3. **Stacked Bar Chart** - Top 10 bang có nhiều tai nạn nhất[4][1]
   - Trục Y: Tên bang (STATE)
   - Trục X: Số lượng tai nạn
   - Phân tầng theo Severity
   - *MDX tham khảo: Câu 1*

4. **Matrix Table** - Bảng chi tiết theo bang và năm[3]
   - Rows: STATE
   - Columns: YEAR
   - Values: Total Accidents, AvgSeverity, DangerIndex
   - Conditional Formatting để highlight các giá trị cao

***

#### **Trang 2: Phân tích Địa điểm & Môi trường (Location & Environment Analysis)**

**Mục đích:** Phân tích ảnh hưởng của vị trí địa lý và yếu tố môi trường đến tai nạn

**4 loại biểu đồ:**

1. **Filled Map (Choropleth Map)** - Phân bố tai nạn theo bang[5][6]
   - Location: STATE (có thể drill-down đến CITY)
   - Color Saturation: Total Accidents hoặc DangerIndex
   - Tooltip: Hiển thị chi tiết STATE, CITY, Total Accidents, AvgSeverity
   - *Sử dụng LATITUDE, LONGITUDE từ DIM_LOCATION*

2. **Clustered Column Chart** - So sánh tai nạn theo yếu tố môi trường[4][3]
   - Trục X: Các yếu tố (Traffic Signal, Stop Sign, Junction, Railway, etc.)
   - Trục Y: Số lượng tai nạn
   - Legends: Có/Không (True/False)
   - *MDX tham khảo: Câu 11*

3. **Treemap** - Top thành phố theo DangerIndex[4]
   - Group: STATE > CITY
   - Size: DangerIndex
   - Color: AvgSeverity
   - *MDX tham khảo: Câu 8*

4. **Pie/Donut Chart** - Phân bố tai nạn theo Day/Night[7]
   - Segments: SUNRISE_SUNSET (Day/Night)
   - Values: Total Accidents
   - Data labels: Percentage và số lượng
   - *MDX tham khảo: Câu 5*

***

#### **Trang 3: Phân tích Thời tiết & Rủi ro (Weather & Risk Analysis)**

**Mục đích:** Đánh giá tác động của điều kiện thời tiết đến tai nạn và xác định rủi ro

**4 loại biểu đồ:**

1. **Scatter Plot** - Mối tương quan giữa điều kiện thời tiết và severity[3]
   - Trục X: TEMPERATURE (hoặc VISIBILITY)
   - Trục Y: Average Severity
   - Bubble Size: Total Accidents
   - Color: WEATHER_CONDITION categories
   - Play Axis: YEAR (để xem xu hướng theo năm)

2. **Stacked Area Chart** - Xu hướng tai nạn theo điều kiện thời tiết[4]
   - Trục X: Thời gian (Month/Quarter)
   - Trục Y: Total Accidents
   - Layers: Top 5 WEATHER_CONDITION (Fair, Cloudy, Rain, Snow, etc.)
   - *MDX tham khảo: Câu 3*

3. **Heatmap (Matrix with Conditional Formatting)** - Tai nạn theo bang và điều kiện thời tiết[8][3]
   - Rows: STATE
   - Columns: WEATHER_CONDITION
   - Values: Total Accidents
   - Color Scale: Từ xanh (thấp) đến đỏ (cao)

4. **Gauge Chart** - Chỉ số nguy hiểm theo các điều kiện[2]
   - Tạo 4 gauges riêng biệt:
     - Rain Conditions DangerIndex
     - Snow Conditions DangerIndex
     - Fog/Low Visibility DangerIndex
     - Extreme Temperature DangerIndex
   - Target: Baseline trung bình
   - Actual: Giá trị hiện tại

***

### **LOOKER STUDIO DASHBOARDS (sử dụng BigQuery)**

#### **Trang 1: Dashboard Tổng quan Hiệu suất (Performance Overview)**

**Mục đích:** Hiển thị các KPI chính và xu hướng tổng thể[9][10][11]

**4 loại biểu đồ:**

1. **Scorecard (4 cards)** - KPIs chính[10][11]
   - Total Accidents (2018-2023)
   - Total Casualties (tính từ SEVERITY * Total)
   - Average Duration (minutes)
   - States Covered (COUNT DISTINCT STATE)

2. **Time Series Chart** - Xu hướng theo thời gian[12][10]
   - Dimension: DATE (hoặc YEAR-MONTH)
   - Metrics: Total Accidents
   - Breakdown: SEVERITY levels
   - Date Range Control: Cho phép filter theo khoảng thời gian

3. **Geo Chart (Map)** - Phân bố địa lý[11][10]
   - Region: STATE
   - Color Dimension: Total Accidents per capita
   - Size Metric: DangerIndex
   - Drill-down: Click vào bang để xem chi tiết CITY

4. **Bar Chart (Horizontal)** - Top 15 thành phố nguy hiểm nhất[10][4]
   - Dimension: CITY
   - Metric: Calculated Field (SEVERITY * COUNT / 1000)
   - Sort: Descending
   - Color: By STATE for context

***

#### **Trang 2: Phân tích Hành vi & Thời gian (Temporal & Behavioral Analysis)**

**Mục đích:** Phân tích các pattern theo thời gian và hành vi

**4 loại biểu đồ:**

1. **Pivot Table** - Tai nạn theo Hour và Day of Week[11][10]
   - Rows: HOUR (0-23)
   - Columns: DAY (Mon-Sun, tính từ DATE)
   - Metrics: COUNT(accidents)
   - Heatmap Styling: Conditional formatting
   - *Tương tự Câu 4 MDX nhưng cho hourly pattern*

2. **Combo Chart** - Xu hướng YoY[10][4]
   - Primary Axis (Bars): Total Accidents by YEAR
   - Secondary Axis (Line): YoY Growth %
   - Dimension: YEAR
   - Color: Positive/Negative growth

3. **Stacked Column Chart** - Phân bố theo Quarter[10]
   - Dimension: QUARTER
   - Metrics: Total Accidents
   - Breakdown Dimension: YEAR
   - Comparison: 2018-2023 side by side

4. **Bullet Chart** - KPI Performance[10]
   - Các metrics:
     - Weekend vs Weekday Accidents (IS_WEEKEND)
     - Day vs Night (SUNRISE_SUNSET)
     - Urban vs Rural (tính từ COUNTY/CITY density)
   - Target lines: Historical averages

***

#### **Trang 3: Phân tích Yếu tố Nguy hiểm (Risk Factors Analysis)**

**Mục đích:** Phân tích các yếu tố môi trường, thời tiết ảnh hưởng đến rủi ro[6][13][7]

**4 loại biểu đồ:**

1. **Sankey Diagram** - Flow từ điều kiện thời tiết đến severity[4][10]
   - Source: WEATHER_CONDITION (nhóm top 10)
   - Target: SEVERITY (1-4)
   - Flow Width: COUNT(accidents)
   - Tooltip: % contribution

2. **Radar Chart** - Phân tích đa chiều yếu tố môi trường[4]
   - Axes: 8 yếu tố (TRAFFIC_SIGNAL, STOP, CROSSING, JUNCTION, RAILWAY, ROUNDABOUT, STATION, BUMP)
   - Values: % tai nạn có yếu tố đó (TRUE)
   - Comparison: High Severity (3-4) vs Low Severity (1-2)

3. **Waterfall Chart** - Đóng góp của các yếu tố thời tiết[10]
   - Starting Point: Baseline accidents
   - Increases/Decreases: Impact của từng WEATHER_CONDITION
   - Categories: Rain (+X%), Snow (+Y%), Fog (+Z%), etc.
   - Final: Total với weather factors

4. **Table with Sparklines** - Top nguy hiểm Streets/Roads[11][10]
   - Columns:
     - STREET name
     - STATE
     - Total Accidents
     - Trend Sparkline (2018-2023)
     - Average Severity
     - DangerIndex Score
   - Sort: By DangerIndex DESC
   - Top 20 records
   - *Tương tự Câu 6 MDX*

***

### **Các Lưu ý Thiết kế Chung**[14][1][2][3][4]

**Color Palette:**
- Severity Levels: 
  - Level 1 (Low): #4CAF50 (Green)
  - Level 2 (Moderate): #FFC107 (Amber)
  - Level 3 (High): #FF9800 (Orange)
  - Level 4 (Critical): #F44336 (Red)
- Neutral background: #F5F5F5
- Text: #212121

**Interactivity:**
- Power BI: Sử dụng Slicers cho YEAR, STATE, SEVERITY
- Looker Studio: Date Range Controls, Dropdown filters cho STATE, CITY
- Cross-filtering giữa các visuals
- Drill-through pages cho chi tiết

**Performance Optimization:**[14][1]
- Power BI: Live connection với SSAS, pre-aggregated measures
- Looker Studio: Pre-calculated tables trong BigQuery, extract mode cho datasets lớn
- Limit data points trên charts (Top N với Others)

**Mobile Responsive:**[3]
- Power BI: Tạo Mobile Layout riêng
- Looker Studio: Responsive canvas, vertical layout priority

**Calculated Fields Cần Tạo:**

**Power BI (trong SSAS Cube):**
```
AvgSeverity = [SEVERITY] / [Total Accidents]
DangerIndex = [Total Accidents] * ([AvgSeverity]^2)
YoYGrowth% = ([Current Year] - [Previous Year]) / [Previous Year]
```

**Looker Studio (trong BigQuery hoặc Calculated Fields):**
```sql
-- DangerIndex
(COUNT(*) * POWER(AVG(SEVERITY), 2))

-- YoY Growth
(COUNT_current - COUNT_previous) / COUNT_previous * 100

-- Casualty Estimate
SUM(SEVERITY * DISTANCE * DURATION / 10000)
```

Thiết kế này đảm bảo mỗi dashboard có mục đích rõ ràng, sử dụng đúng loại biểu đồ cho từng phân tích, và tận dụng tối đa dữ liệu từ schema của bạn. Các MDX queries mẫu bạn cung cấp có thể được ánh xạ trực tiếp vào Power BI measures và Looker Studio calculateted fields.[4][7][13][21]

[1](https://blog.fhyzics.net/microsoft-power-bi-training-and-consulting/the-power-bi-dashboard-design-best-practices)
[2](https://www.aufaitux.com/blog/power-bi-dashboard-design-best-practices/)
[3](https://multishoring.com/blog/power-bi-dashboard-best-practices/)
[4](https://www.eleken.co/blog-posts/dashboard-design-examples-that-catch-the-eye)
[5](https://www.arlingtonva.us/files/sharedassets/public/Transportation/Documents/Data-Dashboard-User-Guide-Feb-2021.pdf)
[6](https://www.boldbi.com/resources/dashboard-examples/government/motor-vehicle-accidents-analysis-dashboard/)
[7](https://github.com/lijesh010/roadaccidentanalysisproject)
[8](https://www.pencilandpaper.io/articles/ux-pattern-analysis-data-dashboards)
[9](https://gaillereports.com/how-to-set-theme-and-layout-in-looker-studio-and-connect-your-ga4-data/)
[10](https://www.owox.com/blog/articles/ga4-bigquery-export-building-looker-studio-dashboard)
[11](https://docs.cloud.google.com/bigquery/docs/visualize-looker-studio)
[12](https://www.mss-int.sg/v3-climate-projections/explore/climate-visualiser/atmospheric-variables/observed-changes/time-series-plots)
[13](https://databox.com/safety-kpi-dashboard)
[14](https://www.kpipartners.com/blogs/azure-analysis-services-with-power-bi-best-practices)
[15](https://www.linkedin.com/posts/sanket-chavan-10856b321_excel-dataanalysis-dashboard-activity-7371976366109421568-tB32)
[16](https://www.reddit.com/r/PowerBI/comments/1m032rk/best_practicesrecommendations_for_ui_fields_in/)
[17](https://www.youtube.com/watch?v=MtaTbeFx2Ew)
[18](https://www.geckoboard.com/dashboard-examples/operations/health-and-safety-dashboard/)
[19](https://www.elevenwriting.com/blog/how-to-build-an-attribution-dashboard-with-bigquery-and-looker-studio)
[20](https://www.youtube.com/watch?v=xzC5H0YPUr4)
[21](https://www.polestarllp.com/blog/best-practices-power-bi-dashboards)
[22](https://lookerstudio.google.com/gallery)
[23](https://github.com/harshitgahlaut/Project_Road_Accident_Dashboard_Excel)
[24](https://learn.microsoft.com/en-us/power-bi/create-reports/service-dashboards-design-tips)
[25](https://cloud.google.com/looker-studio)
[26](https://embeddable.com/blog/how-to-design-dashboards)
[27](https://metgis.com/en/metgis-pro/)
[28](https://www.bigpanda.io/blog/guide-to-incident-response-metrics-and-kpis/)
[29](https://www.visualcrossing.com/resources/documentation/weather-data/free-weather-tools-and-dashboards/)
[30](https://www.figma.com/templates/dashboard-designs/)
[31](https://www.inetsoft.com/info/kpis-on-incident-management-dashboards/)
[32](https://synopticdata.com/data-viewer/)
[33](https://dribbble.com/tags/dashboard-chart)
[34](https://www.tekmon.com/top-7-safety-key-performance-indicators-kpis)
[35](https://www.fusioncharts.com/dashboards/smart-weather-dashboard)
[36](https://community.claris.com/en/s/question/0D53w00005kIHOnCAO/how-do-i-make-a-dashboard-that-includes-more-than-one-graph-and-can-print-on-a-single-page)
[37](https://www.linkedin.com/pulse/car-accident-kpis-metrics-sovit-baral)
[38](https://openweathermap.org)
[39](https://www.onething.design/post/dashboard-design)
[40](https://www.kaggle.com/general/405419)