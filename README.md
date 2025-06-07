# MovieLens Data Fragmentation Project

## Cách chạy
1. Cài PostgreSQL/MySQL.
2. Cài thư viện: `pip install -r requirements.txt`
3. Chỉnh thông tin kết nối trong `config.json`
4. Chạy `assignment_tester.py` để kiểm thử.

## Các hàm chính
- LoadRatings()
- Range_Partition()
- RoundRobin_Partition()
- RoundRobin_Insert()
- Range_Insert()

## Thành viên nhóm
- Nguyễn Văn A - MSSV...
- Trần Thị B - MSSV...
- Lê Văn C - MSSV...

## Phân công
- A: Load dữ liệu & Range_Partition
- B: RoundRobin_Partition & Insert
- C: Báo cáo & kiểm thử
