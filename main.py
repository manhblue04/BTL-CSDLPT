from src.load import LoadRatings
from src.rangePartition import Range_Partition, Range_Insert

# D:/Năm 3/Kỳ 2/Cơ sở dữ liệu phân tán/BTL-CSDLPT/tests/test_data.dat
LoadRatings("D:/Năm 3/Kỳ 2/Cơ sở dữ liệu phân tán/BTL-CSDLPT/tests/test_data.dat")  # Đường dẫn tuyệt đối

print("Dữ liệu đã được nạp vào bảng Ratings.")

Range_Partition("ratings", 4)

Range_Insert("ratings", 3, 5000, 2.5)
