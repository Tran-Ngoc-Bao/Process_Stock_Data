import os

# Lấy danh sách tất cả các mã chứng khoán. Có 2 phần khác biệt trong dữ liệu, phần thứ 2 nhiều hơn rất nhiều
os.system("curl https://iboard-api.ssi.com.vn/statistics/charts/defaultAllStocksV2 > test1.json")

# Lấy dữ liệu của 1 mã có chứa quá khứ
os.system("curl https://iboard-query.ssi.com.vn/exchange-index/VNINDEX?hasHistory=true > test2.json")

# Lấy dữ liệu chi tiết của 1 mã tại thời điểm hiện tại
# Rất nhiều loại mã với cấu trúc khác nhau
os.system("curl https://iboard-query.ssi.com.vn/v2/stock/group/VN30 > test3.json")