# blofin-vol-ws

Theo dõi vol 24h các hợp đồng perpetual trên BloFin qua WebSocket công khai (không cần API key).

## Cài đặt
```bash
npm install
npm install -g pm2        # nếu chưa có
```

## Cấu hình (`config.json`)
| Khóa | Ý nghĩa |
|---|---|
| `baseTokens` | Danh sách token cần theo dõi, vd `["ONDO","SOL","BTC"]` |
| `quote` | Đồng định giá, mặc định `USDT` → cặp `ONDO-USDT` |
| `logIntervalSec` | Bao lâu in bảng ra log một lần |
| `alertPct` / `alertWindowMin` | Cảnh báo khi vol 24h đổi ≥ X% trong N phút (0 = tắt) |
| `csvFile` / `csvIntervalSec` | File CSV lưu lịch sử và tần suất ghi |
| `stateFile` | File JSON số liệu mới nhất |
| `httpPort` | Cổng trang xem trực tiếp (0 = tắt) |

Sửa `config.json` khi đang chạy bằng pm2 → pm2 tự restart với danh sách mới.

## Chạy
```bash
pm2 start ecosystem.config.js
pm2 logs blofin-vol          # xem bảng cập nhật
pm2 stop blofin-vol / pm2 restart blofin-vol
pm2 save && pm2 startup      # tự chạy lại khi khởi động máy
```
Mở http://localhost:3088 để xem bảng tự cập nhật mỗi 2 giây (JSON: `/api`).

Chạy thử không cần pm2: `node index.js`
