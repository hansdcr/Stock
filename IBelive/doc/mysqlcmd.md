# 创建数据库
```
CREATE DATABASE IF NOT EXISTS stock CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

# 查询股票代码为000001.SZ在20250101至20250927的交易数据
```
SELECT
  UNIX_TIMESTAMP(trade_date) as time,
  open,
  high,
  low,
  close,
  ts_code as metric
FROM daily_data
WHERE ts_code = '000001.SZ'
  AND trade_date BETWEEN '2025-01-01' AND '2025-09-30'
ORDER BY trade_date;
```

# mac 下数据库备份
```
/usr/local/mysql-8.0.31-macos12-arm64/bin/mysqldump -h 127.0.0.1 -P 3306 -u root -pxxxx stock > stock_backup_$(date +%Y%m%d_%H%M%S).sql
```


# Grafana中MYSQL查询条件语句
## 根据股票代码查询日线数据
```
SELECT
  trade_date as time,
  open,
  high, 
  low,
  close,
  ts_code
FROM daily_data
WHERE ts_code IN ($stock_code)
      AND trade_date BETWEEN $__timeFrom() AND $__timeTo()
ORDER BY trade_date

```
