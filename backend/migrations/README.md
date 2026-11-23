# 初始结构

`src/models/tables.py` 定义初始关系结构。API 启动时显式创建演示库的表；此版本包含初始 schema 与盘点账入账表 `inventory_ledger_bookings`。需要在已有部署升级表结构时，应单独引入版本化迁移。
