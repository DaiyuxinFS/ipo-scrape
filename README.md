# 港交所 IPO 爬虫

每日爬取港交所主板新上市信息，写入 MySQL 数据库并推送 n8n webhook。

## 功能

- 每日自动爬取港交所主板新上市信息
- 存储 IPO 信息到 MySQL 数据库
- 自动更新股份配发结果 URL
- 推送新上市公告到 n8n webhook
- 定时任务：北京时间每天 20:40 执行

## 部署到 Zeabur

### 1. 准备文件

确保以下文件存在：
- `main.py` - 主程序
- `requirements.txt` - Python 依赖
- `Procfile` - 启动命令

### 2. 配置环境变量

在 Zeabur 项目设置中配置以下环境变量：

```
MYSQL_HOST=sjc1.clusters.zeabur.com
MYSQL_PORT=21517
MYSQL_USER=root
MYSQL_PASSWORD=你的MySQL密码
MYSQL_DB=scrape
WEBHOOK_URL=https://n8n.imixtu.re/webhook/1aa73d9a-6615-4480-a28a-29968f399bb5
ANNOUNCEMENT_WEBHOOK_URL=https://n8n.imixtu.re/webhook/ipo-new
```

### 3. 部署步骤

1. 将代码推送到 Git 仓库（GitHub/GitLab）
2. 在 Zeabur 中创建新项目
3. 连接到你的 Git 仓库
4. 选择服务类型为 **Worker**（不是 Web Service）
5. 配置环境变量
6. 部署

### 4. 验证部署

部署成功后，查看日志确认：
- 脚本是否正常启动
- 数据库连接是否成功
- 调度器是否正常启动

## 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 配置环境变量
export MYSQL_PASSWORD=你的密码
export WEBHOOK_URL=你的webhook地址

# 运行
python main.py
```

## 数据库表结构

```sql
CREATE TABLE ipo (
    id VARCHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    announcement_url TEXT,
    allotment_url TEXT,
    announcement_sent TINYINT(1) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DEFAULT CHARSET=utf8mb4;
```

## 注意事项

- 确保 MySQL 数据库已创建并配置正确
- n8n webhook URL 需要可访问
- 脚本会在启动时立即执行一次，然后按计划定时执行

