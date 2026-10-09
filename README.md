# duanjuku-scraper

短剧库 TG 频道采集 Worker：每天定时抓 `t.me/s/kduanju` 公开预览页的新短剧，
只收夸克链接，直接写入 D1（`resource` + `resource_disk`）。

- 定时：每天北京时间 08:37（`triggers.crons`）
- 手动：`GET /scrape?token=xxx`（需 `SCRAPE_TOKEN` Secret）
- 状态：`GET /`
- 去重：`resource.title` UNIQUE + `INSERT OR IGNORE`
- 断点：`scraper_state.last_seen_id`（D1 表，自动建）
- 通知：Server酱（`NOTICE_API` Secret，有新增/失败才推，省免费额度）
