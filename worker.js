/**
 * duanjuku-scraper —— 短剧库 TG 频道采集 Worker
 *
 * 每天定时抓取 https://t.me/s/kduanju 公开预览页的新短剧消息，
 * 只收标题以"短剧"开头的单剧消息和"X月X日短剧更新汇总"（拆多行），
 * 只收夸克链接，写入 D1（resource + resource_disk，与现有导入格式一致）。
 *
 * 触发方式：
 *   - 定时：wrangler.jsonc triggers.crons（每天北京时间 08:37）
 *   - 手动：GET /scrape?token=xxx（需配 SCRAPE_TOKEN Secret）
 *   - 状态：GET /
 *
 * Secret（Cloudflare 控制台 → Worker → Settings → Variables and Secrets）：
 *   NOTICE_API    Server酱推送地址（https://sctapi.ftqq.com/{SendKey}.send）
 *   SCRAPE_TOKEN  手动触发口令（可选，不配则禁用手动触发）
 *
 * D1：绑定到 duanjuku 数据库（binding 名 DB）。
 * 状态表 scraper_state 首次运行自动建。
 */

const CHANNEL = "kduanju";
const BASE_URL = `https://t.me/s/${CHANNEL}`;
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const BJ_OFFSET = 8 * 3600 * 1000;

const MAX_PAGES_PER_RUN = 60; // 单次最多往回翻 60 页，防异常
const SEED_PAGES_FIRST_RUN = 8; // 首次运行（无状态）只取最近 8 页做种子

const RE_POST = /data-post="kduanju\/(\d+)"/;
const RE_TIME = /<time datetime="([^"]+)"/;
const RE_QUARK = /https?:\/\/pan\.quark\.cn\/s\/[A-Za-z0-9]+/;
const RE_TAG = /<[^>]+>/g;
const RE_BR = /<br\s*\/?>/gi;
const RE_HUIZONG_TITLE = /^\d{1,2}月\d{1,2}日短剧更新汇总/;
const RE_HUIZONG_ITEM = /^\d{1,3}[.、．]\s*(.+?)\s*$/;

/** 最小 HTML 实体解码（标题里偶尔出现） */
function unescapeHtml(s) {
  return String(s)
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)));
}

function cleanText(fragment) {
  return unescapeHtml(fragment.replace(RE_BR, "\n").replace(RE_TAG, "").trim());
}

async function fetchPage(url) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 30000);
  let resp;
  try {
    resp = await fetch(url, { headers: { "User-Agent": UA }, signal: ctrl.signal });
  } catch (e) {
    clearTimeout(timer);
    throw new Error(`NET_FAIL:抓取失败 ${url} (${String((e && e.message) || e).slice(0, 80)})`);
  }
  clearTimeout(timer);
  if (!resp.ok) throw new Error(`HTTP_${resp.status}:抓取失败 ${url}`);
  return await resp.text();
}

/** 解析一页，返回按页面顺序（旧 -> 新）的消息列表 */
function parsePage(html) {
  const posts = [];
  const parts = html.split('<div class="tgme_widget_message_wrap js-widget_message_wrap">');
  for (let i = 1; i < parts.length; i++) {
    const part = parts[i];
    const m = part.match(RE_POST);
    if (!m) continue;
    const pid = parseInt(m[1], 10);
    const t = part.match(RE_TIME);
    let dt = null;
    if (t) {
      const d = new Date(t[1]);
      if (!isNaN(d.getTime())) dt = d;
    }
    const b = part.match(/js-message_text".*?>(.*?)<\/div>\s*$/s);
    const text = b ? cleanText(b[1]) : "";
    posts.push({ pid, dt, text });
  }
  return posts;
}

/** 从一条消息提取短剧行（可能多行） */
function extractRows(post) {
  const lines = post.text.split("\n").map((l) => l.trim()).filter(Boolean);
  if (!lines.length) return [];
  const first = lines[0].replace(/^\s+/, "");
  const qm = post.text.match(RE_QUARK);
  if (!qm) return [];
  const quark = qm[0];
  let dateBj = "";
  if (post.dt) dateBj = new Date(post.dt.getTime() + BJ_OFFSET).toISOString().slice(0, 10);
  const desc = dateBj ? `短剧 · 频道更新于${dateBj}` : "短剧";

  const rows = [];
  if (first.startsWith("短剧")) {
    const title = first.replace(/^短剧[．.．:：、\s]*/, "").trim();
    if (title) rows.push({ title, quark, desc });
  } else if (RE_HUIZONG_TITLE.test(first)) {
    for (let i = 1; i < lines.length; i++) {
      const m = lines[i].match(RE_HUIZONG_ITEM);
      if (!m) continue;
      const title = m[1].trim();
      if (title) rows.push({ title, quark, desc });
    }
  }
  return rows;
}

/** Server酱微信推送（未配置则静默跳过） */
async function sendNotice(noticeApi, title, desp) {
  if (!noticeApi) return;
  try {
    if (String(noticeApi).includes("sctapi.ftqq.com")) {
      const form = new URLSearchParams();
      form.set("title", title);
      form.set("desp", desp);
      await fetch(noticeApi, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: form.toString(),
      });
    }
  } catch (e) {
    console.error("notice fail:", e);
  }
}

async function getState(db) {
  await db.exec(
    "CREATE TABLE IF NOT EXISTS scraper_state (`key` TEXT PRIMARY KEY, value TEXT NOT NULL)"
  );
  const row = await db
    .prepare("SELECT value FROM scraper_state WHERE `key`='last_seen_id'")
    .first();
  return row ? parseInt(row.value, 10) || 0 : 0;
}

async function setState(db, lastSeenId) {
  await db
    .prepare(
      "INSERT INTO scraper_state (`key`, value) VALUES ('last_seen_id', ?) " +
        "ON CONFLICT(`key`) DO UPDATE SET value=excluded.value"
    )
    .bind(String(lastSeenId))
    .run();
}

/** 主采集流程，返回统计 */
async function scrape(db) {
  const sinceId = await getState(db);
  const firstRun = sinceId <= 0;
  const pageCap = firstRun ? SEED_PAGES_FIRST_RUN : MAX_PAGES_PER_RUN;

  let maxPid = sinceId;
  let before = null;
  let pages = 0;
  let inserted = 0;
  let skippedDup = 0;
  let skippedFiltered = 0;
  const seenTitles = new Set();

  while (pages < pageCap) {
    const url = BASE_URL + (before ? `?before=${before}` : "");
    const posts = parsePage(await fetchPage(url));
    if (!posts.length) break;
    pages++;

    let reachedOld = false;
    for (const p of posts) {
      if (p.pid > maxPid) maxPid = p.pid;
      if (!firstRun && p.pid <= sinceId) {
        reachedOld = true;
        continue;
      }
      const items = extractRows(p);
      if (!items.length) {
        skippedFiltered++;
        continue;
      }
      for (const item of items) {
        if (seenTitles.has(item.title)) {
          skippedDup++;
          continue;
        }
        seenTitles.add(item.title);
        // title 有 UNIQUE 约束，INSERT OR IGNORE 天然去重
        const r = await db
          .prepare(
            "INSERT OR IGNORE INTO resource " +
              "(category_key, pinyin, title, `desc`, cover, disk_type, url, hot_num, is_show_home) " +
              "VALUES ('duanju', '', ?, ?, '', '夸克', ?, 0, 0)"
          )
          .bind(item.title, item.desc.slice(0, 100), item.quark)
          .run();
        if (r.meta.changes > 0) {
          const rid = r.meta.last_row_id;
          // pinyin 用 id 兜底（Worker 里不引入拼音库，数字 ID 详情页同样支持）
          await db
            .prepare("UPDATE resource SET pinyin = CAST(? AS TEXT) WHERE id = ?")
            .bind(rid, rid)
            .run();
          await db
            .prepare(
              "INSERT INTO resource_disk (resource_id, disk_type, external_url, url) " +
                "VALUES (?, '夸克', ?, '')"
            )
            .bind(rid, item.quark)
            .run();
          inserted++;
        } else {
          skippedDup++;
        }
      }
    }
    if (reachedOld) break;
    before = posts[0].pid; // 本页最旧的一条，继续往回翻
  }

  await setState(db, maxPid);
  return { inserted, skippedDup, skippedFiltered, pages, maxPid, firstRun };
}

const json = (code, obj) =>
  new Response(JSON.stringify(obj), {
    status: code,
    headers: { "Content-Type": "application/json" },
  });

export default {
  /** 每天定时执行（见 wrangler.jsonc triggers.crons） */
  async scheduled(event, env, ctx) {
    const noticeApi = (env.NOTICE_API || "").trim();
    const now = new Date(Date.now() + BJ_OFFSET).toISOString().slice(0, 19).replace("T", " ");
    try {
      const r = await scrape(env.DB);
      console.log(`scrape ok @ ${now}:`, JSON.stringify(r));
      // Server酱免费版每天 5 条：有新增或失败才推送
      if (r.inserted > 0) {
        await sendNotice(
          noticeApi,
          `短剧库采集：新增 ${r.inserted} 部`,
          `时间：${now}（北京时间）\n新增入库：${r.inserted} 部\n去重跳过：${r.skippedDup}\n翻页：${r.pages}`
        );
      }
    } catch (e) {
      const msg = String((e && e.message) || e).slice(0, 300);
      console.error(`scrape fail @ ${now}:`, msg);
      await sendNotice(noticeApi, "短剧库采集失败", `时间：${now}（北京时间）\n错误：${msg}`);
    }
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    const noticeApi = (env.NOTICE_API || "").trim();

    if (url.pathname === "/scrape") {
      const token = (env.SCRAPE_TOKEN || "").trim();
      if (!token || url.searchParams.get("token") !== token) {
        return json(403, { message: "token 无效（需配 SCRAPE_TOKEN Secret）" });
      }
      try {
        const r = await scrape(env.DB);
        return json(200, { ok: true, ...r });
      } catch (e) {
        const msg = String((e && e.message) || e).slice(0, 300);
        await sendNotice(noticeApi, "短剧库采集失败（手动）", `错误：${msg}`);
        return json(500, { ok: false, message: msg });
      }
    }

    // 状态页
    try {
      const lastSeen = await getState(env.DB);
      const total = await env.DB.prepare("SELECT COUNT(*) AS c FROM resource").first();
      return json(200, {
        ok: true,
        channel: CHANNEL,
        last_seen_id: lastSeen,
        resource_total: total ? total.c : 0,
        hint: "手动触发：/scrape?token=xxx",
      });
    } catch (e) {
      return json(500, { ok: false, message: String((e && e.message) || e).slice(0, 200) });
    }
  },
};
