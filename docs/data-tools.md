# 与A股skill对齐的数据工具

以下为MCP工具调用参数示例，不是shell命令。实际schema由tools/list返回，工具保留旧Baostock入口的独立口径。

## 历史价格

```json
{"code":"600519","frequency":"1d","count":10,"adjust":"raw","no_cache":true,"format":"json"}
```

调用get_price_history。支持frequency=1d/1w/1M、start/end、count=1..2000、adjust=raw/qfq/hfq，默认raw。返回data与meta，注明source、adjust_requested/actual、timezone、source_errors和cache。get_historical_k_data是旧Baostock接口，其复权参数语义仍独立保留。

## 财务指标

```json
{"code":"600519","kind":"financials","as_of":"2026-07-01","count":4,"format":"json"}
```

调用get_financials。kind=financials/profit/growth/balance/cashflow/dupont；start/end/year/quarter筛选报告期，as_of筛选公告日期。缺公告日期拒绝推断；当前供应商数据不能保证恢复所有历史修订。cashflow/dupont是相关指标，不冒充完整表或可精确拆解模型。

## 行业目录、成员与指数历史

调用get_industry_data：

```json
{"action":"directory","taxonomy":"sw","level":1,"no_cache":true}
```

```json
{"action":"members","taxonomy":"sw","symbol":"801080"}
```

```json
{"action":"history","taxonomy":"sw","symbol":"801080","start":"2026-10-01","end":"2026-10-09"}
```

action=directory/members/history；taxonomy=sw/eastmoney_industry/eastmoney_concept；SW目录level=1/2/3。成员/历史使用目录返回的供应商代码或名称。返回status/data/meta，保留原始列、实际source、taxonomy、count和cache；effective_at未知为null。当前成员不等于历史成员，行业指数不等于ETF代理。

get_industry_info(code, with_boards, format="json")查东财个股行业。get_stock_industry及旧行业成员工具使用Baostock分类，不能当作同一个标准。

## 板块时效和原始资讯

get_boards_summary(mode="sub", limit=10, require_fresh=true)要求上游明确新鲜；get_board_members使用本次summary返回的group_key。get_market_news也支持require_fresh。默认保留旧快照及stale，不能把查询成功解释为今天的数据。

get_company_news(code="601899", name="紫金矿业", days=7, no_cache=true)提供原始新闻/公告候选；该工具没有新增业务分类。测试工具能力时保留完整JSON、源状态和覆盖信息，不自行筛选新闻。港美行业与披露能力由港美skill提供，当前MCP不宣称提供这些港美工具。

## 缓存与维护

历史缓存300秒；行业目录1天、行业成员/指数历史300秒。通用模块默认写src/cache，可用ASHARE_DATA_CACHE_DIR配置、ASHARE_NO_CACHE=1绕过；公司新闻使用cache/company_news及自身no_cache参数。这些仍是本地JSON文件，配置尚未统一，当前没有SQLite、自动清理或跨进程请求合并。

缓存命中保留fetched_at/cache_hit/TTL，不能用调用时间冒充数据时间。源错误、合法空结果和旧快照分别解释。scripts/sync_skill_reference.py同步canonical skill数据代码，复制哈希在src/skill_compat/manifest.json；运行不需要父目录仓库。
