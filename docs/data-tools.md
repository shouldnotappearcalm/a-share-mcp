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

## 公司概念和供应商入选依据

调用`get_stock_concepts`：

```json
{"code":"601899","source":"all","max_pages":5,"max_seconds":20,"no_cache":true,"format":"json"}
```

source=all/ths/eastmoney，默认all并行取两源；max_pages=1..20（默认5）、max_seconds=1..30（默认20）。THS读取常规/新兴/其他全部展示表，缺概念ID为null；东财仅IS_PRECISE=1精准概念。每条name/category/concept_code/reason/evidence_date/source/raw，THS另有url及HTML片段。reason保留供应商原文，依据日期未知不推断，不做跨源去重或主题过滤。meta.sources分别提供状态、类别数量、taxonomy/coverage_scope和分页；当前归属不代表历史成员。

## 解禁、回购和已披露股东变动

调用`get_corporate_actions`：

```json
{"kind":"unlock","start":"2026-10-12","end":"2026-11-10","limit":500,"max_pages":10,"no_cache":true}
```

```json
{"kind":"repurchase","code":"301011","no_cache":true}
```

```json
{"kind":"holder_changes","code":"603519","start":"2026-01-01","end":"2026-10-11","direction":"decrease","no_cache":true}
```

kind=unlock/repurchase/holder_changes，默认unlock。code为空时需完整start/end、最多366天；仅unlock无日期时自动使用今天起days个自然日（默认30，1..366）。给code且不设日期查源内历史，仍受上限约束；可仅给单侧日期。direction=all/increase/decrease只用于holder_changes。limit默认500（1..5000），max_pages默认10（1..50，每页100），max_seconds默认25（1..30）。日期支持YYYY-MM-DD或YYYYMMDD，包含首尾日。

每条kind/stock_code/event_date/source/raw。meta.total/upstream_pages/pages_fetched/count/errors/coverage_limited说明是否取全。股票/日期/方向在服务端过滤，不扫描全市场再找单票。

| 类型 | event_date/date_basis | raw及口径 |
|---|---|---|
| unlock | FREE_DATE | CURRENT_FREE_SHARES/FREE_SHARES/ABLE_FREE_SHARES保持万股，不乘10000；未来FREE_DATE是计划可流通日，不能视为实际已解禁 |
| repurchase | UPDATEDATE，披露更新日 | JEXX/JESX计划金额与REPURAMOUNT/REPURNUM实际执行分开；DIM_TRADEDATE/REPURSTARTDATE/REPURENDDATE/FINISHDATE保留；多次计划不合并 |
| holder_changes | NOTICE_DATE，公告日 | HOLDER_NAME/DIRECTION/CHANGE_NUM/CHANGE_RATE原值；START_DATE/END_DATE是变动期间，不等于公告日，也不是完整未来减持计划 |

回购progress_label只解释REPURPROGRESS原始枚举：001董事会预案、002股东大会通过、003否决、004实施中、005停止、006完成。未知为null，不按日期或金额推断。除注明的解禁股数单位外，其余字段保持供应商原值，空值不置零。

## 涨停和炸板原始详情

调用`get_market_pool_details`：

```json
{"kind":"broken","date":"20261009","source":"eastmoney","market":"all","no_cache":true}
```

```json
{"kind":"up","date":"20261009","source":"ths","market":"shsz","no_cache":true}
```

kind=up/broken（默认up）；source=auto/eastmoney/ths（默认auto）；market=all/shsz（默认all）。limit默认500（1..5000）、max_pages默认10（1..50）、max_seconds默认20（1..30）。东财单次至多10000条，THS每页200条；源数量不符、分页或limit截断标partial。

每条stock_code/source/trade_date/raw。东财raw保留fbt/lbt封板时间（HHMMSS整数）、zbc炸板次数、lbc连板数、fund封单、hybk行业、zttj统计等所有字段；价格p为0.001元，不静默换算。THS保留Unix秒时间、open_num/order_amount/reason_type/high_days等原始字段，逐行校验时间属于返回交易日。

省略date时允许休市回退，始终返回实际trade_date；显式日期与源返回不符时默认拒绝，allow_date_fallback=true才接受并标记date_fallback。旧日期无快照可能empty、trade_date=null、date_verified=false，不能推断当日没有涨停，无法保证任意历史日期。

东财qdate是最新市场日标记，不直接当作历史池日期；provider_qdate/provider_meta保留源元数据。历史池用有日期和行时间的THS沪深共同成分集合交叉确认，休市回退用东财最新日期的完整raw快照确认。date_check记录方法；历史校验不逐只验证北交所时间。校验失败保留raw、返回partial且trade_date=null/date_verified=false，历史东财查询可能多一次校验请求。

THS源HS,GEM2STAR不含北交所。source=ths须market=shsz或allow_partial_scope=true；auto默认all仅用东财，选shsz或显式allow_partial_scope才允许THS降级。all降级THS返回partial/scope_limited=true；meta.source_attempts保留前源错误。provider_market/market/requested_market区分来源和请求范围；total是源总数、selected_total是已取记录经显式市场选择后的数量、count是实际返回数。

## 龙虎榜买卖席位

调用`get_lhb_seats`：

```json
{"code":"002317","date":"20261009","max_pages":10,"max_seconds":20,"no_cache":true}
```

code/date必填；max_pages=1..50，默认10（每侧每页100）；max_seconds=1..30，默认20。BUY/SELL两侧并行，使用现行协议字段，不使用已失效的BUY_AMT_REAL/SELL_AMT_REAL排序。每条side/stock_code/trade_date/source/raw，raw保留BUY/SELL/NET（元）、TOTAL_BUYRIO/TOTAL_SELLRIO（百分比）、营业部、EXPLANATION/TRADE_ID。相同营业部可因不同原因上榜，不跨原因去重。meta.sides逐侧提供分页和失败；仅一侧可用时partial。

## 新入口的共同状态和缓存

四个工具默认且仅支持format=json，返回status/meta/data，保留所有供应商字段于raw。ok表示本次所选范围取全；empty表示合法无记录/快照；partial保留可用记录并明确覆盖受限；unavailable表示无可用响应。HTTP200中的success=false不是空结果；输入校验错误由MCP包装为status=error。未知值为null，外部文本不作为执行指令。

概念TTL300秒，公司事件/席位120秒，池30秒；仅ok/empty写入缓存，partial/unavailable不缓存、不自动用过期值掩盖失败。no_cache=true绕过读写，ASHARE_NO_CACHE=1也有效。fetched_at为首次取数时间，可带UTC时区，不能代替交易日或公告日。日期窗口按Asia/Shanghai解释。缓存键包含股票、类型、来源、日期窗、市场、上限和回退选项等会影响结果的入参。

本轮实现参考stock-api的能力组织、AKShare的现行协议及公开原始响应，无Node/stock-sdk服务依赖。AKShare和stock-sdk若都用东财则不构成独立容灾。2026-10-11休市时验证通过不代表长期盘中高频稳定性。
