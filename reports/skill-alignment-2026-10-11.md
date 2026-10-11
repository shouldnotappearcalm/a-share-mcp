# a-share-mcp 与 a-share-skill 对齐（2026-10-11）

参照 a-share-skill 提交 `914ee8492e27ec49d4506cf7fc6810def6bee7bd`，优先对齐数据来源、取数函数、代码语义、字段和新鲜度，而不是只让工具名称相似。

## 对齐矩阵

| skill入口 | MCP入口 | 对齐内容 |
|---|---|---|
| fetch_realtime 股票规范 | AshareDataSource.normalize_code | 同函数体；裸000001按股票，指数显式sh000001 |
| fetch_realtime 分钟聚合 | get_realtime_quote / get_intraday_minute_kline | 同未完成K线过滤/聚合函数；休市使用展示日的前收 |
| fetch_history 历史链路 | get_price_history | 同腾讯→新浪→雪球→东财函数，同行字段、日期过滤、条数、preclose/pctChg |
| fetch_history 财务 | get_financials | 同现行东财→新浪摘要、同报告期过滤与财务维度字段 |
| fetch_history --industry | get_industry_info | 同东财公司概况→quote回退，with_boards默认关闭 |
| fetch_danginvest --summary | get_boards_summary / 旧get_hot_sectors | 同行业/概念维度、参数、原始items和stale元信息 |
| fetch_danginvest --detail | get_board_members | 同groupKey、分页与成分原始数据 |
| fetch_danginvest --news | get_market_news | 同市场快讯、24小时新鲜度标记 |
| fetch_company_news | get_company_news | 同未分类候选、公告双源、时间窗口、缓存与来源诊断 |

MCP保持结构化接口；CLI成功返回裸数组时，MCP仍包装data/meta，但数据部分保持一致。原始JSON入口不会做额外业务筛选。板块概览、板块成分与快讯本轮都返回stale=true，原样保留，不能称最新数据。

## 维护方式

- `src/skill_compat/history.py`、`danginvest.py` 由 `scripts/sync_skill_reference.py` 同步选择的数据函数。CLI print分支转换为字典return，前面的payload计算保持相同。
- `src/company_news.py` 同步原新闻模块至build_payload，去掉CLI main。实时规范和分钟处理同步原函数体。
- `src/skill_compat/manifest.json` 记录来源提交、文件哈希、选取函数和生成文件哈希，回归验证生成文件未漂移。
- 运行时不导入旁目录的a-share-skill；模块包含在MCP仓库内，独立复制/安装仍可启动。

更新参照版本时，在MCP仓库根目录显式运行：

```bash
python scripts/sync_skill_reference.py --source ../a-share-skill
RUN_LIVE_TESTS=1 uv run pytest -q
```

同步后仍须复核原始输出，不把“生成成功”当接口验收。同步脚本已重复运行验证。

## 明确保留的差异

- `get_historical_k_data` 保留Baostock复权/字段兼容；默认skill价格路径为新`get_price_history`。skill自身的源默认价也有差异：腾讯前复权、新浪原始价；MCP标明实际price_basis，不伪造统一复权保证。
- `get_profit_data/get_cash_flow_data/...` 等旧Baostock财务工具保留来源/字段/比例单位，不能拿它们与skill指标切片冒充同一数据。常规skill财务指标使用`get_financials`。
- `get_stock_industry/list_industries/get_industry_members`保留Baostock证监会分类；东财单票行业使用`get_industry_info`，不同分类体系不强行合并。
- `get_hot_sectors`名称保留，但JSON已与skill的DangInvest概念概览一致，字段为groupLabel/changePct等；旧板块字段消费者需调整。
- MCP实时/市场工具上层等待默认35秒（A_SHARE_TOOL_TIMEOUT可调整），给内部30秒批量截止留返回部分结果的余量。新闻模块内部20秒，与skill一致。DangInvest工具默认retries=0用于限时，调用方可显式选1或2；同参数验收也使用0。
- 本轮没有声称A+H上市列表/里程碑等全部skill能力已经迁入MCP。

## 实测

通过真实stdio MCP会话，按相同股票与参数逐项调用skill正式CLI和MCP：

| 对比项 | 数据部分一致 | 备注 |
|---|---|---|
| 600519历史10根 | 是 | 同字段、同值 |
| 600519财务3期 | 是 | 同报告期、同指标值 |
| 600519行业 | 是 | 排除查询时刻字段再比较 |
| 概念概览10条 | 是 | stale=true保持一致 |
| 半导体成分3条 | 是 | stale=true保持一致 |
| 市场快讯5条 | 是 | stale=true保持一致 |

两边查询时刻、来源计时和接口封装不同，不做字节级相等断言。顺序执行的耗时受网络/上游缓存影响，这些数据不是性能对照基准。

35项测试通过（含12项原有在线测试）；MCP注册62个工具。完整CLI stdout/stderr和MCP原始响应在 `reports/raw/data-health-2026-10-11/skill-alignment/`，机器对比追加在[体检JSON](data-health-2026-10-11.json)的skill_alignment字段。


安装复核：按实际uv.lock（MCP1.6.0、AKShare1.18.40、pandas2.2.3）在独立Python3.12环境运行，35项测试同样通过并注册62工具。另一个较新依赖环境（MCP1.30.0、AKShare1.19.1、pandas3.0.6）也通过35项。新增.python-version=3.12，避免uv默认选3.14编译旧锁中的numpy/pandas。运行时独立复制源目录也能发现62工具。
