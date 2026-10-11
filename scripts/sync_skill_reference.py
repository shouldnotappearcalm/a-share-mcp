from pathlib import Path
import ast,json,hashlib,subprocess
import argparse
parser=argparse.ArgumentParser(description='Sync standalone data reference functions from a-share-skill')
parser.add_argument('--source',required=True,help='Path to a-share-skill repository')
args=parser.parse_args()
root=Path(__file__).resolve().parents[1]
source_repo=Path(args.source).resolve()
source=source_repo/'a-share-data/scripts'
package=root/'src/skill_compat';package.mkdir(exist_ok=True);(package/'__init__.py').write_text('"""Data functions synchronized from the a-share-data skill; no sibling imports."""\n')
specs={
 'history':('fetch_history_fallback.py',{'_build_session','normalize_code','_code_digits','_to_secid','_get_json','_get_text','get_kline_sina','get_kline_tencent','_estimate_fetch_count','get_kline_xueqiu','get_kline_eastmoney','_fetch_kline_uncached','_fetch_kline_with_fallback','_format_kline_output','_industry_from_eastmoney','_get_boards_from_akshare','_scan_boards_for_code','cmd_industry','_financials_from_akshare','_financial_single_from_akshare','_filter_financial_periods'}),
 'danginvest':('fetch_danginvest.py',{'_build_session','_get_json','normalize_mode','normalize_sort','_now_meta','cmd_news','cmd_summary','cmd_detail'})}
manifest={'source_worktree_dirty':bool(subprocess.check_output(['git','-C',str(source_repo),'status','--porcelain'],text=True).strip()),'source_repo':'https://github.com/shouldnotappearcalm/a-share-skill','source_commit':subprocess.check_output(['git','-C',str(source_repo),'rev-parse','HEAD'],text=True).strip(),'modules':{}}
for target,(filename,names) in specs.items():
 text=(source/filename).read_text();tree=ast.parse(text);selected=[]
 for node in tree.body:
  if isinstance(node,(ast.Import,ast.ImportFrom,ast.Assign,ast.AnnAssign)):
   selected.append(node)
  elif isinstance(node,ast.Try) and all(isinstance(statement,(ast.Import,ast.ImportFrom)) for statement in node.body):
   selected.append(node)
  elif isinstance(node,(ast.FunctionDef,ast.ClassDef)) and node.name in names:
   if node.name.startswith('cmd_'):
    # CLI commands become dict-returning functions; JSON payload computation is unchanged.
    body=[]
    for statement in node.body:
     if isinstance(statement,ast.If) and ast.unparse(statement.test) in ('output_json','args.output_json'):
      call=statement.body[0].value
      body.append(ast.Return(value=call.args[0].args[0]));break
     body.append(statement)
    node.body=body
   selected.append(node)
 module=ast.fix_missing_locations(ast.Module(body=selected,type_ignores=[]))
 generated='# Generated from a-share-data; see manifest.json. No runtime sibling dependency.\n'+ast.unparse(module)+'\n'
 (package/(target+'.py')).write_text(generated)
 manifest['modules'][target]={'source':filename,'source_sha256':hashlib.sha256(text.encode()).hexdigest(),'functions':sorted(names),'generated_sha256':hashlib.sha256(generated.encode()).hexdigest()}
# Keep the canonical stock normalization and minute-bar handling bodies identical.
p=root/'src/use_cases/realtime_market.py';original=p.read_text();ref=ast.parse((source/'fetch_realtime.py').read_text());own=ast.parse(original)
helper_names={'_get_market_status','_is_trading_session','_is_forming_minute_bar','_drop_forming_minute_bars','_aggregate_intraday_data'}
helpers='\n\n'.join(ast.get_source_segment((source/'fetch_realtime.py').read_text(),node) for node in ref.body if isinstance(node,ast.FunctionDef) and node.name in helper_names)
start=min(node.lineno for node in own.body if isinstance(node,ast.FunctionDef) and node.name in {'_get_market_status','_aggregate_intraday_data'})
end=max(node.end_lineno for node in own.body if isinstance(node,ast.FunctionDef) and node.name in {'_get_market_status','_aggregate_intraday_data'})
lines=original.splitlines();lines[start-1:end]=helpers.splitlines();text='\n'.join(lines)+'\n'
if 'from datetime import timedelta, time as time_type' not in text:
 text=text.replace('from typing import List','from typing import List, Optional\nfrom datetime import timedelta, time as time_type',1)
text=text.replace('_aggregate_intraday_data(df_min, today)','_aggregate_intraday_data(df_min, today, now=now, freq_minutes=5)',1)
text=text.replace('_aggregate_intraday_data(df_min, today)','_aggregate_intraday_data(df_min, today, now=now, freq_minutes=15)',1)
p.write_text(text)
# Reuse canonical normalization (keeping the MCP data source's raw-price contract).
ref_code=ast.get_source_segment((source/'fetch_realtime.py').read_text(),next(n for n in ref.body if isinstance(n,ast.FunctionDef) and n.name=='normalize_code'))
p=root/'src/ashare_data_source.py';s=p.read_text();tree=ast.parse(s);cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='AshareDataSource');node=next(n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='normalize_code');lines=s.splitlines();lines[node.lineno-1:node.end_lineno]=['    '+line for line in ref_code.splitlines()];p.write_text('\n'.join(lines)+'\n')
p=root/'src/use_cases/market_data.py';s=p.read_text();s=s if "result_df['MA60']" in s else s.replace("result_df['MA20'] = MA(CLOSE, 20)","result_df['MA20'] = MA(CLOSE, 20)\n                result_df['MA60'] = MA(CLOSE, 60)");p.write_text(s)
manifest['realtime_helpers']={'source':'fetch_realtime.py','functions':sorted(helper_names|{'normalize_code'}),'source_sha256':hashlib.sha256((source/'fetch_realtime.py').read_bytes()).hexdigest()}
news_text=(source/'fetch_company_news.py').read_text()
(root/'src/company_news.py').write_text(news_text[:news_text.index('\ndef main():')].rstrip()+'\n')
manifest['company_news']={'source':'fetch_company_news.py','source_sha256':hashlib.sha256((source/'fetch_company_news.py').read_bytes()).hexdigest(),'note':'src/company_news.py keeps the source module through build_payload; CLI main removed'}
(package/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
print('Skill-compatible data modules and exact realtime helpers synchronized')

for module in ('data_runtime','fetch_industry'):
 original=(source/(module+'.py')).read_text()
 generated=original
 (package/(module+'.py')).write_text(generated)
 manifest.setdefault('copied_modules',{})[module]={'source':module+'.py','source_sha256':hashlib.sha256(original.encode()).hexdigest()}
(package/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
