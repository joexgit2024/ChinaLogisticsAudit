import json, pandas as pd
from pathlib import Path

def main():
    rate_path = Path('uploads') / 'FEDEX Rate Card (1).xlsx'
    inv_path = Path('uploads') / 'CN.INVAWB0814162804111.xls'
    output = {}
    if rate_path.exists():
        try:
            xls = pd.ExcelFile(rate_path)
            output['rate_card'] = {'sheets': xls.sheet_names}
            samples = {}
            for name in xls.sheet_names[:8]:
                try:
                    df = xls.parse(name)
                    samples[name] = {
                        'columns': df.columns.tolist()[:40],
                        'row_count': len(df),
                        'sample_rows': df.head(3).fillna('').astype(str).to_dict(orient='records')
                    }
                except Exception as e:
                    samples[name] = {'error': str(e)}
            output['rate_card']['samples'] = samples
        except Exception as e:
            output['rate_card_error'] = str(e)
    else:
        output['rate_card_missing'] = str(rate_path)
    if inv_path.exists():
        try:
            xls2 = pd.ExcelFile(inv_path)
            output['invoice'] = {'sheets': xls2.sheet_names}
            samples2 = {}
            for name in xls2.sheet_names[:5]:
                try:
                    df = xls2.parse(name)
                    samples2[name] = {
                        'columns': df.columns.tolist()[:40],
                        'row_count': len(df),
                        'sample_rows': df.head(3).fillna('').astype(str).to_dict(orient='records')
                    }
                except Exception as e:
                    samples2[name] = {'error': str(e)}
            output['invoice']['samples'] = samples2
        except Exception as e:
            output['invoice_error'] = str(e)
    else:
        output['invoice_missing'] = str(inv_path)
    print(json.dumps(output, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main()
