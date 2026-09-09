import sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import openpyxl

file_path = sys.argv[1]
wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
ws = wb.active
rows = list(ws.iter_rows(values_only=True))
data = rows[3:]  # skip 3 header rows
out = []
for r in data:
    sku, cat = r[1], r[2]
    if sku and cat:
        out.append({'sku': int(sku), 'cat': str(cat)})
print(json.dumps(out, ensure_ascii=False))
