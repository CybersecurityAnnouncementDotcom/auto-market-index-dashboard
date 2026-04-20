#!/usr/bin/env python3
"""Parse a single MUVVI-format XLSX and emit JSON rows to stdout.
Usage: parse_xlsx_to_stdin.py <path.xlsx>
"""
import sys, json
try:
    import openpyxl
except ImportError:
    sys.stderr.write("openpyxl not installed\n")
    sys.exit(2)

def main():
    if len(sys.argv) != 2:
        sys.stderr.write("usage: parse_xlsx_to_stdin.py <path.xlsx>\n")
        sys.exit(2)
    path = sys.argv[1]
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        print("[]")
        return
    header_idx = None
    for i, row in enumerate(rows[:5]):
        if row and any(c and "Index" in str(c) for c in row):
            header_idx = i
            break
    if header_idx is None:
        print("[]")
        return
    headers = [str(c).strip() if c is not None else "" for c in rows[header_idx]]
    data = []
    for r in rows[header_idx + 1:]:
        if not r or r[0] is None:
            continue
        d = r[0]
        if hasattr(d, "date"):
            d = d.date()
        rec = {"date": str(d)}
        for h, v in zip(headers[1:], r[1:]):
            if h and v is not None:
                rec[h] = v
        data.append(rec)
    print(json.dumps(data, default=str))

if __name__ == "__main__":
    main()
