from pathlib import Path
p=Path('outputs/hadoop/q3.tsv')
if not p.exists():
    print('q3.tsv missing')
else:
    b=p.read_bytes()
    if b.startswith(b'\xff\xfe') or b.startswith(b'\xfe\xff') or b.startswith(b'\x00'):
        try:
            s=b.decode('utf-16')
            p.write_text(s, encoding='utf-8')
            print('Converted outputs/hadoop/q3.tsv to UTF-8')
        except Exception as e:
            print('Decode error', e)
    else:
        print('q3.tsv looks already utf-8')
