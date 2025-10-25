import sys, subprocess
pdf_path = r"C:\\Users\\angel\\OneDrive\\Desktop\\6-Information_Integration.pdf"
out_path = r"C:\\Users\\angel\\LSDM\\_pdf_text.txt"
try:
    import PyPDF2
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "PyPDF2", "--quiet"]) 
    import PyPDF2
text = []
with open(pdf_path, 'rb') as f:
    reader = PyPDF2.PdfReader(f)
    for i, page in enumerate(reader.pages):
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        text.append(f"\n\n===== PAGE {i+1} =====\n\n" + t)
all_text = "".join(text)
with open(out_path, 'w', encoding='utf-8', errors='ignore') as g:
    g.write(all_text)
print("OK")
