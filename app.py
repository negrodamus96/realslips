import io, ntpath, re, tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pytesseract
import streamlit as st
from pdf2image import convert_from_path

st.set_page_config(page_title="Payslip OCR → CSV", layout="centered")
st.title("Payslip OCR → CSV")
st.caption("Upload PDFs (multi-page supported). Each page becomes its own row. Click **Process** then download CSV.")

# === Output columns ===
BASE_COLUMNS = [
    "Payslip Date", "Employee Name", "IPPIS Number", "Total Gross Earnings", "Total Net Earnings",
    "Legacy ID", "MDA/School/Command", "Ministry", "Department", "Location",
    "Job", "Grade", "Step", "Gender", "Tax State", "TIN", "Date of Appointment",
    "Date of Birth", "Bank Name", "Account Number", "PFA Name", "Pension PIN"
]
AUX_COLUMNS = ["Source File", "Page #"]
OUTPUT_COLUMNS = AUX_COLUMNS + BASE_COLUMNS

# === Patterns ===
P = {
    "employee_name": re.compile(r"Employee Name"),
    "ippis_number": re.compile(r"IPPIS Number"),
    "legacy_id": re.compile(r"Legacy ID"),
    "mda": re.compile(r"MDA/School/Command"),
    "department": re.compile(r"Department"),
    "location": re.compile(r"Location"),
    "job": re.compile(r"Job"),
    "bank_name": re.compile(r"Bank Name"),
    "account_number": re.compile(r"Account Number"),
    "total_gross": re.compile(r"Total Gross Earnings"),
    "total_net": re.compile(r"Total Net Earnings"),
    "grade": re.compile(r"Grade"),
    "step": re.compile(r"Step"),
    "gender": re.compile(r"Gender"),
    "ministry": re.compile(r"Ministry"),
    "tax_state": re.compile(r"Tax State"),
    "tin": re.compile(r"TIN"),
    "date_of_appointment": re.compile(r"Date of Appointment"),
    "date_of_birth": re.compile(r"Date of Birth"),
    "pfa_name": re.compile(r"PFA Name"),
    "pension_pin": re.compile(r"Pension PIN"),
}

def _extract_field_value(line: str) -> str:
    return line.split(":", 1)[1].strip() if ":" in line else ""

def _process_image(image) -> dict:
    """OCR a single image (one PDF page) and parse fields."""
    text = pytesseract.image_to_string(
        image, lang='eng',
        output_type=pytesseract.Output.DICT,
        config="--psm 6"
    )["text"]
    lines = [ln.strip() for ln in text.splitlines()]

    out = {}
    for i, line in enumerate(lines):
        if not line:
            continue

        colon_count = line.count(":")
        if colon_count > 1:
            parts = line.split(":")
            if P["employee_name"].search(line):
                if i > 0 and lines[i-1].strip():
                    out["Payslip Date"] = lines[i-1].strip()
                elif i > 1 and lines[i-2].strip():
                    out["Payslip Date"] = lines[i-2].strip()
                if len(parts) >= 3:
                    out["Employee Name"] = (parts[1][:-5] if len(parts[1]) > 5 else parts[1]).strip()
                    out["Grade"] = parts[2].strip()

            elif P["ippis_number"].search(line) and len(parts) >= 3:
                out["IPPIS Number"] = (parts[1][:-4] if len(parts[1]) > 4 else parts[1]).strip()
                out["Step"] = parts[2].strip()

            elif P["legacy_id"].search(line) and len(parts) >= 3:
                out["Legacy ID"] = (parts[1][:-6] if len(parts[1]) > 6 else parts[1]).strip()
                out["Gender"] = parts[2].strip()

            elif P["mda"].search(line) and len(parts) >= 3:
                out["MDA/School/Command"] = (parts[1][:-9] if len(parts[1]) > 9 else parts[1]).strip()
                out["Tax State"] = parts[2].strip()

            elif P["department"].search(line) and len(parts) >= 3:
                out["Department"] = (parts[1][:-3] if len(parts[1]) > 3 else parts[1]).strip()
                out["TIN"] = parts[2].strip()

            elif P["location"].search(line) and len(parts) >= 3:
                out["Location"] = (parts[1][:-19] if len(parts[1]) > 19 else parts[1]).strip()
                out["Date of Appointment"] = parts[2].strip()

            elif P["job"].search(line) and len(parts) >= 3:
                out["Job"] = (parts[1][:-13] if len(parts[1]) > 13 else parts[1]).strip()
                out["Date of Birth"] = parts[2].strip()

            elif P["bank_name"].search(line) and len(parts) >= 3:
                out["Bank Name"] = (parts[1][:-8] if len(parts[1]) > 8 else parts[1]).strip()
                out["PFA Name"] = parts[2].strip()

            elif P["account_number"].search(line) and len(parts) >= 3:
                out["Account Number"] = (parts[1][:-11] if len(parts[1]) > 11 else parts[1]).strip()
                out["Pension PIN"] = parts[2].replace("|", "").strip()

            elif P["total_gross"].search(line) and len(parts) >= 2:
                out["Total Gross Earnings"] = parts[1].replace("N", "").strip()

            elif P["total_net"].search(line):
                parts2 = line.split("gs:")
                if len(parts2) >= 2:
                    out["Total Net Earnings"] = parts2[1].replace("N", "").strip()

        elif colon_count == 1:
            if P["employee_name"].search(line):
                out["Employee Name"] = _extract_field_value(line)
            elif P["grade"].search(line):
                out["Grade"] = _extract_field_value(line)
            elif P["ippis_number"].search(line):
                out["IPPIS Number"] = _extract_field_value(line)
            elif P["step"].search(line):
                out["Step"] = _extract_field_value(line)
            elif P["legacy_id"].search(line):
                out["Legacy ID"] = _extract_field_value(line)
            elif P["gender"].search(line):
                out["Gender"] = _extract_field_value(line)
            elif P["mda"].search(line):
                out["MDA/School/Command"] = _extract_field_value(line)
            elif P["ministry"].search(line):
                out["Ministry"] = _extract_field_value(line)
            elif P["tax_state"].search(line):
                out["Tax State"] = _extract_field_value(line)
            elif P["department"].search(line):
                out["Department"] = _extract_field_value(line)
            elif P["tin"].search(line):
                out["TIN"] = _extract_field_value(line)
            elif P["location"].search(line):
                out["Location"] = _extract_field_value(line)
            elif P["date_of_appointment"].search(line):
                out["Date of Appointment"] = _extract_field_value(line)
            elif P["job"].search(line):
                out["Job"] = _extract_field_value(line)
            elif P["date_of_birth"].search(line):
                out["Date of Birth"] = _extract_field_value(line)
            elif P["bank_name"].search(line):
                out["Bank Name"] = _extract_field_value(line)
            elif P["pfa_name"].search(line):
                out["PFA Name"] = _extract_field_value(line)
            elif P["account_number"].search(line):
                out["Account Number"] = _extract_field_value(line)
            elif P["pension_pin"].search(line):
                v = _extract_field_value(line)
                out["Pension PIN"] = v.replace("|", "") if "|" in v else v
            elif P["total_gross"].search(line):
                out["Total Gross Earnings"] = _extract_field_value(line).replace("N", "").strip()
            elif P["total_net"].search(line):
                out["Total Net Earnings"] = _extract_field_value(line).replace("N", "").strip()

    return out

def _ocr_pdf_bytes_split_pages(pdf_bytes: bytes, source_name: str) -> list[dict]:
    """
    Convert PDF bytes to page images, OCR each page, and return
    a list of dicts (one dict per page).
    """
    with tempfile.TemporaryDirectory() as td:
        pdf_path = Path(td) / "in.pdf"
        pdf_path.write_bytes(pdf_bytes)

        images = convert_from_path(str(pdf_path), thread_count=4)

        rows = []
        for page_idx, img in enumerate(images, start=1):
            page_data = _process_image(img)
            page_data.setdefault("Payslip Date", "")
            # Add provenance columns
            row = {col: page_data.get(col, "") for col in BASE_COLUMNS}
            row["Source File"] = ntpath.basename(source_name)
            row["Page #"] = page_idx
            rows.append(row)
        return rows

# ===== UI =====
uploaded = st.file_uploader(
    "Drop PDF payslips (multi-page allowed)",
    type=["pdf"],
    accept_multiple_files=True
)

col1, col2 = st.columns([1, 3])
with col1:
    run = st.button("Process", type="primary", use_container_width=True)

if uploaded and run:
    all_rows = []
    progress = st.progress(0, text="Starting…")
    errors = []

    # Parallelize across files; pages are handled sequentially within each file
    with ThreadPoolExecutor(max_workers=min(6, len(uploaded))) as ex:
        futures = {ex.submit(_ocr_pdf_bytes_split_pages, f.read(), f.name): f.name for f in uploaded}
        total = len(futures)
        for i, fut in enumerate(as_completed(futures), start=1):
            fname = futures[fut]
            try:
                page_rows = fut.result()
                all_rows.extend(page_rows)
            except Exception as e:
                errors.append(f"{fname}: {e}")

            progress.progress(i/total, text=f"Processed {i}/{total} file(s)")

    # Build DataFrame (each page = one row)
    # Ensure column order
    df = pd.DataFrame(all_rows, columns=OUTPUT_COLUMNS) if all_rows else pd.DataFrame(columns=OUTPUT_COLUMNS)

    st.success(f"Done. {len(df)} page(s) extracted.")
    if errors:
        st.warning("Some files had issues:\n- " + "\n- ".join(errors))

    st.dataframe(df, use_container_width=True, height=420)

    # Download CSV
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    st.download_button(
        "⬇️ Download CSV",
        data=csv_buf.getvalue(),
        file_name="data.csv",
        mime="text/csv",
        use_container_width=True
    )

with st.expander("Notes"):
    st.markdown(
        "- Multi-page PDFs are split automatically; each page becomes one CSV row.\n"
        "- If a field is blank, it wasn’t confidently detected on that page.\n"
        "- Clear, high-resolution PDFs improve accuracy.\n"
        "- Nothing is stored; processing occurs in session memory only."
    )

