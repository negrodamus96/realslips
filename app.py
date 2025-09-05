import streamlit as st
import pytesseract
import csv
from pdf2image import convert_from_path
import os
import glob
import ntpath
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from PyPDF2 import PdfReader, PdfWriter
import gc
from io import BytesIO
import tempfile
import time
from datetime import datetime

# Streamlit page config
st.set_page_config(
    page_title="PDF OCR Processor",
    page_icon="📄",
    layout="wide"
)

# Configuration
current_path = os.path.abspath(os.getcwd())

# Pre-compile regex patterns
patterns = {
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
    "pension_pin": re.compile(r"Pension PIN")
}

def extract_field_value(line):
    return line.split(":", 1)[1].strip() if ":" in line else ""

def split_pdf_by_pages(pdf_path, temp_dir):
    base_filename = os.path.splitext(ntpath.basename(pdf_path))[0]
    output_files = []
    
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PdfReader(file)
            total_pages = len(pdf_reader.pages)
            
            if total_pages == 1:
                return [pdf_path]
            
            for page_num in range(total_pages):
                pdf_writer = PdfWriter()
                pdf_writer.add_page(pdf_reader.pages[page_num])
                
                buffer = BytesIO()
                pdf_writer.write(buffer)
                buffer.seek(0)
                
                output_filename = f"{base_filename}_page_{page_num + 1}.pdf"
                output_filepath = os.path.join(temp_dir, output_filename)
                
                with open(output_filepath, 'wb') as output_file:
                    output_file.write(buffer.getvalue())
                
                output_files.append(output_filepath)
                buffer.close()
                
        return output_files
        
    except Exception as e:
        st.error(f"Error splitting PDF: {str(e)}")
        return [pdf_path]

def process_image(image):
    image = image.convert('L')
    custom_config = r'--oem 1 --psm 6'
    
    content = pytesseract.image_to_string(
        image, 
        lang='eng', 
        output_type=pytesseract.Output.DICT, 
        config=custom_config
    )
    
    content_list = content["text"].split("\n")
    health_values_dict = {}
    
    for i, line in enumerate(content_list):
        line_clean = line.strip()
        if not line_clean:
            continue
            
        if line_clean.count(":") > 1:
            if patterns["employee_name"].search(line_clean):
                if i > 0 and content_list[i-1].strip():
                    health_values_dict["Payslip Date"] = content_list[i-1].strip()
                
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Employee Name"] = parts[1][:-5].strip() if len(parts[1]) > 5 else parts[1].strip()
                    health_values_dict["Grade"] = parts[2].strip()
                    
            elif patterns["ippis_number"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["IPPIS Number"] = parts[1][:-4].strip() if len(parts[1]) > 4 else parts[1].strip()
                    health_values_dict["Step"] = parts[2].strip()
                    
            elif patterns["total_gross"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 2:
                    health_values_dict["Total Gross Earnings"] = parts[1].replace("N", "").strip()
                    
            elif patterns["total_net"].search(line_clean):
                parts = line_clean.split("gs:")
                if len(parts) >= 2:
                    health_values_dict["Total Net Earnings"] = parts[1].replace("N", "").strip()
                    
        elif line_clean.count(":") == 1:
            for field, pattern in patterns.items():
                if pattern.search(line_clean):
                    value = extract_field_value(line_clean)
                    if "earnings" in field.lower():
                        value = value.replace("N", "")
                    health_values_dict[field.replace("_", " ").title()] = value
                    break
    
    return health_values_dict

def process_pdf(pdf_path, img_temp_dir):
    filename = ntpath.basename(pdf_path).split(".")[0]
    
    images = convert_from_path(
        pdf_path,
        output_folder=img_temp_dir,
        fmt='jpeg',
        dpi=200,
        grayscale=True,
        thread_count=2,
    )
    
    health_values_dict = {}
    
    with ThreadPoolExecutor(max_workers=min(2, len(images))) as executor:
        results = list(executor.map(process_image, images))
        for result in results:
            health_values_dict.update(result)
    
    del images
    gc.collect()
    
    return health_values_dict

def main():
    st.title("📄 PDF OCR Processor")
    st.markdown("Extract payroll data from PDF payslips using OCR")
    
    uploaded_files = st.file_uploader(
        "Upload PDF files", 
        type="pdf", 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button("🚀 Process PDFs", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Create temporary directories
            with tempfile.TemporaryDirectory() as temp_dir:
                img_temp_dir = os.path.join(temp_dir, "images")
                pdf_temp_dir = os.path.join(temp_dir, "pdfs")
                os.makedirs(img_temp_dir, exist_ok=True)
                os.makedirs(pdf_temp_dir, exist_ok=True)
                
                # Save uploaded files
                pdf_paths = []
                for uploaded_file in uploaded_files:
                    file_path = os.path.join(pdf_temp_dir, uploaded_file.name)
                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    pdf_paths.append(file_path)
                
                list_of_health_values = []
                list_of_health_labels = [
                    "Payslip Date", "Employee Name", "IPPIS Number", "Total Gross Earnings", "Total Net Earnings",
                    "Legacy ID", "MDA/School/Command", "Ministry", "Department", "Location",
                    "Job", "Grade", "Step", "Gender", "Tax State", "TIN", "Date of Appointment",
                    "Date of Birth", "Bank Name", "Account Number", "PFA Name", "Pension PIN"
                ]
                
                all_pdf_files_to_process = []
                for pdf_file in pdf_paths:
                    status_text.text(f"Preparing: {ntpath.basename(pdf_file)}")
                    split_files = split_pdf_by_pages(pdf_file, temp_dir)
                    all_pdf_files_to_process.extend(split_files)
                
                total_files = len(all_pdf_files_to_process)
                status_text.text(f"Processing {total_files} pages...")
                
                processed_count = 0
                with ThreadPoolExecutor(max_workers=4) as executor:
                    future_to_pdf = {executor.submit(process_pdf, pdf_path, img_temp_dir): pdf_path for pdf_path in all_pdf_files_to_process}
                    
                    for future in as_completed(future_to_pdf):
                        pdf_path = future_to_pdf[future]
                        try:
                            result = future.result()
                            list_of_health_values.append(result)
                            processed_count += 1
                            progress_bar.progress(processed_count / total_files)
                            status_text.text(f"Processed {processed_count}/{total_files}: {ntpath.basename(pdf_path)}")
                        except Exception as e:
                            st.error(f"Error processing {pdf_path}: {str(e)}")
                
                # Generate CSV
                if list_of_health_values:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"payslip_data_{timestamp}.csv"
                    
                    csv_data = []
                    for row in list_of_health_values:
                        csv_data.append([row.get(label, "") for label in list_of_health_labels])
                    
                    # Display results
                    st.success(f"✅ Processed {len(list_of_health_values)} records successfully!")
                    
                    # Show preview
                    st.subheader("📊 Data Preview")
                    st.dataframe(list_of_health_values[:10])
                    
                    # Download button
                    csv_string = "\n".join([",".join(map(str, row)) for row in [list_of_health_labels] + csv_data])
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_string,
                        file_name=csv_filename,
                        mime="text/csv"
                    )
                else:
                    st.warning("No data extracted from the PDFs")

if __name__ == "__main__":
    main()
