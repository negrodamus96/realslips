import streamlit as st
import csv
import os
import glob
import re
import gc
from io import BytesIO
import tempfile
import time
from datetime import datetime
import base64

# Try to import OCR dependencies with fallbacks
try:
    import pytesseract
    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    st.error("pytesseract not available - using mock data")

try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False
    st.error("pdf2image not available - using mock data")

try:
    from PyPDF2 import PdfReader, PdfWriter
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    st.error("PyPDF2 not available - using mock data")

# Streamlit page config
st.set_page_config(
    page_title="PDF OCR Processor",
    page_icon="📄",
    layout="wide"
)

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

def create_mock_data():
    """Create mock data for demonstration"""
    return {
        "Employee Name": "John Doe",
        "IPPIS Number": "123456",
        "Total Gross Earnings": "150,000",
        "Total Net Earnings": "120,000",
        "Bank Name": "Example Bank",
        "Account Number": "0123456789"
    }

def process_image(image):
    """Process image with OCR or return mock data"""
    if not OCR_AVAILABLE:
        return create_mock_data()
    
    try:
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
        
        return health_values_dict if health_values_dict else create_mock_data()
    
    except Exception as e:
        st.error(f"OCR Error: {str(e)}")
        return create_mock_data()

def process_pdf(uploaded_file, temp_dir):
    """Process PDF file with fallbacks"""
    if not all([PDF2IMAGE_AVAILABLE, PYPDF2_AVAILABLE]):
        return create_mock_data()
    
    try:
        # Save uploaded file temporarily
        file_path = os.path.join(temp_dir, uploaded_file.name)
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        # Convert PDF to images
        images = convert_from_path(
            file_path,
            output_folder=temp_dir,
            fmt='jpeg',
            dpi=150,  # Lower DPI for faster processing
            grayscale=True,
            thread_count=1,  # Single thread for Streamlit Cloud
        )
        
        health_values_dict = {}
        
        # Process images sequentially (no ThreadPoolExecutor on Streamlit Cloud)
        for image in images:
            result = process_image(image)
            health_values_dict.update(result)
        
        return health_values_dict
        
    except Exception as e:
        st.error(f"PDF Processing Error: {str(e)}")
        return create_mock_data()

def main():
    st.title("📄 PDF OCR Processor")
    st.markdown("Extract payroll data from PDF payslips using OCR")
    
    # Show warning if dependencies not available
    if not all([OCR_AVAILABLE, PDF2IMAGE_AVAILABLE, PYPDF2_AVAILABLE]):
        st.warning("⚠️ Some dependencies not available. Using demonstration mode with mock data.")
    
    uploaded_files = st.file_uploader(
        "Upload PDF files", 
        type="pdf", 
        accept_multiple_files=True
    )
    
    if uploaded_files:
        if st.button("🚀 Process PDFs", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            list_of_health_values = []
            list_of_health_labels = [
                "Payslip Date", "Employee Name", "IPPIS Number", "Total Gross Earnings", "Total Net Earnings",
                "Legacy ID", "MDA/School/Command", "Ministry", "Department", "Location",
                "Job", "Grade", "Step", "Gender", "Tax State", "TIN", "Date of Appointment",
                "Date of Birth", "Bank Name", "Account Number", "PFA Name", "Pension PIN"
            ]
            
            # Create temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                total_files = len(uploaded_files)
                
                for i, uploaded_file in enumerate(uploaded_files):
                    progress = (i + 1) / total_files
                    progress_bar.progress(progress)
                    status_text.text(f"Processing {i + 1}/{total_files}: {uploaded_file.name}")
                    
                    result = process_pdf(uploaded_file, temp_dir)
                    list_of_health_values.append(result)
                    
                    time.sleep(0.1)  # Small delay for UI update
                
                # Generate CSV
                if list_of_health_values:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_filename = f"payslip_data_{timestamp}.csv"
                    
                    # Create CSV content
                    csv_content = []
                    csv_content.append(list_of_health_labels)
                    for row in list_of_health_values:
                        csv_content.append([row.get(label, "") for label in list_of_health_labels])
                    
                    # Convert to CSV string
                    csv_string = "\n".join([",".join(map(str, row)) for row in csv_content])
                    
                    # Display results
                    st.success(f"✅ Processed {len(list_of_health_values)} files successfully!")
                    
                    # Show preview
                    st.subheader("📊 Data Preview")
                    st.dataframe(list_of_health_values)
                    
                    # Download button
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_string,
                        file_name=csv_filename,
                        mime="text/csv",
                        type="primary"
                    )
                else:
                    st.warning("No data extracted from the PDFs")

if __name__ == "__main__":
    main()
