import pytesseract
import csv
from pdf2image import convert_from_path
import os
import glob
import ntpath
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from PyPDF2 import PdfReader, PdfWriter
import shutil
import gc
from io import BytesIO
import time

current_path = os.path.abspath(os.getcwd())

# Create necessary directories if they don't exist
os.makedirs(current_path + '/PDF', exist_ok=True)
os.makedirs(current_path + '/CSV', exist_ok=True)
os.makedirs(current_path + '/IMG', exist_ok=True)
os.makedirs(current_path + '/TEMP', exist_ok=True)

files = glob.glob(current_path + '/PDF' + '/**/*.[pP][dD][fF]', recursive=True)
list_of_health_values = []
csv_path = current_path + '/CSV/' + 'data.csv'
list_of_health_labels = ["Payslip Date", "Employee Name", "IPPIS Number", "Total Gross Earnings", "Total Net Earnings",
                        "Legacy ID", "MDA/School/Command", "Ministry", "Department", "Location",
                        "Job", "Grade", "Step", "Gender", "Tax State", "TIN", "Date of Appointment",
                        "Date of Birth", "", "Bank Name", "Account Number", "PFA Name", "Pension PIN"]

# Pre-compile regex patterns for faster matching
patterns = {
    "multi_colon": re.compile(r".*:.*:.*"),
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

# Optimized settings
MAX_WORKERS = 8  # Adjust based on your CPU cores
OCR_CONFIG = r'--oem 1 --psm 6 -c tessedit_do_invert=0'
PDF_CONVERSION_DPI = 200  # Lower DPI for faster processing

def timed_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} took {end_time - start_time:.2f} seconds")
        return result
    return wrapper

def extract_field_value(line, field_name):
    """Extract field value from a line with single colon"""
    if ":" in line:
        return line.split(":", 1)[1].strip()
    return ""

@timed_function
def split_pdf_by_pages(pdf_path):
    """Split a multi-page PDF into individual single-page PDFs using memory buffer"""
    temp_dir = current_path + '/TEMP'
    base_filename = os.path.splitext(ntpath.basename(pdf_path))[0]
    output_files = []
    
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PdfReader(file)
            total_pages = len(pdf_reader.pages)
            
            if total_pages == 1:
                return [pdf_path]
            
            print(f"   Splitting {pdf_path} into {total_pages} individual pages...")
            
            for page_num in range(total_pages):
                pdf_writer = PdfWriter()
                pdf_writer.add_page(pdf_reader.pages[page_num])
                
                # Write to memory first for faster I/O
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
        print(f"Error splitting PDF {pdf_path}: {str(e)}")
        return [pdf_path]

@timed_function
def process_image(image):
    """Process a single image with optimized OCR settings"""
    # Pre-process image for better performance
    image = image.convert('L')  # Convert to grayscale
    
    content = pytesseract.image_to_string(
        image, 
        lang='eng', 
        output_type=pytesseract.Output.DICT, 
        config=OCR_CONFIG
    )
    
    content_list = content["text"].split("\n")
    health_values_dict = {}
    
    for i, line in enumerate(content_list):
        line_clean = line.strip()
        if not line_clean:
            continue
            
        # Check for multi-colon lines first (more efficient)
        if line_clean.count(":") > 1:
            if patterns["employee_name"].search(line_clean):
                # Payslip Date logic
                if i > 0 and content_list[i-1].strip():
                    health_values_dict["Payslip Date"] = content_list[i-1].strip()
                elif i > 1 and content_list[i-2].strip():
                    health_values_dict["Payslip Date"] = content_list[i-2].strip()
                
                # Extract Employee Name and Grade
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Employee Name"] = parts[1][:-5].strip() if len(parts[1]) > 5 else parts[1].strip()
                    health_values_dict["Grade"] = parts[2].strip()
                    
            elif patterns["ippis_number"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["IPPIS Number"] = parts[1][:-4].strip() if len(parts[1]) > 4 else parts[1].strip()
                    health_values_dict["Step"] = parts[2].strip()
                    
            elif patterns["legacy_id"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Legacy ID"] = parts[1][:-6].strip() if len(parts[1]) > 6 else parts[1].strip()
                    health_values_dict["Gender"] = parts[2].strip()
                    
            elif patterns["mda"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["MDA/School/Command"] = parts[1][:-9].strip() if len(parts[1]) > 9 else parts[1].strip()
                    health_values_dict["Tax State"] = parts[2].strip()
                    
            elif patterns["department"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Department"] = parts[1][:-3].strip() if len(parts[1]) > 3 else parts[1].strip()
                    health_values_dict["TIN"] = parts[2].strip()
                    
            elif patterns["location"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Location"] = parts[1][:-19].strip() if len(parts[1]) > 19 else parts[1].strip()
                    health_values_dict["Date of Appointment"] = parts[2].strip()
                    
            elif patterns["job"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Job"] = parts[1][:-13].strip() if len(parts[1]) > 13 else parts[1].strip()
                    health_values_dict["Date of Birth"] = parts[2].strip()
                    
            elif patterns["bank_name"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Bank Name"] = parts[1][:-8].strip() if len(parts[1]) > 8 else parts[1].strip()
                    health_values_dict["PFA Name"] = parts[2].strip()
                    
            elif patterns["account_number"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 3:
                    health_values_dict["Account Number"] = parts[1][:-11].strip() if len(parts[1]) > 11 else parts[1].strip()
                    pension_pin = parts[2].replace("|", "").strip()
                    health_values_dict["Pension PIN"] = pension_pin
                    
            elif patterns["total_gross"].search(line_clean):
                parts = line_clean.split(":")
                if len(parts) >= 2:
                    health_values_dict["Total Gross Earnings"] = parts[1].replace("N", "").strip()
                    
            elif patterns["total_net"].search(line_clean):
                parts = line_clean.split("gs:")
                if len(parts) >= 2:
                    health_values_dict["Total Net Earnings"] = parts[1].replace("N", "").strip()
                    
        # Single colon lines
        elif line_clean.count(":") == 1:
            if patterns["employee_name"].search(line_clean):
                health_values_dict["Employee Name"] = extract_field_value(line_clean, "Employee Name")
            elif patterns["grade"].search(line_clean):
                health_values_dict["Grade"] = extract_field_value(line_clean, "Grade")
            elif patterns["ippis_number"].search(line_clean):
                health_values_dict["IPPIS Number"] = extract_field_value(line_clean, "IPPIS Number")
            elif patterns["step"].search(line_clean):
                health_values_dict["Step"] = extract_field_value(line_clean, "Step")
            elif patterns["legacy_id"].search(line_clean):
                health_values_dict["Legacy ID"] = extract_field_value(line_clean, "Legacy ID")
            elif patterns["gender"].search(line_clean):
                health_values_dict["Gender"] = extract_field_value(line_clean, "Gender")
            elif patterns["mda"].search(line_clean):
                health_values_dict["MDA/School/Command"] = extract_field_value(line_clean, "MDA/School/Command")
            elif patterns["ministry"].search(line_clean):
                health_values_dict["Ministry"] = extract_field_value(line_clean, "Ministry")
            elif patterns["tax_state"].search(line_clean):
                health_values_dict["Tax State"] = extract_field_value(line_clean, "Tax State")
            elif patterns["department"].search(line_clean):
                health_values_dict["Department"] = extract_field_value(line_clean, "Department")
            elif patterns["tin"].search(line_clean):
                health_values_dict["TIN"] = extract_field_value(line_clean, "TIN")
            elif patterns["location"].search(line_clean):
                health_values_dict["Location"] = extract_field_value(line_clean, "Location")
            elif patterns["date_of_appointment"].search(line_clean):
                health_values_dict["Date of Appointment"] = extract_field_value(line_clean, "Date of Appointment")
            elif patterns["job"].search(line_clean):
                health_values_dict["Job"] = extract_field_value(line_clean, "Job")
            elif patterns["date_of_birth"].search(line_clean):
                health_values_dict["Date of Birth"] = extract_field_value(line_clean, "Date of Birth")
            elif patterns["bank_name"].search(line_clean):
                health_values_dict["Bank Name"] = extract_field_value(line_clean, "Bank Name")
            elif patterns["pfa_name"].search(line_clean):
                health_values_dict["PFA Name"] = extract_field_value(line_clean, "PFA Name")
            elif patterns["account_number"].search(line_clean):
                health_values_dict["Account Number"] = extract_field_value(line_clean, "Account Number")
            elif patterns["pension_pin"].search(line_clean):
                pension_pin = extract_field_value(line_clean, "Pension PIN")
                health_values_dict["Pension PIN"] = pension_pin.replace("|", "") if "|" in pension_pin else pension_pin
            elif patterns["total_gross"].search(line_clean):
                health_values_dict["Total Gross Earnings"] = extract_field_value(line_clean, "Total Gross Earnings").replace("N", "")
            elif patterns["total_net"].search(line_clean):
                health_values_dict["Total Net Earnings"] = extract_field_value(line_clean, "Total Net Earnings").replace("N", "")
    
    return health_values_dict

@timed_function
def process_pdf(pdf_path):
    """Process a single PDF file with optimized settings"""
    filename = ntpath.basename(pdf_path).split(".")[0]
    print(f"Processing: {filename}")
    
    # Convert PDF to images with optimized settings
    images = convert_from_path(
        pdf_path,
        output_folder=current_path + '/IMG',
        fmt='jpeg',  # JPEG is faster to process than PNG
        jpegopt={"quality": 85, "progressive": True, "optimize": True},
        dpi=PDF_CONVERSION_DPI,  # Reduced DPI for faster processing
        output_file=filename,
        thread_count=4,
        grayscale=True,  # Convert to grayscale for faster OCR
    )
    
    print(f'   {len(images)} images converted successfully for {filename}!')
    print('   OCR processing started. Please wait...')
    
    health_values_dict = {}
    
    # Process images in parallel within the same PDF
    with ThreadPoolExecutor(max_workers=min(4, len(images))) as executor:
        future_to_image = {executor.submit(process_image, image): image for image in images}
        
        for future in as_completed(future_to_image):
            try:
                result = future.result()
                health_values_dict.update(result)
            except Exception as e:
                print(f"Error processing image: {str(e)}")
    
    # Force garbage collection
    del images
    gc.collect()
    
    return health_values_dict

# Check for already processed files to skip them
processed_files = set()
if os.path.exists(csv_path):
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Assuming filename is stored in some field, adjust as needed
                if 'Original Filename' in row:
                    processed_files.add(row['Original Filename'])
    except:
        pass

# First, split all multi-page PDFs into single pages
all_pdf_files_to_process = []
for pdf_file in files:
    filename = ntpath.basename(pdf_file)
    if filename in processed_files:
        print(f"Skipping already processed: {filename}")
        continue
        
    print(f"Checking: {filename}")
    split_files = split_pdf_by_pages(pdf_file)
    all_pdf_files_to_process.extend(split_files)

print(f"\nTotal files to process: {len(all_pdf_files_to_process)}")

# Process PDFs in parallel with more workers
with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(all_pdf_files_to_process))) as executor:
    future_to_pdf = {executor.submit(process_pdf, pdf_path): pdf_path for pdf_path in all_pdf_files_to_process}
    
    for future in as_completed(future_to_pdf):
        pdf_path = future_to_pdf[future]
        try:
            result = future.result()
            # Add original filename for tracking
            result['Original Filename'] = ntpath.basename(pdf_path)
            list_of_health_values.append(result)
            print(f"Completed processing: {ntpath.basename(pdf_path)}")
        except Exception as e:
            print(f"Error processing {pdf_path}: {str(e)}")

print("\nWriting to CSV file.")
# Write to file
with open(csv_path, 'w', newline="", encoding='utf-8') as csvfile: 
    writer = csv.DictWriter(csvfile, fieldnames=list_of_health_labels + ['Original Filename']) 
    writer.writeheader() 
    writer.writerows(list_of_health_values)
print("CSV file created!")

print("\n=========Completed successfully!!!=========")

print("\nWould you like to delete the temporary files? [y/n]")
x = input().strip().lower()
if x == "y":
    # Delete the images
    img_files = glob.glob(current_path + '/IMG' + '/*')
    for f in img_files:
        try:
            os.remove(f)
        except:
            pass
    
    # Delete temporary split PDFs
    temp_files = glob.glob(current_path + '/TEMP' + '/*')
    for f in temp_files:
        try:
            os.remove(f)
        except:
            pass
    
    print("Temporary files deleted successfully!")
else:
    print("Skipping cleanup!")

