import os
import re
import pandas as pd
import logging
import pdfplumber # Import pdfplumber for direct PDF table extraction

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bank_statement_extraction.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Folder paths
input_folder = "input_files"
output_excel_folder = "output_files"
os.makedirs(output_excel_folder, exist_ok=True)

def clean_amount(amount_str):
    """
    Clean amount string
    """
    if pd.isna(amount_str):
        return amount_str
    # Convert to string
    amount_str = str(amount_str)
    # Remove non-digit characters (keep digits, commas, periods, and minus signs)
    cleaned = re.sub(r"[^0-9.,\-]", "", amount_str)
    # Handle Chinese comma (，) replacement with English comma (,)
    cleaned = cleaned.replace("，", ",")
    # If multiple periods, keep only the last one (assuming last is decimal separator)
    if cleaned.count('.') > 1:
        parts = cleaned.split('.')
        cleaned = ''.join(parts[:-1]) + '.' + parts[-1] # Join all parts before last with '', then add last part
    # If it looks like a comma is used as decimal separator (e.g., 1,234.00 vs 1.234,00 format)
    # This is tricky without locale context, but HK often uses commas for thousands, period for decimal.
    # We'll assume commas are thousand separators if there are more than 3 digits after a comma
    # and a period exists before it, or if comma is not near the end.
    # A simple heuristic: if there's a period and a comma, and comma is not last few chars, likely thousand sep.
    # Or, if comma is used and no period, and comma is in a typical thousand place.
    # For now, we'll keep it simple and assume comma is thousand separator if period exists.
    # Or if comma is likely decimal (e.g., ends with ,xx), swap. This is complex.
    # Let's stick to the original logic for now, just fix the multiple periods.
    # Remove leading/trailing whitespace
    cleaned = cleaned.strip()
    return cleaned

def clean_table_data(df):
    """
    Clean table data
    """
    # Make a copy to avoid SettingWithCopyWarning
    df_cleaned = df.copy()
    
    # Identify potential date columns (common names)
    date_col_keywords = ["date", "日期"]
    date_cols = [col for col in df_cleaned.columns if any(kw.lower() in str(col).lower() for kw in date_col_keywords)]
    
    # Identify potential amount columns (common names)
    amount_col_keywords = ["withdrawals", "deposits", "balance", "amount", "支出", "存入", "結餘", "金額"]
    amount_cols = [col for col in df_cleaned.columns if any(kw.lower() in str(col).lower() for kw in amount_col_keywords)]
    
    logger.info(f"Identified date columns for cleaning: {date_cols}")
    logger.info(f"Identified amount columns for cleaning: {amount_cols}")

    # Clean date columns (basic cleaning: remove extra spaces/non-date chars if needed, but mainly log)
    for col in date_cols:
        if col in df_cleaned.columns:
             # Basic cleaning for dates, might need adjustment based on actual format
             df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
             # Example: Remove non-alphanumeric characters except common date separators
             # df_cleaned[col] = df_cleaned[col].apply(lambda x: re.sub(r"[^0-9A-Za-z/\-]", "", x) if pd.notna(x) else x)

    # Clean amount columns
    for col in amount_cols:
        if col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].apply(clean_amount)

    return df_cleaned

def process_pdf_with_pdfplumber(file_path, basename):
    """
    Process a PDF file using pdfplumber to extract tables.
    """
    logger.info(f"Processing PDF file with pdfplumber: {file_path}")
    all_tables = []

    try:
        with pdfplumber.open(file_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                logger.info(f"📄 Page {page_num} -> extracting tables...")
                print(f"📄 Page {page_num} -> extracting tables...")

                # Extract tables from the page
                # You might need to adjust table_settings if default detection isn't perfect
                tables = page.extract_tables()
                logger.info(f"   ✅ Found {len(tables)} table(s) on page {page_num}.")
                print(f"   ✅ Found {len(tables)} table(s).")

                for idx, table_data in enumerate(tables, start=1):
                    if not table_data:
                        logger.warning(f"      ⚠️ Table {idx} on page {page_num} is empty.")
                        print(f"      ⚠️ Table {idx} on page {page_num} is empty.")
                        continue

                    # Assume first row is header
                    if len(table_data) > 1:
                        header = table_data[0]
                        rows = table_data[1:]
                        # Create DataFrame
                        try:
                            df = pd.DataFrame(rows, columns=header)
                            logger.info(f"      📋 Table {idx} has {len(df)} rows and {len(df.columns)} columns.")
                            print(f"      📋 Table {idx} has {len(df)} rows and {len(df.columns)} columns.")

                            # Clean the extracted table data
                            df_cleaned = clean_table_data(df)

                            # Preview table (show first 3 rows and column names)
                            preview = f"Columns: {list(df_cleaned.columns)}\n"
                            for i in range(min(3, len(df_cleaned))):
                                preview += f"Row {i+1}: {list(df_cleaned.iloc[i].values)}\n"
                            logger.info(f"      📋 Table {idx} preview:\n{preview}")
                            print(f"      📋 Table {idx} preview:\n{preview}")

                            all_tables.append(df_cleaned)
                        except Exception as e:
                            logger.error(f"      ⚠️ Error creating DataFrame for table {idx} on page {page_num}: {str(e)}")
                            print(f"      ⚠️ Error creating DataFrame for table {idx} on page {page_num}: {str(e)}")
                    else:
                        logger.warning(f"      ⚠️ Table {idx} on page {page_num} has no data rows.")
                        print(f"      ⚠️ Table {idx} on page {page_num} has no data rows.")

    except Exception as e:
        logger.error(f"❌ Error processing PDF file {file_path} with pdfplumber: {str(e)}")
        print(f"❌ Error processing PDF file {file_path} with pdfplumber: {str(e)}")
        return []

    if not all_tables:
        logger.warning(f"❌ No valid tables found in {basename} using pdfplumber.")
        print(f"❌ No valid tables found in {basename} using pdfplumber.")
        return []

    # Save Excel
    excel_path = os.path.join(output_excel_folder, f"{basename}_extracted.xlsx") # Slightly different name to distinguish
    try:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for idx, df in enumerate(all_tables):
                # Ensure sheet name is valid (<= 31 chars, no invalid chars)
                sheet_name = f"Sheet_{idx+1}"
                try:
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                except Exception as e:
                    logger.warning(f"⚠️ Could not use sheet name '{sheet_name}'. Using default. Error: {e}")
                    df.to_excel(writer, index=False) # Writes to default sheet name (usually 'Sheet1', 'Sheet2', etc.)
        logger.info(f"✅ Excel saved: {excel_path}")
        print(f"✅ Excel saved: {excel_path}")
        return [excel_path] # Return path for confirmation
    except Exception as e:
        logger.error(f"❌ Error saving Excel file: {str(e)}")
        print(f"❌ Error saving Excel file: {str(e)}")
        return []

def main():
    """
    Main function: process all PDF files in the input folder using pdfplumber
    """
    logger.info(f"\n📂 Starting PDF Table Extraction (using pdfplumber) from: {input_folder}")
    print(f"\n📂 Starting PDF Table Extraction (using pdfplumber) from: {input_folder}")

    # Check input folder
    if not os.path.exists(input_folder):
        logger.error(f"❌ Input folder '{input_folder}' does not exist.")
        print(f"❌ Input folder '{input_folder}' does not exist.")
        return

    # Get all supported PDF files (this version focuses on PDFs)
    files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    if not files:
        logger.warning("❌ No PDF files found in the input folder.")
        print("❌ No PDF files found in the input folder.")
        return

    logger.info(f"Found {len(files)} PDF file(s) to process")
    print(f"Found {len(files)} PDF file(s) to process")

    processed_files = []
    # Process each PDF file
    for fname in files:
        fpath = os.path.join(input_folder, fname)
        basename = os.path.splitext(fname)[0]
        logger.info(f"\n🚀 Processing: {fname}")
        print(f"\n🚀 Processing: {fname}")
        result = process_pdf_with_pdfplumber(fpath, basename)
        if result: # If a file path was returned, it means it was processed successfully
             processed_files.extend(result)

    logger.info(f"\n🎉 Processing complete. Successfully processed files: {len(processed_files)}")
    print(f"\n🎉 Processing complete. Successfully processed files: {len(processed_files)}")
    if processed_files:
        logger.info("Processed files:")
        for f in processed_files:
            logger.info(f" - {f}")

if __name__ == "__main__":
    # Ensure pdfplumber is installed. You might need to run `pip install pdfplumber`
    # The import is already at the top.
    main()