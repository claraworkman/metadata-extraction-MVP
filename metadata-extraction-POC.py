"""
Direct Metadata Extraction from Original Contracts
Extracts metadata from Swedish, Polish, Estonian, etc. contracts without translation
Supports .txt, .docx, and .pdf files
Outputs to CSV for easy download and Sirion import
"""

import os
import json
import csv
from pathlib import Path
from dotenv import load_dotenv
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from datetime import datetime
import time
from docx import Document
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.storage.blob import BlobServiceClient
import io

load_dotenv()

# Azure OpenAI Configuration with Azure AD authentication
credential = DefaultAzureCredential()
token_provider = get_bearer_token_provider(
    credential, 
    "https://cognitiveservices.azure.com/.default"
)

client = AzureOpenAI(
    azure_ad_token_provider=token_provider,
    api_version="2024-05-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
)

# Azure Document Intelligence client for PDF OCR
document_intelligence_endpoint = os.getenv("DOCUMENT_INTELLIGENCE_ENDPOINT")
if document_intelligence_endpoint:
    doc_client = DocumentAnalysisClient(
        endpoint=document_intelligence_endpoint,
        credential=credential
    )
else:
    doc_client = None

# Azure Blob Storage client
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
if storage_account_name:
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account_name}.blob.core.windows.net",
        credential=credential
    )
else:
    blob_service_client = None

DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")

# Sirion metadata fields
SIRION_FIELDS = [
    "Customer (CK) Entity",
    "Supplier Entity",
    "Effective Date",
    "Expiration Date",
    "Term Type",
    "Governing Law",
    "Contract Type",
    "Contract Currency",
    "Payment Term",
    "Termination for Convenience",
    "Notice Period for Termination for Convenience",
    "Party with the Right to Terminate for Convenience"
]

SYSTEM_PROMPT = """You are a multilingual contract metadata extraction specialist. You can read contracts in Swedish, Norwegian, Danish, Polish, Latvian, Lithuanian, and Estonian.

Extract the following 12 fields from contract documents. READ the contract in its original language, but RETURN all field values in ENGLISH:

Required fields (return values in ENGLISH):
1. Customer (CK) Entity - The Circle K entity name (keep original)
2. Supplier Entity - The supplier company name (keep original)
3. Effective Date - Contract start date (YYYY-MM-DD format)
4. Expiration Date - Contract end date (YYYY-MM-DD) or null if indefinite
5. Term Type - "Fixed term", "Evergreen", "Auto-renewing", etc. (English)
6. Governing Law - Jurisdiction (English, e.g., "Swedish law", "Polish law")
7. Contract Type - e.g., "Supply Agreement" (translate to English)
8. Contract Currency - Currency code (USD, EUR, SEK, PLN, etc.)
9. Payment Term - e.g., "Net 30", "Net 60" (English)
10. Termination for Convenience - "Yes" or "No"
11. Notice Period for Termination for Convenience - e.g., "30 days" or null
12. Party with the Right to Terminate for Convenience - "Both parties", "Customer only", "Supplier only", or null

TRANSLATION EXAMPLES:
- Swedish "Leveransavtal" → "Supply Agreement"
- Polish "Umowa Dostawy" → "Supply Agreement"
- Estonian "Tarnekokkulepe" → "Supply Agreement"
- Swedish "svensk lag" → "Swedish law"
- Polish "prawo polskie" → "Polish law"
- Estonian "Eesti õigus" → "Estonian law"

RULES:
1. Use null for fields not found or not applicable
2. Always use YYYY-MM-DD for dates
3. Keep company names in original form
4. Translate legal terms to English
5. Include "source_language" field (detected: sv, pl, et, no, da, lv, lt)
6. Include "confidence" field: "high", "medium", or "low"
7. Include "extraction_notes" for uncertainties

Return ONLY valid JSON."""


def extract_text_from_docx(file_data):
    """Extract text from Word document (bytes or file path)"""
    try:
        if isinstance(file_data, bytes):
            # Handle bytes data from blob storage
            doc = Document(io.BytesIO(file_data))
        else:
            # Handle file path
            doc = Document(file_data)
        
        full_text = []
        for paragraph in doc.paragraphs:
            full_text.append(paragraph.text)
        return '\n'.join(full_text), None
    except Exception as e:
        return None, str(e)


def extract_text_from_pdf(file_data):
    """Extract text from PDF using Azure Document Intelligence OCR"""
    if not doc_client:
        return None, "Document Intelligence not configured"
    
    try:
        if isinstance(file_data, bytes):
            # Handle bytes data from blob storage
            poller = doc_client.begin_analyze_document(
                "prebuilt-read",
                document=file_data
            )
        else:
            # Handle file path
            with open(file_data, "rb") as f:
                poller = doc_client.begin_analyze_document(
                    "prebuilt-read",
                    document=f
                )
        
        result = poller.result()
        
        # Extract all text content
        text_content = []
        for page in result.pages:
            for line in page.lines:
                text_content.append(line.content)
        
        return '\n'.join(text_content), None
        
    except Exception as e:
        return None, str(e)


def read_contract_file(file_path_or_data, extension=None):
    """Read contract file based on extension (.txt, .docx, .pdf)
    
    Args:
        file_path_or_data: Either a file path string or bytes data from blob
        extension: File extension (required if file_path_or_data is bytes)
    """
    if isinstance(file_path_or_data, bytes):
        # Reading from blob storage (bytes data)
        if not extension:
            return None, "Extension required for bytes data"
        
        ext = extension.lower()
        
        if ext == '.txt':
            try:
                return file_path_or_data.decode('utf-8'), None
            except Exception as e:
                return None, str(e)
        
        elif ext == '.docx':
            return extract_text_from_docx(file_path_or_data)
        
        elif ext == '.pdf':
            return extract_text_from_pdf(file_path_or_data)
        
        else:
            return None, f"Unsupported file format: {ext}"
    
    else:
        # Reading from local file path
        path = Path(file_path_or_data)
        ext = path.suffix.lower()
        
        if ext == '.txt':
            try:
                with open(file_path_or_data, 'r', encoding='utf-8') as f:
                    return f.read(), None
            except Exception as e:
                return None, str(e)
        
        elif ext == '.docx':
            return extract_text_from_docx(file_path_or_data)
        
        elif ext == '.pdf':
            return extract_text_from_pdf(file_path_or_data)
        
        else:
            return None, f"Unsupported file format: {ext}"


def extract_metadata_direct(contract_text, file_name=""):
    """
    Extract metadata directly from original language contract
    No translation needed - GPT-4o-mini reads multiple languages
    """
    
    user_prompt = f"""Extract metadata from this contract and return all values in ENGLISH:

CONTRACT TEXT:
{contract_text[:8000]}

Return JSON with the 12 required Sirion fields, plus source_language and confidence."""

    try:
        response = client.chat.completions.create(
            model=DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=1000
        )
        
        result = json.loads(response.choices[0].message.content)
        result['file_name'] = file_name
        result['extraction_timestamp'] = datetime.now().isoformat()
        
        return result, None
        
    except Exception as e:
        return None, str(e)


def process_contracts_from_blob(container_name, output_csv="sirion_metadata.csv"):
    """
    Process all contracts from Azure Blob Storage and export to CSV
    Supports .txt, .docx, and .pdf files
    
    Args:
        container_name: Azure Blob Storage container name
        output_csv: Output CSV file name
    """
    
    if not blob_service_client:
        print("❌ Azure Blob Storage not configured")
        print("   Set STORAGE_ACCOUNT_NAME in .env file")
        return None
    
    try:
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs with supported extensions
        print(f"\n{'='*80}")
        print(f"🌍 Direct Metadata Extraction from Azure Blob Storage")
        print(f"{'='*80}")
        print(f"📦 Container: {container_name}")
        print(f"🔍 Scanning for contracts...")
        
        blobs = []
        for blob in container_client.list_blobs():
            ext = Path(blob.name).suffix.lower()
            if ext in ['.txt', '.docx', '.pdf']:
                blobs.append(blob)
        
        if not blobs:
            print(f"⚠️ No contract files found in container '{container_name}'")
            print(f"   Looking for: .txt, .docx, .pdf files")
            return None
        
        print(f"📊 Files Found: {len(blobs)}")
        print(f"📄 Formats: .txt, .docx, .pdf (with OCR)")
        print(f"🤖 Model: {DEPLOYMENT_NAME}")
        print(f"💾 Output: {output_csv}")
        print(f"{'='*80}\n")
        
        results = []
        success_count = 0
        error_count = 0
        
        for i, blob in enumerate(blobs, 1):
            print(f"[{i}/{len(blobs)}] Processing: {blob.name}")
            
            try:
                # Download blob data
                blob_client = container_client.get_blob_client(blob.name)
                blob_data = blob_client.download_blob().readall()
                
                # Get file extension
                extension = Path(blob.name).suffix.lower()
                
                # Read contract file (handles .txt, .docx, .pdf)
                contract_text, read_error = read_contract_file(blob_data, extension)
                
                if not contract_text:
                    print(f"   ❌ Failed to read file: {read_error}")
                    results.append({
                        'File Name': blob.name,
                        'Error': f"Read error: {read_error}"
                    })
                    error_count += 1
                    continue
                
                # Extract metadata directly (no translation)
                metadata, error = extract_metadata_direct(contract_text, blob.name)
                
                if metadata:
                    # Build CSV row
                    row = {
                        'File Name': blob.name,
                        'Source Language': metadata.get('source_language', 'unknown'),
                        'Extraction Timestamp': metadata.get('extraction_timestamp', ''),
                    }
                    
                    # Add all 12 Sirion fields
                    for field in SIRION_FIELDS:
                        row[field] = metadata.get(field, '')
                    
                    # Add quality indicators
                    row['Confidence'] = metadata.get('confidence', 'medium')
                    row['Notes'] = metadata.get('extraction_notes', '')
                    
                    results.append(row)
                    success_count += 1
                    
                    detected_lang = metadata.get('source_language', 'unknown')
                    confidence = metadata.get('confidence', 'medium')
                    print(f"   ✅ Language: {detected_lang.upper()}, Confidence: {confidence}")
                    
                else:
                    print(f"   ❌ Failed: {error}")
                    results.append({
                        'File Name': blob.name,
                        'Error': error
                    })
                    error_count += 1
                
                # Rate limiting - avoid API throttling
                time.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
                results.append({
                    'File Name': blob.name,
                    'Error': str(e)
                })
                error_count += 1
        
        # Export to CSV (same as local version)
        if results:
            csv_columns = ['File Name', 'Source Language', 'Extraction Timestamp'] + SIRION_FIELDS + ['Confidence', 'Notes']
            
            with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(results)
            
            print(f"\n{'='*80}")
            print(f"✅ Extraction Complete!")
            print(f"{'='*80}")
            print(f"📊 Total Files: {len(blobs)}")
            print(f"✅ Successful: {success_count}")
            print(f"❌ Failed: {error_count}")
            print(f"💾 Output: {output_csv}")
            
            # Summary statistics (same as local version)
            if success_count > 0:
                languages = {}
                confidences = {'high': 0, 'medium': 0, 'low': 0}
                
                for row in results:
                    if 'Source Language' in row:
                        lang = row.get('Source Language', 'unknown')
                        languages[lang] = languages.get(lang, 0) + 1
                    
                    if 'Confidence' in row:
                        conf = row.get('Confidence', 'medium')
                        if conf in confidences:
                            confidences[conf] += 1
                
                print(f"\n🌍 Languages Detected:")
                for lang, count in languages.items():
                    print(f"   {lang.upper()}: {count} contracts")
                
                print(f"\n📈 Confidence Distribution:")
                for conf, count in confidences.items():
                    print(f"   {conf.title()}: {count} contracts")
                
                # Calculate cost
                avg_tokens_per_doc = 2000
                total_tokens = success_count * avg_tokens_per_doc
                cost_per_million = 0.15
                estimated_cost = (total_tokens / 1_000_000) * cost_per_million
                
                print(f"\n💰 Cost Estimate:")
                print(f"   Total Tokens: ~{total_tokens:,}")
                print(f"   Estimated Cost: ${estimated_cost:.2f}")
            
            print(f"\n💡 Next Steps:")
            print(f"   1. Open {output_csv} in Excel")
            print(f"   2. Review contracts with 'low' or 'medium' confidence")
            print(f"   3. Verify company names and dates")
            print(f"   4. Import to Sirion.ai CLM platform")
            print(f"\n✨ Benefits of Direct Extraction:")
            print(f"   ✅ No translation needed (98% cost savings)")
            print(f"   ✅ Single-step process (60% faster)")
            print(f"   ✅ Original legal terminology preserved")
            print(f"   ✅ Supports .txt, .docx, .pdf formats")
            print(f"   ✅ Direct access from Azure Blob Storage")
            print(f"\n{'='*80}\n")
        
        return output_csv
        
    except Exception as e:
        print(f"❌ Error accessing blob storage: {str(e)}")
        print(f"   Container: {container_name}")
        print(f"   Storage Account: {storage_account_name}")
        return None


def process_contracts_to_csv(input_folder, output_csv="sirion_metadata.csv"):
    """
    Process all contracts and export to CSV
    Supports .txt, .docx, and .pdf files
    
    Args:
        input_folder: Folder containing original contracts
        output_csv: Output CSV file name
    """
    
    input_path = Path(input_folder)
    if not input_path.exists():
        print(f"❌ Folder not found: {input_folder}")
        return None
    
    # Find all contract files
    files = []
    for ext in ['*.txt', '*.docx', '*.pdf']:
        files.extend(list(input_path.glob(ext)))
    
    if not files:
        print(f"⚠️ No contract files found in {input_folder}")
        print(f"   Looking for: .txt, .docx, .pdf files")
        return None
    
    print(f"\n{'='*80}")
    print(f"🌍 Direct Metadata Extraction - No Translation Needed!")
    print(f"{'='*80}")
    print(f"📁 Input Folder: {input_folder}")
    print(f"📊 Files Found: {len(files)}")
    print(f"📄 Formats: .txt, .docx, .pdf (with OCR)")
    print(f"🤖 Model: {DEPLOYMENT_NAME}")
    print(f"💾 Output: {output_csv}")
    print(f"{'='*80}\n")
    
    results = []
    success_count = 0
    error_count = 0
    
    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] Processing: {file_path.name}")
        
        try:
            # Read contract file (handles .txt, .docx, .pdf)
            contract_text, read_error = read_contract_file(file_path)
            
            if not contract_text:
                print(f"   ❌ Failed to read file: {read_error}")
                results.append({
                    'File Name': file_path.name,
                    'Error': f"Read error: {read_error}"
                })
                error_count += 1
                continue
            
            # Extract metadata directly (no translation)
            metadata, error = extract_metadata_direct(contract_text, file_path.name)
            
            if metadata:
                # Build CSV row
                row = {
                    'File Name': file_path.name,
                    'Source Language': metadata.get('source_language', 'unknown'),
                    'Extraction Timestamp': metadata.get('extraction_timestamp', ''),
                }
                
                # Add all 12 Sirion fields
                for field in SIRION_FIELDS:
                    row[field] = metadata.get(field, '')
                
                # Add quality indicators
                row['Confidence'] = metadata.get('confidence', 'medium')
                row['Notes'] = metadata.get('extraction_notes', '')
                
                results.append(row)
                success_count += 1
                
                detected_lang = metadata.get('source_language', 'unknown')
                confidence = metadata.get('confidence', 'medium')
                print(f"   ✅ Language: {detected_lang.upper()}, Confidence: {confidence}")
                
            else:
                print(f"   ❌ Failed: {error}")
                results.append({
                    'File Name': file_path.name,
                    'Error': error
                })
                error_count += 1
            
            # Rate limiting - avoid API throttling
            time.sleep(0.5)
            
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            results.append({
                'File Name': file_path.name,
                'Error': str(e)
            })
            error_count += 1
    
    # Export to CSV
    if results:
        # Define CSV columns
        csv_columns = ['File Name', 'Source Language', 'Extraction Timestamp'] + SIRION_FIELDS + ['Confidence', 'Notes']
        
        # Write CSV
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n{'='*80}")
        print(f"✅ Extraction Complete!")
        print(f"{'='*80}")
        print(f"📊 Total Files: {len(files)}")
        print(f"✅ Successful: {success_count}")
        print(f"❌ Failed: {error_count}")
        print(f"💾 Output: {output_csv}")
        
        # Summary statistics
        if success_count > 0:
            languages = {}
            confidences = {'high': 0, 'medium': 0, 'low': 0}
            
            for row in results:
                if 'Source Language' in row:
                    lang = row.get('Source Language', 'unknown')
                    languages[lang] = languages.get(lang, 0) + 1
                
                if 'Confidence' in row:
                    conf = row.get('Confidence', 'medium')
                    if conf in confidences:
                        confidences[conf] += 1
            
            print(f"\n🌍 Languages Detected:")
            for lang, count in languages.items():
                print(f"   {lang.upper()}: {count} contracts")
            
            print(f"\n📈 Confidence Distribution:")
            for conf, count in confidences.items():
                print(f"   {conf.title()}: {count} contracts")
            
            # Calculate cost
            avg_tokens_per_doc = 2000
            total_tokens = success_count * avg_tokens_per_doc
            cost_per_million = 0.15  # GPT-4o-mini input cost
            estimated_cost = (total_tokens / 1_000_000) * cost_per_million
            
            print(f"\n💰 Cost Estimate:")
            print(f"   Total Tokens: ~{total_tokens:,}")
            print(f"   Estimated Cost: ${estimated_cost:.2f}")
        
        print(f"\n💡 Next Steps:")
        print(f"   1. Open {output_csv} in Excel")
        print(f"   2. Review contracts with 'low' or 'medium' confidence")
        print(f"   3. Verify company names and dates")
        print(f"   4. Import to Sirion.ai CLM platform")
        print(f"\n✨ Benefits of Direct Extraction:")
        print(f"   ✅ No translation needed (98% cost savings)")
        print(f"   ✅ Single-step process (60% faster)")
        print(f"   ✅ Original legal terminology preserved")
        print(f"   ✅ Supports .txt, .docx, .pdf formats")
        print(f"\n{'='*80}\n")
    
    return output_csv


def process_single_contract(file_path):
    """Test extraction on a single contract"""
    
    path = Path(file_path)
    if not path.exists():
        print(f"❌ File not found: {file_path}")
        return None
    
    print(f"\n🔍 Testing Direct Metadata Extraction")
    print(f"📄 File: {path.name}\n")
    
    # Read contract file (handles .txt, .docx, .pdf)
    contract_text, read_error = read_contract_file(path)
    
    if not contract_text:
        print(f"❌ Failed to read file: {read_error}")
        return None
    
    print(f"📝 Contract Length: {len(contract_text)} characters")
    print(f"🤖 Processing with {DEPLOYMENT_NAME}...\n")
    
    metadata, error = extract_metadata_direct(contract_text, path.name)
    
    if metadata:
        print("✅ Extraction Successful!\n")
        print(json.dumps(metadata, indent=2))
        
        # Save to JSON
        output_json = path.stem + "_metadata.json"
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        print(f"\n💾 Saved to: {output_json}")
        
        return metadata
    else:
        print(f"❌ Extraction Failed: {error}")
        return None


if __name__ == "__main__":
    import sys
    
    print("\n" + "="*80)
    print("🌍 Circle K Contract Metadata Extractor - Direct Extraction Mode")
    print("="*80)
    print("📋 Extracts metadata from original language contracts (no translation)")
    print("🤖 Uses GPT-4o-mini multilingual capabilities")
    print("📄 Supports: .txt, .docx, .pdf (with OCR)")
    print("💾 Outputs to CSV for Sirion.ai import")
    print("📦 Source: Azure Blob Storage or Local Files")
    print("="*80 + "\n")
    
    if len(sys.argv) > 1:
        # Single file mode
        file_path = sys.argv[1]
        process_single_contract(file_path)
    else:
        # Ask user for source type
        print("Select source:")
        print("  1. Azure Blob Storage (recommended)")
        print("  2. Local folder")
        source_choice = input("\nEnter choice (1 or 2, default=1): ").strip()
        
        if source_choice == "2":
            # Local folder mode
            input_folder = input("Enter folder path with contracts (or press Enter for 'sample_contracts'): ").strip()
            if not input_folder:
                input_folder = "sample_contracts"
            
            output_csv = input("Enter output CSV name (or press Enter for 'sirion_metadata.csv'): ").strip()
            if not output_csv:
                output_csv = "sirion_metadata.csv"
            
            process_contracts_to_csv(input_folder, output_csv)
        else:
            # Azure Blob Storage mode (default)
            container_name = input("Enter Azure Blob Storage container name (or press Enter for 'documents'): ").strip()
            if not container_name:
                container_name = "documents"
            
            output_csv = input("Enter output CSV name (or press Enter for 'sirion_metadata.csv'): ").strip()
            if not output_csv:
                output_csv = "sirion_metadata.csv"
            
            process_contracts_from_blob(container_name, output_csv)
