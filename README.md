# Metadata Extraction POC

Extract contract metadata directly from multilingual contracts (Swedish, Polish, Estonian, Norwegian, Danish, Latvian, Lithuanian) without translation.

## 🎯 What This Does

- Reads contracts from **Azure Blob Storage** or local files
- Supports `.txt`, `.docx`, or `.pdf` formats
- Extracts 12 Sirion metadata fields using GPT-4o-mini
- **Parallel processing**: 10-20x faster with concurrent workers
- **Smart retry logic**: Handles rate limits automatically
- Outputs structured CSV ready for Sirion.ai import
- Processes contracts in original language, returns English metadata
- No translation step needed (98% cost savings)

## ⚡ Performance

- **Sequential Mode**: ~3 hours for 6,000 contracts
- **Parallel Mode (10 workers)**: ~20 minutes for 6,000 contracts
- **Cost**: Same $2 total (parallel doesn't increase API calls)
- **Retry Logic**: Automatically handles rate limits and transient errors

## 📋 Requirements

- Python 3.8+
- Azure OpenAI access (gpt-4o-mini deployment)
- Azure Blob Storage account (for cloud storage)
- Azure Document Intelligence (for PDF OCR)
- Azure CLI installed and authenticated (`az login`)

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Azure Services
Edit `.env` file with your Azure endpoints:
```ini
AZURE_OPENAI_ENDPOINT=https://your-openai-endpoint.cognitiveservices.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4o-mini
DOCUMENT_INTELLIGENCE_ENDPOINT=https://your-doc-intel-endpoint.cognitiveservices.azure.com/
STORAGE_ACCOUNT_NAME=yourstorageaccount

# Optional: Parallel processing configuration
MAX_WORKERS=10        # Concurrent workers (default: 10)
MAX_RETRIES=3         # Retry attempts per file (default: 3)
RETRY_DELAY=2         # Seconds between retries (default: 2)
```

**Parallel Processing Settings**:
- `MAX_WORKERS=10`: Process 10 files simultaneously (adjust based on rate limits)
- `MAX_RETRIES=3`: Retry failed files up to 3 times with exponential backoff
- `RETRY_DELAY=2`: Initial delay between retries (doubles each attempt)

### 3. Login to Azure
```bash
az login
```

### 4. Run the Script
```bash
python metadata-extraction-POC.py
```

When prompted:
1. **Select source**: 
   - `1` for Azure Blob Storage (recommended)
   - `2` for local folder
2. **Container/Folder**: Enter container name or folder path
3. **Output CSV**: Enter filename (or press Enter for default)

## 📦 Azure Blob Storage Mode (Recommended)

### Upload Contracts to Blob Storage
```bash
# Using Azure CLI
az storage blob upload-batch \
  --account-name stcirclektranslation \
  --destination documents \
  --source ./contracts/ \
  --auth-mode login
```

### Run Extraction
```bash
python metadata-extraction-POC.py

Select source:
  1. Azure Blob Storage (recommended)
  2. Local folder
Enter choice: 1

Enter Azure Blob Storage container name: documents
Enter output CSV name: sirion_metadata.csv
```

The script will:
- Connect to Azure Blob Storage
- List all .txt, .docx, .pdf files
- Download and process each file
- Extract metadata directly
- Create CSV output

## 💻 Local Folder Mode

For testing or small batches:
```bash
python metadata-extraction-POC.py

Select source:
  1. Azure Blob Storage (recommended)
  2. Local folder
Enter choice: 2

Enter folder path: sample_contracts
Enter output CSV name: test.csv
```

## 📊 Output

The script creates a CSV file with:
- File name
- Source language detected
- 12 Sirion metadata fields (in English)
- Confidence score (high/medium/low)
- Extraction notes

### Fields Extracted:
1. Customer (CK) Entity
2. Supplier Entity
3. Effective Date
4. Expiration Date
5. Term Type
6. Governing Law
7. Contract Type
8. Contract Currency
9. Payment Term
10. Termination for Convenience
11. Notice Period for Termination for Convenience
12. Party with the Right to Terminate for Convenience

## 💰 Cost

- **~$0.0003 per contract** (using GPT-4o-mini)
- **6,000 contracts ≈ $2 total**

## ⏱️ Speed

- **.txt files**: 1-2 seconds each
- **.docx files**: 2-3 seconds each
- **.pdf files**: 5-10 seconds each (OCR)

## 🌍 Supported Languages

- Swedish (sv)
- Norwegian (no)
- Danish (da)
- Polish (pl)
- Latvian (lv)
- Lithuanian (lt)
- Estonian (et)

## 📁 File Structure

```
metadata-extraction-poc/
├── metadata-extraction-POC.py    # Main script
├── requirements.txt               # Python dependencies
├── .env                          # Azure configuration
└── README.md                     # This file
```

## 🔧 Advanced Usage

### Test Single Contract
```bash
python metadata-extraction-POC.py "path/to/contract.txt"
```

### Process Specific Folder
```bash
python metadata-extraction-POC.py
# Enter: C:\Contracts\Batch1
# Enter: batch1_output.csv
```

## ⚠️ Troubleshooting

**"Authentication failed"**
- Run `az login` to refresh credentials

**"Module not found"**
- Run `pip install -r requirements.txt`

**"Folder not found"**
- Check folder path exists and has read permissions

**PDF returns blank**
- Ensure PDF has selectable text (not just scanned image)

## ✅ Next Steps

1. Review generated CSV in Excel
2. Check contracts with "medium" or "low" confidence
3. Verify company names and dates
4. Import to Sirion.ai CLM platform

## 📞 Support

For issues or questions about this POC, contact the AI team.
