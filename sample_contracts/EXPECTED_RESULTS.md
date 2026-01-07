# Expected Metadata Extraction Results for Sample Contracts

## Swedish Supply Agreement (swedish_supply_agreement.txt)

| Field | Expected Value | Notes |
|-------|---------------|-------|
| Customer (CK) Entity | Circle K Sverige AB | Swedish entity |
| Supplier Entity | Scandinavian Food Suppliers AB | |
| Effective Date | 2024-04-01 | Stated as "1 april 2024" |
| Expiration Date | null | "löper på obestämd tid" = indefinite term |
| Term Type | Evergreen | Runs until terminated |
| Governing Law | Swedish law | "svensk lag" |
| Contract Type | Supply Agreement | "Leveransavtal" |
| Contract Currency | SEK | Swedish Kronor |
| Payment Term | Net 60 | "60 dagar från fakturadatum" |
| Termination for Convenience | Yes | Both parties can terminate |
| Notice Period | 90 days | "90 dagars skriftlig varsel" |
| Party with Right to Terminate | Both parties | "båda parter" |

## Polish Supply Agreement (polish_supply_agreement.txt)

| Field | Expected Value | Notes |
|-------|---------------|-------|
| Customer (CK) Entity | Circle K Polska Sp. z o.o. | Polish entity |
| Supplier Entity | Polski Dostawca Żywności Sp. z o.o. | |
| Effective Date | 2024-04-01 | Stated as "1 kwietnia 2024" |
| Expiration Date | 2026-03-31 | 24 months from effective date |
| Term Type | Fixed term | 24 months, then auto-renews |
| Governing Law | Polish law | "prawo polskie" |
| Contract Type | Supply Agreement | "Umowa Dostawy" |
| Contract Currency | PLN | Polish Zloty |
| Payment Term | Net 45 | "45 dni od daty faktury" |
| Termination for Convenience | Yes | Both parties can terminate |
| Notice Period | 60 days | "60-dniowym wyprzedzeniem" |
| Party with Right to Terminate | Both parties | "obie strony" |

## Estonian Supply Agreement (estonian_supply_agreement.txt)

| Field | Expected Value | Notes |
|-------|---------------|-------|
| Customer (CK) Entity | Circle K Eesti AS | Estonian entity |
| Supplier Entity | Eesti Toidutarnijad OÜ | |
| Effective Date | 2024-04-01 | Stated as "1. aprillil 2024" |
| Expiration Date | null | "kehtib tähtajatult" = indefinite |
| Term Type | Evergreen | Until terminated |
| Governing Law | Estonian law | "Eesti õigus" |
| Contract Type | Supply Agreement | "Tarnekokkulepe" |
| Contract Currency | EUR | Euro |
| Payment Term | Net 30 | "30 päeva jooksul" |
| Termination for Convenience | Yes | Both parties can terminate |
| Notice Period | 60 days | "60-päevase" |
| Party with Right to Terminate | Both parties | "mõlema poole" |

## Testing Instructions

1. **Upload samples**: `python test_upload_samples.py`
2. **Translate**: `python batch_translate.py`
3. **Download**: `python download_translated.py`
4. **Verify**: Compare English translations in `translated_contracts/` folder with this reference

## Success Criteria

- ✅ All files translate without errors
- ✅ Formatting is preserved (headings, articles, signatures)
- ✅ Legal terminology is accurate
- ✅ Company names remain unchanged
- ✅ Dates are correctly translated
- ✅ Numbers and currencies are preserved

## Common Translation Checks

**Swedish → English**:
- "Leveransavtal" → "Supply Agreement"
- "träder i kraft" → "enters into force"
- "löper på obestämd tid" → "runs indefinitely"

**Polish → English**:
- "Umowa Dostawy" → "Supply Agreement"
- "wchodzi w życie" → "enters into force"
- "wypowiedzenie" → "termination"

**Estonian → English**:
- "Tarnekokkulepe" → "Supply Agreement"
- "jõustub" → "enters into force"
- "tähtajatult" → "indefinitely"
