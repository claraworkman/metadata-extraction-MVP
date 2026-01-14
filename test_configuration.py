"""
Test script to validate the Related Master Agreement field is properly configured
"""
import sys
sys.path.insert(0, ".")

# Import the SIRION_FIELDS from the script
from managed_identity_metadata_extraction_POC import SIRION_FIELDS, SYSTEM_PROMPT

print("\n" + "="*80)
print("🧪 Testing Configuration Changes")
print("="*80)

# Test 1: Check if Related Master Agreement field exists
print("\n✅ TEST 1: Check SIRION_FIELDS includes 'Related Master Agreement'")
print(f"Total fields: {len(SIRION_FIELDS)}")
print("\nFields list:")
for i, field in enumerate(SIRION_FIELDS, 1):
    marker = "← NEW FIELD" if field == "Related Master Agreement" else ""
    print(f"  {i}. {field} {marker}")

if "Related Master Agreement" in SIRION_FIELDS:
    print("\n✅ PASS: 'Related Master Agreement' field found in SIRION_FIELDS")
else:
    print("\n❌ FAIL: 'Related Master Agreement' field NOT found in SIRION_FIELDS")
    sys.exit(1)

# Test 2: Check if SYSTEM_PROMPT mentions the new field
print("\n✅ TEST 2: Check SYSTEM_PROMPT includes instructions for 'Related Master Agreement'")
if "Related Master Agreement" in SYSTEM_PROMPT:
    print("✅ PASS: 'Related Master Agreement' instructions found in SYSTEM_PROMPT")
    # Extract the relevant section
    lines = SYSTEM_PROMPT.split('\n')
    for i, line in enumerate(lines):
        if 'Related Master Agreement' in line:
            print(f"\n📋 Instruction found at line {i}:")
            print(f"   {line.strip()}")
else:
    print("❌ FAIL: 'Related Master Agreement' instructions NOT found in SYSTEM_PROMPT")
    sys.exit(1)

# Test 3: Verify field count
print("\n✅ TEST 3: Verify correct field count")
expected_count = 12  # Original 11 + 1 new field
if len(SIRION_FIELDS) == expected_count:
    print(f"✅ PASS: Field count is correct ({expected_count} fields)")
else:
    print(f"❌ FAIL: Expected {expected_count} fields, found {len(SIRION_FIELDS)}")
    sys.exit(1)

print("\n" + "="*80)
print("🎉 ALL TESTS PASSED!")
print("="*80)
print("\n📝 Summary:")
print("   - 'Related Master Agreement' field added to SIRION_FIELDS")
print("   - SYSTEM_PROMPT updated with extraction instructions")
print("   - Total Sirion fields: 12 (was 11)")
print("\n💡 The script is ready to extract master agreement relationships!")
print("="*80 + "\n")
