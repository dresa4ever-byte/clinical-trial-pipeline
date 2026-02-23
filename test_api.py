"""
=============================================================================
  ClinicalTrials.gov API v2 — TEST & EXPLORATION SCRIPT
=============================================================================
  Purpose:  Test the API, see the response structure, and compare with CSV data.
  
  How to run:  Open in Spyder → Run (F5)
  
  Prerequisites:  pip install requests
  
  Rate Limit: ~50 requests/minute (no authentication needed)
=============================================================================
"""

import requests
import json
import time


BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


# =============================================================================
# TEST 1: Basic API call — get a few studies
# =============================================================================
section("TEST 1: Basic API call (3 studies)")

response = requests.get(
    BASE_URL,
    params={
        "format": "json",
        "pageSize": 3,
        "countTotal": "true",
    },
    timeout=30,
)

print(f"Status code: {response.status_code}")
print(f"Content type: {response.headers.get('content-type')}")

data = response.json()
print(f"Total studies available: {data.get('totalCount', 'N/A'):,}")
print(f"Studies returned: {len(data.get('studies', []))}")

# Show the top-level keys
if data.get("studies"):
    first_study = data["studies"][0]
    print(f"\nTop-level keys in a study object:")
    for key in first_study.keys():
        print(f"  → {key}")


# =============================================================================
# TEST 2: Explore the structure of ONE study in detail
# =============================================================================
section("TEST 2: Detailed structure of one study")

if data.get("studies"):
    study = data["studies"][0]
    
    # protocolSection is where most of our data lives
    protocol = study.get("protocolSection", {})
    
    print("protocolSection contains these modules:")
    for key in protocol.keys():
        print(f"  → {key}")
    
    # --- Identification Module ---
    ident = protocol.get("identificationModule", {})
    print(f"\n--- identificationModule ---")
    print(f"  NCT ID:     {ident.get('nctId', 'N/A')}")
    print(f"  Brief Title: {ident.get('briefTitle', 'N/A')[:80]}...")
    print(f"  Full Title:  {ident.get('officialTitle', 'N/A')[:80] if ident.get('officialTitle') else 'N/A'}...")
    
    # --- Organization ---
    org = ident.get("organization", {})
    print(f"\n--- organization ---")
    print(f"  Org Name:  {org.get('fullName', 'N/A')}")
    print(f"  Org Class: {org.get('class', 'N/A')}")
    
    # --- Status Module ---
    status = protocol.get("statusModule", {})
    print(f"\n--- statusModule ---")
    print(f"  Overall Status: {status.get('overallStatus', 'N/A')}")
    start = status.get("startDateStruct", {})
    print(f"  Start Date:     {start.get('date', 'N/A')} (type: {start.get('type', 'N/A')})")
    
    # --- Design Module ---
    design = protocol.get("designModule", {})
    print(f"\n--- designModule ---")
    print(f"  Study Type: {design.get('studyType', 'N/A')}")
    phases = design.get("phases", [])
    print(f"  Phases:     {phases}")
    
    enrollment = design.get("enrollmentInfo", {})
    print(f"  Enrollment: {enrollment.get('count', 'N/A')} ({enrollment.get('type', 'N/A')})")
    
    # --- Conditions Module ---
    cond_module = protocol.get("conditionsModule", {})
    print(f"\n--- conditionsModule ---")
    conditions = cond_module.get("conditions", [])
    print(f"  Conditions: {conditions}")
    mesh_terms = cond_module.get("keywords", [])
    print(f"  Keywords: {mesh_terms}")
    
    # --- Interventions Module ---
    interv_module = protocol.get("armsInterventionsModule", {})
    interventions = interv_module.get("interventions", [])
    print(f"\n--- armsInterventionsModule ---")
    print(f"  Interventions ({len(interventions)}):")
    for inv in interventions[:3]:
        print(f"    → {inv.get('type', '?')}: {inv.get('name', '?')}")
        if inv.get("description"):
            print(f"      Description: {inv['description'][:80]}...")
    
    # --- Eligibility Module ---
    elig = protocol.get("eligibilityModule", {})
    print(f"\n--- eligibilityModule ---")
    print(f"  Min Age: {elig.get('minimumAge', 'N/A')}")
    print(f"  Max Age: {elig.get('maximumAge', 'N/A')}")
    print(f"  Sex:     {elig.get('sex', 'N/A')}")
    std_ages = elig.get("stdAges", [])
    print(f"  Std Ages: {std_ages}")
    
    # --- Outcomes Module ---
    outcomes = protocol.get("outcomesModule", {})
    primary = outcomes.get("primaryOutcomes", [])
    print(f"\n--- outcomesModule ---")
    print(f"  Primary Outcomes ({len(primary)}):")
    for o in primary[:2]:
        print(f"    → {o.get('measure', 'N/A')[:80]}...")
    
    # --- Contacts/Locations Module ---
    contacts = protocol.get("contactsLocationsModule", {})
    locations = contacts.get("locations", [])
    print(f"\n--- contactsLocationsModule ---")
    print(f"  Locations ({len(locations)}):")
    for loc in locations[:3]:
        print(f"    → {loc.get('facility', '?')}, {loc.get('city', '?')}, {loc.get('country', '?')}")
    
    # --- Sponsor Module ---
    sponsor = protocol.get("sponsorCollaboratorsModule", {})
    lead = sponsor.get("leadSponsor", {})
    print(f"\n--- sponsorCollaboratorsModule ---")
    print(f"  Lead Sponsor: {lead.get('name', 'N/A')}")
    print(f"  Sponsor Class: {lead.get('class', 'N/A')}")
    resp_party = sponsor.get("responsibleParty", {})
    print(f"  Responsible Party Type: {resp_party.get('type', 'N/A')}")


# =============================================================================
# TEST 3: Search with filters (like the CSV data)
# =============================================================================
section("TEST 3: Filtered search — Breast Cancer, Phase 2, Recruiting")

response2 = requests.get(
    BASE_URL,
    params={
        "format": "json",
        "query.cond": "Breast Cancer",
        "filter.overallStatus": "RECRUITING",
        "filter.phase": "PHASE2",
        "pageSize": 5,
        "countTotal": "true",
    },
    timeout=30,
)

data2 = response2.json()
print(f"Total matching studies: {data2.get('totalCount', 'N/A'):,}")
print(f"Returned: {len(data2.get('studies', []))}")

for s in data2.get("studies", [])[:3]:
    p = s.get("protocolSection", {})
    ident = p.get("identificationModule", {})
    status = p.get("statusModule", {})
    design = p.get("designModule", {})
    print(f"\n  {ident.get('nctId', '?')}: {ident.get('briefTitle', '?')[:60]}...")
    print(f"    Status: {status.get('overallStatus')}, Phase: {design.get('phases', [])}")


# =============================================================================
# TEST 4: Pagination — how to get multiple pages
# =============================================================================
section("TEST 4: Pagination test")

# First page
resp_page1 = requests.get(
    BASE_URL,
    params={
        "format": "json",
        "query.cond": "Diabetes",
        "pageSize": 3,
        "countTotal": "true",
    },
    timeout=30,
)

page1 = resp_page1.json()
print(f"Total matching: {page1.get('totalCount', 'N/A'):,}")
print(f"Page 1 studies: {len(page1.get('studies', []))}")
next_token = page1.get("nextPageToken")
print(f"Next page token: {next_token}")

# Second page (using pageToken)
if next_token:
    time.sleep(1)  # respect rate limits
    resp_page2 = requests.get(
        BASE_URL,
        params={
            "format": "json",
            "query.cond": "Diabetes",
            "pageSize": 3,
            "pageToken": next_token,
        },
        timeout=30,
    )
    page2 = resp_page2.json()
    print(f"Page 2 studies: {len(page2.get('studies', []))}")
    print(f"Next token exists: {'nextPageToken' in page2}")


# =============================================================================
# TEST 5: Mapping API fields → our CSV columns
# =============================================================================
section("TEST 5: FIELD MAPPING — API vs CSV")

print("""
This is how API fields map to our CSV columns:

CSV Column                    →  API JSON Path
─────────────────────────────────────────────────────────────────
Organization Full Name        →  protocolSection.identificationModule.organization.fullName
Organization Class            →  protocolSection.identificationModule.organization.class
Responsible Party             →  protocolSection.sponsorCollaboratorsModule.responsibleParty.type
Brief Title                   →  protocolSection.identificationModule.briefTitle
Full Title                    →  protocolSection.identificationModule.officialTitle
Overall Status                →  protocolSection.statusModule.overallStatus
Start Date                    →  protocolSection.statusModule.startDateStruct.date
Standard Age                  →  protocolSection.eligibilityModule.stdAges (list)
Conditions                    →  protocolSection.conditionsModule.conditions (list)
Primary Purpose               →  protocolSection.designModule.designInfo.primaryPurpose
Interventions                 →  protocolSection.armsInterventionsModule.interventions[].name
Intervention Description      →  protocolSection.armsInterventionsModule.interventions[].description
Study Type                    →  protocolSection.designModule.studyType
Phases                        →  protocolSection.designModule.phases (list)
Outcome Measure               →  protocolSection.outcomesModule.primaryOutcomes[].measure
Medical Subject Headings      →  protocolSection.conditionsModule.keywords (list)

BONUS fields from API (not in CSV!):
  NCT ID                      →  protocolSection.identificationModule.nctId
  Enrollment count            →  protocolSection.designModule.enrollmentInfo.count
  Locations (city, country)   →  protocolSection.contactsLocationsModule.locations[]
  Lead Sponsor                →  protocolSection.sponsorCollaboratorsModule.leadSponsor.name
""")


# =============================================================================
# SAVE ONE FULL STUDY AS JSON (for reference)
# =============================================================================
section("SAVING SAMPLE")

if data.get("studies"):
    with open("api_sample_study.json", "w", encoding="utf-8") as f:
        json.dump(data["studies"][0], f, indent=2, ensure_ascii=False)
    print("Saved first study to 'api_sample_study.json' for reference")
    
print("\nDone! Now you can see exactly what the API returns.")
print("Paste this output back to Claude so we can build the API ingestor.")
