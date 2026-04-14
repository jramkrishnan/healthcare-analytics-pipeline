"""
Synthetic Healthcare Data Generator
Generates realistic hospital admissions, patient, diagnosis, 
and Medicare cost data for pipeline development and testing.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
import os

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data/seed_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Reference data ────────────────────────────────────────────────────────────

STATES = ["MA", "NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC"]

HOSPITAL_TYPES = ["Acute Care", "Critical Access", "Teaching", "Specialty"]

ICD10_CODES = [
    ("I21.0", "ST elevation myocardial infarction of anterior wall"),
    ("J18.9", "Pneumonia, unspecified organism"),
    ("N18.3", "Chronic kidney disease, stage 3"),
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("I50.9", "Heart failure, unspecified"),
    ("J44.1", "Chronic obstructive pulmonary disease with acute exacerbation"),
    ("A41.9", "Sepsis, unspecified organism"),
    ("K92.1", "Melena"),
    ("I63.9", "Cerebral infarction, unspecified"),
    ("M54.5", "Low back pain"),
    ("I10",   "Essential (primary) hypertension"),
    ("Z79.4", "Long-term (current) use of insulin"),
    ("F32.9", "Major depressive disorder, single episode, unspecified"),
    ("G43.909","Migraine, unspecified, not intractable, without status migrainosus"),
    ("K29.70", "Gastritis, unspecified, without bleeding"),
]

DRG_CODES = [
    ("470",  "MAJOR JOINT REPLACEMENT OR REATTACHMENT OF LOWER EXTREMITY W/O MCC"),
    ("871",  "SEPTICEMIA OR SEVERE SEPSIS W/O MV >96 HOURS W MCC"),
    ("291",  "HEART FAILURE & SHOCK W MCC"),
    ("292",  "HEART FAILURE & SHOCK W CC"),
    ("193",  "SIMPLE PNEUMONIA & PLEURISY W MCC"),
    ("194",  "SIMPLE PNEUMONIA & PLEURISY W CC"),
    ("682",  "RENAL FAILURE W MCC"),
    ("683",  "RENAL FAILURE W CC"),
    ("392",  "ESOPHAGITIS, GASTROENT & MISC DIGEST DISORDERS W/O MCC"),
    ("641",  "MISC DISORDERS OF NUTRITION, METABOLISM, FLUIDS/ELECTROLYTES W MCC"),
]

INSURANCE = ["Medicare", "Medicaid", "Private", "Self-Pay", "VA"]
GENDERS    = ["M", "F"]
RACES      = ["White", "Black", "Hispanic", "Asian", "Other"]
ADM_TYPES  = ["Emergency", "Elective", "Urgent", "Newborn"]
DISPOSITIONS = ["Home", "SNF", "Rehab", "AMA", "Expired", "Home Health"]


# ── Generators ────────────────────────────────────────────────────────────────

def gen_hospitals(n=50):
    rows = []
    for i in range(1, n + 1):
        state = random.choice(STATES)
        rows.append({
            "hospital_id":   f"H{i:04d}",
            "hospital_name": f"{state} General Hospital {i}",
            "state":         state,
            "city":          f"City_{i}",
            "bed_count":     random.randint(50, 800),
            "hospital_type": random.choice(HOSPITAL_TYPES),
            "teaching_flag": random.choice([True, False]),
        })
    return rows


def gen_patients(n=5000):
    rows = []
    for _ in range(n):
        rows.append({
            "patient_id":      str(uuid.uuid4())[:12],
            "age":             random.randint(18, 90),
            "gender":          random.choice(GENDERS),
            "race":            random.choice(RACES),
            "insurance_type":  random.choice(INSURANCE),
            "zip_code":        f"{random.randint(10000, 99999)}",
            "state":           random.choice(STATES),
        })
    return rows


def gen_admissions(patients, hospitals, n=15000):
    rows = []
    patient_ids  = [p["patient_id"] for p in patients]
    hospital_ids = [h["hospital_id"] for h in hospitals]

    for _ in range(n):
        admit_date    = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 1095))
        los           = random.randint(1, 30)
        discharge_date = admit_date + timedelta(days=los)

        # 30-day readmission probability influenced by LOS and age
        pat           = random.choice(patients)
        readmit_prob  = 0.12 + (los > 7) * 0.08 + (pat["age"] > 65) * 0.06
        readmitted    = random.random() < readmit_prob

        icd           = random.choice(ICD10_CODES)
        rows.append({
            "admission_id":        str(uuid.uuid4())[:12],
            "patient_id":          pat["patient_id"],
            "hospital_id":         random.choice(hospital_ids),
            "admit_date":          admit_date.date(),
            "discharge_date":      discharge_date.date(),
            "admission_type":      random.choice(ADM_TYPES),
            "primary_diagnosis_code": icd[0],
            "primary_diagnosis_desc": icd[1],
            "los_days":            los,
            "readmitted_30_days":  readmitted,
            "discharge_disposition": random.choice(DISPOSITIONS),
            "total_charges":       round(random.uniform(2000, 120000), 2),
            "icu_hours":           random.randint(0, 72) if random.random() < 0.3 else 0,
        })
    return rows


def gen_diagnoses(admissions):
    """Generate secondary diagnoses for each admission (comorbidities)."""
    rows = []
    for adm in admissions:
        n_dx = random.randint(1, 5)
        used = {adm["primary_diagnosis_code"]}
        for rank in range(2, n_dx + 2):
            icd = random.choice(ICD10_CODES)
            while icd[0] in used:
                icd = random.choice(ICD10_CODES)
            used.add(icd[0])
            rows.append({
                "diagnosis_id":    str(uuid.uuid4())[:12],
                "admission_id":    adm["admission_id"],
                "icd_code":        icd[0],
                "icd_description": icd[1],
                "diagnosis_rank":  rank,
                "diagnosis_type":  "Secondary",
            })
    return rows


def gen_medicare_costs(hospitals):
    rows = []
    for hosp in hospitals:
        for drg_code, drg_desc in DRG_CODES:
            if random.random() < 0.7:          # not every hospital has every DRG
                discharges = random.randint(10, 500)
                avg_covered = round(random.uniform(8000, 80000), 2)
                avg_total   = round(avg_covered * random.uniform(0.3, 0.6), 2)
                avg_medicare = round(avg_total * random.uniform(0.8, 0.95), 2)
                rows.append({
                    "cost_id":                  str(uuid.uuid4())[:12],
                    "hospital_id":              hosp["hospital_id"],
                    "drg_code":                 drg_code,
                    "drg_description":          drg_desc,
                    "total_discharges":         discharges,
                    "avg_covered_charges":      avg_covered,
                    "avg_total_payments":       avg_total,
                    "avg_medicare_payments":    avg_medicare,
                    "year":                     random.choice([2021, 2022, 2023]),
                })
    return rows


# ── Writer ────────────────────────────────────────────────────────────────────

def write_csv(filename, rows):
    if not rows:
        return
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓  {filename:35s}  {len(rows):>6} rows  →  {path}")


if __name__ == "__main__":
    print("\n🏥  Generating synthetic healthcare data …\n")
    hospitals  = gen_hospitals(50)
    patients   = gen_patients(5000)
    admissions = gen_admissions(patients, hospitals, 15000)
    diagnoses  = gen_diagnoses(admissions)
    costs      = gen_medicare_costs(hospitals)

    write_csv("hospitals.csv",      hospitals)
    write_csv("patients.csv",       patients)
    write_csv("admissions.csv",     admissions)
    write_csv("diagnoses.csv",      diagnoses)
    write_csv("medicare_costs.csv", costs)

    print(f"\n✅  Done — {len(hospitals)+len(patients)+len(admissions)+len(diagnoses)+len(costs):,} total rows written to {OUTPUT_DIR}\n")
