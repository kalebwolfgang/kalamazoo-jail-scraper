import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
from datetime import datetime, date

BASE_URL = "https://cad.kccda911.org/NewWorld.InmateInquiry/MI3913900"
DB_FILE = "kalamazoo_jail.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ─── DATABASE SETUP ───────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS people (
            subject_id      TEXT PRIMARY KEY,
            name            TEXT,
            age             TEXT,
            gender          TEXT,
            race            TEXT,
            address         TEXT,
            is_homeless     INTEGER DEFAULT 0,
            is_out_of_county INTEGER DEFAULT 0,
            first_seen      TEXT,
            last_seen       TEXT,
            total_bookings  INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            booking_pk          INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id          TEXT,
            booking_date        TEXT,
            housing_facility    TEXT,
            bond_type           TEXT,
            total_bond_amount   TEXT,
            total_bail_amount   TEXT,
            in_custody          INTEGER DEFAULT 1,
            booking_number      TEXT,
            last_updated        TEXT,
            UNIQUE(subject_id, booking_date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS charges (
            charge_pk           INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id          TEXT,
            booking_date        TEXT,
            charge_number       TEXT,
            description         TEXT,
            charge_category     TEXT,
            offense_date        TEXT,
            disposition         TEXT,
            disposition_date    TEXT,
            sentence_length_raw TEXT,
            sentence_days       INTEGER,
            arresting_agency    TEXT,
            booking_number      TEXT,
            last_updated        TEXT,
            UNIQUE(subject_id, booking_date, charge_number)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_pk         INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date       TEXT,
            snapshot_time       TEXT,
            total_in_custody    INTEGER,
            total_black         INTEGER,
            total_white         INTEGER,
            total_hispanic      INTEGER,
            total_unknown_race  INTEGER,
            total_other_race    INTEGER,
            total_male          INTEGER,
            total_female        INTEGER,
            total_pretrial      INTEGER,
            total_sentenced     INTEGER,
            total_homeless      INTEGER,
            total_out_of_county INTEGER,
            total_multiple_bookings INTEGER,
            avg_days_in_custody REAL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS daily_presence (
            presence_pk     INTEGER PRIMARY KEY AUTOINCREMENT,
            subject_id      TEXT,
            snapshot_date   TEXT,
            in_custody      INTEGER,
            days_in_custody INTEGER,
            UNIQUE(subject_id, snapshot_date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            log_pk          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time        TEXT,
            status          TEXT,
            records_scraped INTEGER,
            new_people      INTEGER,
            new_bookings    INTEGER,
            updated_charges INTEGER,
            errors          INTEGER,
            duration_seconds REAL,
            notes           TEXT
        )
    """)

    conn.commit()
    return conn

# ─── HELPER FUNCTIONS ─────────────────────────────────────────────

def is_homeless(address):
    if not address:
        return True
    address = address.strip()
    if address == "":
        return True
    if address == "MICHIGAN":
        return True
    if "49048" in address:
        return True
    return False

def is_out_of_county(address):
    if not address or is_homeless(address):
        return False
    kalamazoo_cities = [
        "KALAMAZOO", "PORTAGE", "GALESBURG", "RICHLAND",
        "VICKSBURG", "SCHOOLCRAFT", "PARCHMENT", "COMSTOCK",
        "CLIMAX", "PAVILION", "TEXAS", "OSHTEMO", "COOPER"
    ]
    address_upper = address.upper()
    return not any(city in address_upper for city in kalamazoo_cities)

def categorize_charge(description):
    desc = description.lower()
    if any(x in desc for x in ['murder', 'homicide', 'manslaughter', 'csc', 'sexual', 'rape', 'kidnap']):
        return 'Violent-Serious'
    elif any(x in desc for x in ['assault', 'battery', 'robbery', 'carjack', 'domestic violence', 'aggravated']):
        return 'Violent'
    elif any(x in desc for x in ['weapon', 'firearm', 'concealed', 'gun', 'discharge']):
        return 'Weapons'
    elif any(x in desc for x in ['deliver', 'manufactur', 'sell', 'distribut', 'trafficking']):
        return 'Drug-Delivery'
    elif any(x in desc for x in ['possess', 'meth', 'cocaine', 'heroin', 'narc', 'vcsa', 'controlled substance']):
        return 'Drug-Possession'
    elif any(x in desc for x in ['burglary', 'breaking', 'entering']):
        return 'Burglary'
    elif any(x in desc for x in ['trespass', 'parks', 'airport rules', 'county park']):
        return 'Trespass'
    elif any(x in desc for x in ['retail fraud', 'larceny', 'theft', 'fraud', 'forgery', 'uttering']):
        return 'Theft-Fraud'
    elif any(x in desc for x in ['probation', 'conditional release', 'warrant', 'hold -']):
        return 'Probation-Warrant'
    elif any(x in desc for x in ['license', 'suspended', 'driving', 'intoxicated', 'owi', 'operating']):
        return 'Traffic-DUI'
    elif any(x in desc for x in ['trespass', 'disorderly', 'disturbing', 'open intox', 'open container']):
        return 'Public-Order'
    elif any(x in desc for x in ['non-support', 'neglect child', 'child']):
        return 'Family'
    elif any(x in desc for x in ['resist', 'obstruct', 'fleeing', 'escape']):
        return 'Resist-Obstruct'
    else:
        return 'Other'

def parse_sentence_days(sentence_str):
    if not sentence_str:
        return None
    s = sentence_str.lower().strip()
    try:
        if 'year' in s:
            num = float(re.search(r'[\d.]+', s).group())
            return int(num * 365)
        elif 'month' in s:
            num = float(re.search(r'[\d.]+', s).group())
            return int(num * 30)
        elif 'day' in s:
            num = float(re.search(r'[\d.]+', s).group())
            return int(num)
    except:
        return None
    return None

def days_since_booking(booking_date_str):
    if not booking_date_str:
        return None
    try:
        parts = booking_date_str.split(' ')[0]
        booked = datetime.strptime(parts, '%m/%d/%Y')
        return (datetime.now() - booked).days
    except:
        return None

def get_field(soup, css_class):
    tag = soup.find("li", class_=css_class)
    if tag:
        span = tag.find("span")
        if span:
            return span.get_text(strip=True)
    return ""

# ─── ROSTER SCRAPING ──────────────────────────────────────────────

def get_roster_page(page_num):
    today = date.today().strftime("%m/%d/%Y")
    week_ago = (date.today() - __import__('datetime').timedelta(days=7)).strftime("%m/%d/%Y")
    params = {
        "BookingFromDate": week_ago,
        "BookingToDate": today,
        "Page": page_num
    }
    r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def parse_roster_row(row):
    cells = row.find_all("td")
    if len(cells) < 5:
        return None
    link = row.find("a")
    if not link:
        return None
    return {
        "name": link.text.strip(),
        "href": link.get("href", ""),
        "in_custody": cells[1].text.strip() == "Yes",
        "race": cells[2].text.strip(),
        "gender": cells[3].text.strip(),
        "multiple_bookings": cells[4].text.strip() == "Yes",
        "housing_facility": cells[5].text.strip() if len(cells) > 5 else ""
    }

# ─── DETAIL PAGE PARSING ──────────────────────────────────────────

def parse_detail_page(href):
    url = "https://cad.kccda911.org" + href
    time.sleep(0.5)
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    subject_id = href.split("/")[-1]

    data = {
        "subject_id": subject_id,
        "age": get_field(soup, "Age"),
        "address": get_field(soup, "Address"),
        "bookings": []
    }

    for booking_div in soup.find_all("div", class_="Booking"):

        def get_bf(css_class):
            tag = booking_div.find("li", class_=css_class)
            if tag:
                span = tag.find("span")
                if span:
                    return span.get_text(strip=True)
            return ""

        # Bond types from the bond table
        bond_types = []
        bond_table = booking_div.find("div", class_="BookingBonds")
        if bond_table:
            for row in bond_table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    bt = cells[0].get_text(strip=True)
                    if bt and bt != "No data":
                        bond_types.append(bt)
        bond_type_str = ", ".join(bond_types) if bond_types else "None"

        booking = {
            "booking_date": get_bf("BookingDate"),
            "housing_facility": get_bf("HousingFacility"),
            "bond_type": bond_type_str,
            "total_bond_amount": get_bf("TotalBondAmount"),
            "total_bail_amount": get_bf("TotalBailAmount"),
            "charges": []
        }

        charges_div = booking_div.find("div", class_="BookingCharges")
        if charges_div:
            for row in charges_div.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) >= 7:
                    desc = cells[1].get_text(strip=True)
                    sentence_raw = cells[5].get_text(strip=True)
                    booking_num = cells[7].get_text(strip=True) if len(cells) > 7 else ""
                    charge = {
                        "charge_number": cells[0].get_text(strip=True),
                        "description": desc,
                        "charge_category": categorize_charge(desc),
                        "offense_date": cells[2].get_text(strip=True),
                        "disposition": cells[3].get_text(strip=True),
                        "disposition_date": cells[4].get_text(strip=True),
                        "sentence_length_raw": sentence_raw,
                        "sentence_days": parse_sentence_days(sentence_raw),
                        "arresting_agency": cells[6].get_text(strip=True),
                        "booking_number": booking_num
                    }
                    booking["charges"].append(charge)

        data["bookings"].append(booking)

    return data

# ─── DATABASE WRITES ──────────────────────────────────────────────

def save_person(conn, roster_row, detail):
    c = conn.cursor()
    now = datetime.now().isoformat()
    subject_id = detail["subject_id"]
    address = detail.get("address", "")
    homeless = 1 if is_homeless(address) else 0
    out_of_county = 1 if is_out_of_county(address) else 0

    c.execute("""
        INSERT INTO people
        (subject_id, name, age, gender, race, address,
         is_homeless, is_out_of_county, first_seen, last_seen, total_bookings)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(subject_id) DO UPDATE SET
            last_seen = excluded.last_seen,
            age = excluded.age,
            address = excluded.address,
            is_homeless = excluded.is_homeless,
            is_out_of_county = excluded.is_out_of_county,
            total_bookings = (SELECT COUNT(*) FROM bookings WHERE subject_id = excluded.subject_id)
    """, (
        subject_id,
        roster_row["name"],
        detail.get("age", ""),
        roster_row["gender"],
        roster_row["race"],
        address,
        homeless,
        out_of_county,
        now, now,
        len(detail.get("bookings", []))
    ))

    for booking in detail.get("bookings", []):
        booking_date = booking["booking_date"]
        booking_number = ""
        if booking["charges"]:
            booking_number = booking["charges"][0].get("booking_number", "")

        c.execute("""
            INSERT INTO bookings
            (subject_id, booking_date, housing_facility, bond_type,
             total_bond_amount, total_bail_amount, in_custody,
             booking_number, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(subject_id, booking_date) DO UPDATE SET
                in_custody = excluded.in_custody,
                bond_type = excluded.bond_type,
                total_bond_amount = excluded.total_bond_amount,
                last_updated = excluded.last_updated
        """, (
            subject_id,
            booking_date,
            booking["housing_facility"],
            booking["bond_type"],
            booking["total_bond_amount"],
            booking["total_bail_amount"],
            1 if roster_row["in_custody"] else 0,
            booking_number,
            now
        ))

        for charge in booking.get("charges", []):
            c.execute("""
                INSERT INTO charges
                (subject_id, booking_date, charge_number, description,
                 charge_category, offense_date, disposition, disposition_date,
                 sentence_length_raw, sentence_days, arresting_agency,
                 booking_number, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subject_id, booking_date, charge_number) DO UPDATE SET
                    disposition = excluded.disposition,
                    disposition_date = excluded.disposition_date,
                    sentence_length_raw = excluded.sentence_length_raw,
                    sentence_days = excluded.sentence_days,
                    last_updated = excluded.last_updated
            """, (
                subject_id,
                booking_date,
                charge["charge_number"],
                charge["description"],
                charge["charge_category"],
                charge["offense_date"],
                charge["disposition"],
                charge["disposition_date"],
                charge["sentence_length_raw"],
                charge["sentence_days"],
                charge["arresting_agency"],
                charge["booking_number"],
                now
            ))

    conn.commit()

# ─── SNAPSHOT RECORDING ───────────────────────────────────────────

def record_snapshot(conn, all_people_today):
    c = conn.cursor()
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    snap_time = now.strftime("%H:%M:%S")

    total = len(all_people_today)
    black = sum(1 for p in all_people_today if p["race"] == "Black")
    white = sum(1 for p in all_people_today if p["race"] == "White")
    hispanic = sum(1 for p in all_people_today if p["race"] == "Hispanic")
    unknown = sum(1 for p in all_people_today if p["race"] in ("Unknown", ""))
    other = total - black - white - hispanic - unknown
    male = sum(1 for p in all_people_today if p["gender"] == "Male")
    female = sum(1 for p in all_people_today if p["gender"] == "Female")
    homeless = sum(1 for p in all_people_today if p.get("is_homeless", False))
    out_of_county = sum(1 for p in all_people_today if p.get("is_out_of_county", False))
    multiple = sum(1 for p in all_people_today if p.get("multiple_bookings", False))

    # Pretrial = in custody with at least one open charge
    pretrial = 0
    sentenced = 0
    days_list = []

    for p in all_people_today:
        subject_id = p["subject_id"]
        c.execute("""
            SELECT disposition FROM charges
            WHERE subject_id = ?
            ORDER BY booking_date DESC
        """, (subject_id,))
        charge_disps = [r[0] for r in c.fetchall()]
        has_open = any(d == "" or d is None for d in charge_disps)
        if has_open:
            pretrial += 1
        else:
            sentenced += 1

        d = days_since_booking(p.get("latest_booking_date", ""))
        if d is not None:
            days_list.append(d)

    avg_days = round(sum(days_list) / len(days_list), 1) if days_list else 0

    c.execute("""
        INSERT INTO snapshots
        (snapshot_date, snapshot_time, total_in_custody,
         total_black, total_white, total_hispanic,
         total_unknown_race, total_other_race,
         total_male, total_female,
         total_pretrial, total_sentenced,
         total_homeless, total_out_of_county,
         total_multiple_bookings, avg_days_in_custody)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today, snap_time, total,
        black, white, hispanic,
        unknown, other,
        male, female,
        pretrial, sentenced,
        homeless, out_of_county,
        multiple, avg_days
    ))

    # Daily presence
    for p in all_people_today:
        d = days_since_booking(p.get("latest_booking_date", ""))
        c.execute("""
            INSERT OR IGNORE INTO daily_presence
            (subject_id, snapshot_date, in_custody, days_in_custody)
            VALUES (?, ?, 1, ?)
        """, (p["subject_id"], today, d))

    conn.commit()
    print(f"Snapshot recorded: {total} in custody | {pretrial} pretrial | {sentenced} sentenced | avg {avg_days} days")

# ─── MAIN SCRAPE FUNCTION ─────────────────────────────────────────

def scrape():
    start_time = datetime.now()
    print(f"Starting scrape at {start_time}")
    conn = init_db()

    total_scraped = 0
    new_people = 0
    new_bookings = 0
    updated_charges = 0
    errors = 0
    page = 1
    all_people_today = []

    while True:
        print(f"Fetching roster page {page}...")
        try:
            soup = get_roster_page(page)
        except Exception as e:
            print(f"Failed to fetch page {page}: {e}")
            errors += 1
            break

        table = soup.find("table")
        if not table:
            print("No table found, stopping.")
            break

        rows = table.find_all("tr")[1:]
        if not rows:
            print("No rows found, stopping.")
            break

        for row in rows:
            roster_row = parse_roster_row(row)
            if not roster_row or not roster_row["href"]:
                continue

            subject_id = roster_row["href"].split("/")[-1]

            try:
                detail = parse_detail_page(roster_row["href"])

                # Check if person is new
                c = conn.cursor()
                c.execute("SELECT subject_id FROM people WHERE subject_id = ?", (subject_id,))
                is_new = c.fetchone() is None
                if is_new:
                    new_people += 1

                save_person(conn, roster_row, detail)
                total_scraped += 1

                # Track for snapshot
                latest_booking_date = ""
                if detail.get("bookings"):
                    latest_booking_date = detail["bookings"][0].get("booking_date", "")

                all_people_today.append({
                    "subject_id": subject_id,
                    "race": roster_row["race"],
                    "gender": roster_row["gender"],
                    "multiple_bookings": roster_row["multiple_bookings"],
                    "is_homeless": is_homeless(detail.get("address", "")),
                    "is_out_of_county": is_out_of_county(detail.get("address", "")),
                    "latest_booking_date": latest_booking_date
                })

                print(f"  {'NEW' if is_new else 'UPD'}: {roster_row['name']} | {roster_row['race']} | bookings: {len(detail['bookings'])}")

            except Exception as e:
                print(f"  Error on {roster_row['name']}: {e}")
                errors += 1

        next_link = soup.find("a", string="Next")
        if not next_link:
            break
        page += 1
        time.sleep(1)

    # Record snapshot for this run
    if all_people_today:
        record_snapshot(conn, all_people_today)

    # Log the run
    duration = (datetime.now() - start_time).total_seconds()
    c = conn.cursor()
    c.execute("""
        INSERT INTO scrape_log
        (run_time, status, records_scraped, new_people,
         new_bookings, updated_charges, errors, duration_seconds, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        start_time.isoformat(),
        "success" if errors == 0 else "partial",
        total_scraped, new_people,
        new_bookings, updated_charges,
        errors, duration, ""
    ))
    conn.commit()
    conn.close()

    print(f"\nDone in {duration:.1f}s. {total_scraped} scraped, {new_people} new, {errors} errors.")

if __name__ == "__main__":
    scrape()
