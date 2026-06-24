import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
import csv
from datetime import datetime, date, timedelta

BASE_URL = "https://cad.kccda911.org/NewWorld.InmateInquiry/MI3913900"
DB_FILE = "kalamazoo_jail.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

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

def get_field(soup, css_class):
    tag = soup.find("li", class_=css_class)
    if tag:
        span = tag.find("span")
        if span:
            return span.get_text(strip=True)
    return ""

def get_roster_page(page_num):
    today = date.today()
    week_ago = (today - timedelta(days=7)).strftime("%m/%d/%Y")
    params = {
        "BookingFromDate": week_ago,
        "BookingToDate": today.strftime("%m/%d/%Y"),
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

def save_person(conn, roster_row, detail):
    c = conn.cursor()
    now = datetime.now().isoformat()
    subject_id = detail["subject_id"]
    address = detail.get("address", "")
    c.execute("""
        INSERT INTO people
        (subject_id, name, age, gender, race, address,
         first_seen, last_seen, total_bookings)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(subject_id) DO UPDATE SET
            last_seen = excluded.last_seen,
            age = excluded.age,
            address = excluded.address,
            total_bookings = (SELECT COUNT(*) FROM bookings WHERE subject_id = excluded.subject_id)
    """, (
        subject_id, roster_row["name"], detail.get("age", ""),
        roster_row["gender"], roster_row["race"], address,
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
            subject_id, booking_date, booking["housing_facility"],
            booking["bond_type"], booking["total_bond_amount"],
            booking["total_bail_amount"],
            1 if roster_row["in_custody"] else 0,
            booking_number, now
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
                subject_id, booking_date, charge["charge_number"],
                charge["description"], charge["charge_category"],
                charge["offense_date"], charge["disposition"],
                charge["disposition_date"], charge["sentence_length_raw"],
                charge["sentence_days"], charge["arresting_agency"],
                charge["booking_number"], now
            ))
    conn.commit()


def generate_spreadsheets(conn):
    print("\nGenerating master CSV...")
    c = conn.cursor()
    try:
        with open('jail_master.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'subject_id', 'name', 'age', 'gender', 'race', 'address',
                'first_seen', 'last_seen', 'total_bookings',
                'booking_date', 'housing_facility', 'bond_type',
                'total_bond_amount', 'total_bail_amount', 'in_custody',
                'booking_number',
                'charge_number', 'charge_description', 'charge_category',
                'offense_date', 'disposition', 'disposition_date',
                'sentence_length_raw', 'sentence_days', 'arresting_agency'
            ])
            c.execute("""
                SELECT
                    p.subject_id, p.name, p.age, p.gender, p.race, p.address,
                    p.first_seen, p.last_seen, p.total_bookings,
                    b.booking_date, b.housing_facility, b.bond_type,
                    b.total_bond_amount, b.total_bail_amount, b.in_custody,
                    b.booking_number,
                    c.charge_number, c.description, c.charge_category,
                    c.offense_date, c.disposition, c.disposition_date,
                    c.sentence_length_raw, c.sentence_days, c.arresting_agency
                FROM people p
                JOIN bookings b ON p.subject_id = b.subject_id
                LEFT JOIN charges c
                    ON c.subject_id = b.subject_id
                    AND c.booking_date = b.booking_date
                ORDER BY p.name, b.booking_date DESC, c.charge_number
            """)
            writer.writerows(c.fetchall())
            print(" -> Created jail_master.csv")
    except Exception as e:
        print(f"Error creating jail_master.csv: {e}")


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
            break

        rows = table.find_all("tr")[1:]
        if not rows:
            break

        valid_rows_this_page = 0

        for row in rows:
            roster_row = parse_roster_row(row)
            if not roster_row or not roster_row["href"]:
                continue

            valid_rows_this_page += 1
            subject_id = roster_row["href"].split("/")[-1]
            try:
                detail = parse_detail_page(roster_row["href"])
                c = conn.cursor()
                c.execute("SELECT subject_id FROM people WHERE subject_id = ?", (subject_id,))
                is_new = c.fetchone() is None
                if is_new:
                    new_people += 1
                save_person(conn, roster_row, detail)
                total_scraped += 1
            except Exception as e:
                errors += 1

        if valid_rows_this_page == 0:
            break

        next_link = soup.find("a", string=lambda s: s and "next" in s.lower())
        if not next_link or page >= 20:
            break

        page += 1
        time.sleep(1)

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

    generate_spreadsheets(conn)

    conn.close()
    print(f"\nDone in {duration:.1f}s. {total_scraped} scraped, {new_people} new, {errors} errors.")

if __name__ == "__main__":
    scrape()
