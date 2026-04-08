import requests
from bs4 import BeautifulSoup
import sqlite3
import time
from datetime import datetime, date

BASE_URL = "https://cad.kccda911.org/NewWorld.InmateInquiry/MI3913900"
DB_FILE = "kalamazoo_jail.db"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS people (
            subject_id TEXT PRIMARY KEY,
            name TEXT,
            age TEXT,
            gender TEXT,
            race TEXT,
            address TEXT,
            first_seen TEXT,
            last_seen TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            subject_id TEXT,
            booking_date TEXT,
            housing_facility TEXT,
            total_bond_amount TEXT,
            total_bail_amount TEXT,
            charges TEXT,
            scraped_at TEXT,
            PRIMARY KEY (subject_id, booking_date)
        )
    """)
    conn.commit()
    return conn

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

def get_detail_links(soup):
    links = []
    for a in soup.find_all("a", href=True):
        if "/Inmate/Detail/" in a["href"]:
            links.append((a.text.strip(), a["href"]))
    return links

def parse_detail(name, href):
    url = "https://cad.kccda911.org" + href
    time.sleep(0.5)
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    subject_id = href.split("/")[-1]

    def get_field(css_class):
        tag = soup.find("li", class_=css_class)
        if tag:
            span = tag.find("span")
            if span:
                return span.get_text(strip=True)
        return ""

    data = {
        "subject_id": subject_id,
        "name": name,
        "age": get_field("Age"),
        "gender": get_field("Gender"),
        "race": get_field("Race"),
        "address": get_field("Address"),
        "bookings": []
    }

    for booking_div in soup.find_all("div", class_="Booking"):
        booking = {
            "booking_date": "",
            "housing_facility": "",
            "total_bond_amount": "",
            "total_bail_amount": "",
            "charges": []
        }

        def get_booking_field(css_class):
            tag = booking_div.find("li", class_=css_class)
            if tag:
                span = tag.find("span")
                if span:
                    return span.get_text(strip=True)
            return ""

        booking["booking_date"] = get_booking_field("BookingDate")
        booking["housing_facility"] = get_booking_field("HousingFacility")
        booking["total_bond_amount"] = get_booking_field("TotalBondAmount")
        booking["total_bail_amount"] = get_booking_field("TotalBailAmount")

        charges_table = booking_div.find("div", class_="BookingCharges")
        if charges_table:
            for row in charges_table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if cells:
                    charge = {
                        "number": cells[0].get_text(strip=True) if len(cells) > 0 else "",
                        "description": cells[1].get_text(strip=True) if len(cells) > 1 else "",
                        "offense_date": cells[2].get_text(strip=True) if len(cells) > 2 else "",
                        "disposition": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                        "disposition_date": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                        "sentence_length": cells[5].get_text(strip=True) if len(cells) > 5 else "",
                        "arresting_agency": cells[6].get_text(strip=True) if len(cells) > 6 else "",
                    }
                    booking["charges"].append(str(charge))

        data["bookings"].append(booking)

    return data

def save(conn, data):
    c = conn.cursor()
    now = datetime.now().isoformat()

    c.execute("""
        INSERT OR IGNORE INTO people
        (subject_id, name, age, gender, race, address, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (data["subject_id"], data["name"], data["age"],
          data["gender"], data["race"], data["address"], now, now))

    c.execute("UPDATE people SET last_seen=? WHERE subject_id=?",
              (now, data["subject_id"]))

    for b in data["bookings"]:
        charges_text = "; ".join(b["charges"])
        c.execute("""
            INSERT OR IGNORE INTO bookings
            (subject_id, booking_date, housing_facility,
             total_bond_amount, total_bail_amount, charges, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data["subject_id"], b["booking_date"], b["housing_facility"],
              b["total_bond_amount"], b["total_bail_amount"],
              charges_text, now))

    conn.commit()

def scrape():
    print(f"Starting scrape at {datetime.now()}")
    conn = init_db()
    total = 0
    page = 1

    while True:
        print(f"Fetching page {page}...")
        soup = get_roster_page(page)
        links = get_detail_links(soup)

        if not links:
            print("No more results.")
            break

        for name, href in links:
            try:
                data = parse_detail(name, href)
                save(conn, data)
                total += 1
                print(f"  Saved: {name} | bookings: {len(data['bookings'])}")
            except Exception as e:
                print(f"  Error on {name}: {e}")

        next_link = soup.find("a", string="Next")
        if not next_link:
            break

        page += 1
        time.sleep(1)

    print(f"\nDone. {total} records saved.")
    conn.close()

if __name__ == "__main__":
    scrape()
