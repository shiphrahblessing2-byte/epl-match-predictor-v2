import os, sys, requests
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parent))
client = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

TEAM_MAP = {
    # EPL — API-Football IDs
    33:"Manchester United", 34:"Newcastle United", 35:"Bournemouth",
    36:"Fulham", 39:"Wolverhampton Wanderers", 40:"Liverpool",
    41:"Southampton", 42:"Arsenal", 44:"Burnley", 45:"Everton",
    46:"Leicester City", 47:"Tottenham Hotspur", 48:"West Ham United",
    49:"Chelsea", 50:"Manchester City", 51:"Brighton & Hove Albion",
    52:"Crystal Palace", 55:"Brentford", 57:"Ipswich Town",
    62:"Sheffield United", 63:"Leeds United", 65:"Nottingham Forest",
    66:"Aston Villa", 301:"Sunderland",
    # EPL — ESPN IDs
    331:"Brighton & Hove Albion", 337:"Brentford", 349:"AFC Bournemouth",
    357:"Leeds United", 359:"Arsenal", 360:"Manchester United",
    361:"Newcastle United", 362:"Aston Villa", 363:"Chelsea",
    364:"Liverpool", 366:"Sheffield United", 367:"Tottenham Hotspur",
    368:"Everton", 370:"Fulham", 371:"West Ham United", 373:"Burnley",
    375:"Leicester City", 376:"Southampton", 379:"Ipswich Town",
    380:"Wolverhampton Wanderers", 381:"Crystal Palace",
    382:"Manchester City", 383:"Burnley", 384:"Crystal Palace",
    393:"Nottingham Forest", 395:"Sunderland", 398:"Luton Town",
    1359:"Luton Town",
    # La Liga
    83:"Valencia", 84:"Atlético Madrid", 85:"Barcelona",
    86:"Real Madrid", 88:"Deportivo Alavés", 89:"Sevilla",
    92:"Villarreal", 93:"Real Betis", 94:"Real Sociedad",
    95:"Osasuna", 96:"Celta Vigo", 97:"Espanyol", 98:"Getafe",
    101:"Athletic Club", 102:"Rayo Vallecano", 243:"Girona",
    244:"Mallorca", 1068:"Las Palmas", 1538:"Leganés",
    2922:"Real Valladolid", 3747:"Levante", 3751:"Elche",
    3752:"Real Oviedo", 3842:"Almería", 5413:"Eldense",
    6832:"Racing de Santander", 9812:"Deportivo La Coruña",
    17534:"Real Murcia",
    # Germany
    134:"Bayern Munich", 132:"Bayer Leverkusen",
    148:"Borussia Dortmund", 142:"RB Leipzig",
    2528:"SC Freiburg", 2533:"VfB Stuttgart",
    2715:"Eintracht Frankfurt", 2720:"RB Leipzig", 2722:"Bayern Munich",
    2290:"Borussia Dortmund", 2250:"Bayer Leverkusen",
    # Italy
    110:"Inter Milan", 111:"AC Milan", 112:"Lazio",
    114:"AS Roma", 105:"Juventus", 166:"Atalanta",
    2790:"Inter Milan", 2994:"AS Roma", 3019:"Bologna", 2502:"Lazio",
    # France
    103:"Paris Saint-Germain", 169:"Monaco", 104:"Olympique de Marseille",
    165:"Lyon", 160:"Lens", 2980:"Lille", 2988:"Nice", 2990:"Lyon",
    # Portugal
    174:"Sporting CP", 175:"Benfica", 176:"FC Porto",
    597:"Braga", 598:"FC Porto", 605:"Sporting CP",
    # Netherlands
    138:"PSV Eindhoven", 139:"Feyenoord", 124:"Ajax",
    3746:"AZ Alkmaar", 3611:"Go Ahead Eagles", 3691:"FC Utrecht",
    # Scotland
    256:"Celtic", 257:"Rangers", 611:"Celtic", 614:"Rangers",
    # Turkey
    432:"Galatasaray", 433:"Fenerbahçe", 435:"Beşiktaş",
    436:"Trabzonspor", 437:"Başakşehir",
    # Belgium
    131:"Club Brugge", 594:"Genk", 3706:"Genk", 17544:"KAA Gent",
    # Others
    107:"Olympiacos", 493:"Basel", 494:"Young Boys",
    521:"Sturm Graz", 570:"Bodø/Glimt", 10414:"Bodø/Glimt",
    440:"Maccabi Tel Aviv", 5260:"Maccabi Tel Aviv",
    442:"PAOK", 4411:"PAOK", 443:"Panathinaikos",
    519:"Malmö FF", 524:"Red Bull Salzburg",
    529:"Qarabağ FK", 6997:"Qarabağ FK",
    555:"Ferencváros", 7914:"Ferencváros", 17632:"Ferencváros",
    559:"Crvena zvezda", 909:"Crvena zvezda",
    572:"Midtjylland", 575:"FC Copenhagen",
    887:"Viktoria Plzeň", 5807:"Sparta Prague",
    938:"Ludogorets", 17629:"Ludogorets",
    989:"Brann", 997:"Molde",
    1895:"FCSB", 1929:"Dnipro-1",
    1941:"Shakhtar Donetsk", 1963:"Dynamo Kyiv",
    2559:"Hoffenheim", 484:"Anderlecht",
    622:"Hibernian", 617:"Aberdeen",
    7911:"Legia Warsaw", 4039:"Slavia Prague",
    11706:"Dinamo Zagreb", 12030:"Slovan Bratislava",
    11336:"FC Nordsjælland", 11420:"HJK Helsinki",
    17630:"Sheriff Tiraspol", 17631:"Olimpija Ljubljana",
    17520:"Zrinjski", 7834:"Shamrock Rovers",
    20032:"Pafos FC", 20713:"APOEL", 21005:"Omonia Nicosia",
    21530:"Tobol Kostanay", 22281:"NK Celje",
    126:"Real Betis", 153:"Villarreal", 179:"Real Sociedad",
    # Add these to TEAM_MAP — the 12 remaining unknowns
    125: "PSV Eindhoven",          # duplicate ESPN ID
    140: "Espanyol",               # duplicate ESPN ID  
    152: "Olympique de Marseille", # duplicate ESPN ID
    167: "Udinese",                # Serie A
    268: "Viktoria Plzeň",         # Czech Republic (duplicate)
    441: "Maccabi Haifa",          # Israel
    502: "FK Austria Wien",        # Austria
    620: "Heart of Midlothian",    # Scotland
    10834: "Hapoel Beer Sheva",    # Israel
    13018: "Hammarby",             # Sweden
    13083: "Djurgårdens IF",       # Sweden
    13294: "IFK Göteborg",         # Sweden
}

# Paginate all matches to get all team IDs
all_rows, page = [], 0
while True:
    resp = client.table("matches").select("home_team_id,away_team_id") \
        .range(page * 1000, (page + 1) * 1000 - 1).execute()
    all_rows.extend(resp.data)
    if len(resp.data) < 1000:
        break
    page += 1

team_ids = set()
for row in all_rows:
    team_ids.add(row["home_team_id"])
    team_ids.add(row["away_team_id"])

print(f"Total unique teams: {len(team_ids)}")

# Update matches table
mapped, skipped = 0, []
for tid in sorted(team_ids):
    name = TEAM_MAP.get(tid)
    if name:
        client.table("matches").update({"home_team": name}).eq("home_team_id", tid).execute()
        client.table("matches").update({"away_team": name}).eq("away_team_id", tid).execute()
        print(f"  ✅ {tid} → {name}")
        mapped += 1
    else:
        skipped.append(tid)
        print(f"  ❌ {tid} → unknown")

print(f"\n✅ Mapped: {mapped}/{len(team_ids)}")
if skipped:
    print(f"❌ Unknown IDs: {skipped}")

# ── Write to teams table ────────────────────────────────
print("\n📝 Populating teams table...")
team_rows = [
    {"team_id": tid, "team_name": name}   # ← was "name", now "team_name"
    for tid, name in TEAM_MAP.items()
]

# Upsert in batches of 100
for i in range(0, len(team_rows), 100):
    batch = team_rows[i:i+100]
    client.table("teams").upsert(batch, on_conflict="team_id").execute()
    print(f"  ✅ Batch {i//100 + 1}: {len(batch)} teams upserted")

print(f"✅ teams table populated with {len(team_rows)} entries")