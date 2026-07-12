#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
===============================================================================
   MASTER TRADING BOT  --  4 bots in 1
===============================================================================
   TAB 1  INTRADAY    : NSE 1-min Fibonacci breakout scanner   (yfinance)
   TAB 2  SWING       : NSE 1M-levels / 1D-candles Fib bot      (yfinance)
   TAB 3  INVESTMENT  : NSE weekly investment-level scanner     (yfinance)
   TAB 4  OPTIONS     : Dhan multi-index options bot + backtest (Dhan API)

   Login (Dhan Client ID + Access Token) is MANDATORY. After login the
   dashboard opens with 4 clean tabs. Intraday/Swing/Investment run on
   yfinance; Options runs on the Dhan API using your credentials.

   Run:   python master_bot.py
   Open:  http://localhost:5000   (auto-opens)
===============================================================================
"""

import os
import sys
import json
import time
import queue
import threading
import webbrowser
import subprocess
import datetime as dt
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed


# =============================================================================
# AUTO-INSTALL DEPENDENCIES
# =============================================================================

def _install(pkg):
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg,
                               "-q", "--no-warn-script-location"])
        return True
    except Exception as e:
        print(f"Failed to install {pkg}: {e}")
        return False

try:
    from flask import Flask, Response, render_template_string, request, jsonify
except ImportError:
    print("Installing Flask...")
    _install("flask")
    from flask import Flask, Response, render_template_string, request, jsonify

try:
    import requests
except ImportError:
    _install("requests")
    import requests

try:
    import pandas as pd
except ImportError:
    _install("pandas")
    import pandas as pd

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    print("Installing yfinance...")
    YF_AVAILABLE = _install("yfinance")
    try:
        import yfinance as yf
    except ImportError:
        YF_AVAILABLE = False

try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
except ImportError:
    _install("pytz")
    import pytz
    IST = pytz.timezone("Asia/Kolkata")

# fixed-offset IST for places that need a plain tz (swing bot used this)
IST_FIXED = dt.timezone(dt.timedelta(hours=5, minutes=30))

print(f"[OK] Python {sys.version.split()[0]} | libraries loaded")


# =============================================================================
# GLOBAL STATE
# =============================================================================

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 5000))

# Dhan credentials captured at login (needed for Options tab)
bot_credentials = {}          # {"client_id":..., "access_token":...}
logged_in = threading.Event()  # set once login succeeds

# per-engine start guards so background threads spin up only once
_started = {"intraday": False, "swing": False, "investment": False, "options": False}
_start_lock = threading.Lock()


# =============================================================================
# =============================================================================
#   ENGINE 1 : INTRADAY  (1-min Fibonacci breakout, yfinance)
#   -- core logic identical to your OptinKuber bot, HTTP server removed
# =============================================================================
# =============================================================================

INTRA_REQ_DELAY = 0.02      # tighter pause -> faster sweep
INTRA_MAX_WORKERS = 20      # more parallel fetches
INTRA_REFRESH_SEC = 180

# NOTE: Put your full 1000-symbol list here. A representative slice is used
# below so the file stays readable; extend INTRA_SYMBOLS freely.
INTRA_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY", "BHARTIARTL",
    "SBIN", "ITC", "LT", "KOTAKBANK", "AXISBANK", "HINDUNILVR",
    "BAJFINANCE", "M&M", "MARUTI", "SUNPHARMA", "TITAN", "TATASTEEL",
    "NTPC", "POWERGRID", "ONGC", "ADANIENT", "ADANIPORTS", "GRASIM",
    "HCLTECH", "TECHM", "WIPRO", "ASIANPAINT", "JSWSTEEL", "ULTRACEMCO",
    "NESTLEIND", "CIPLA", "DRREDDY", "EICHERMOT", "BAJAJ-AUTO", "HINDALCO",
    "COALINDIA", "BEL", "APOLLOHOSP", "INDIGO", "SBILIFE", "HDFCLIFE",
    "SHRIRAMFIN", "JIOFIN", "TATACONSUM", "TRENT", "ETERNAL", "MAXHEALTH",
    "TMPV", "BAJAJFINSV", "DIVISLAB", "HEROMOTOCO", "BRITANNIA", "INDUSINDBK",
    "BPCL", "DABUR", "MARICO", "PIDILITIND", "BERGEPAINT", "HAVELLS",
    "VOLTAS", "AUROPHARMA", "LUPIN", "VEDL", "TATAPOWER", "ADANIGREEN",
    "RECLTD", "PFC", "MUTHOOTFIN", "TVSMOTOR", "ASHOKLEY", "IRCTC",
    "DMART", "PAGEIND", "FEDERALBNK", "IDFCFIRSTB", "BANDHANBNK", "CANBK",
    "BANKBARODA", "PNB", "UNIONBANK", "TORNTPOWER", "IEX", "TATACHEM",
    "PIIND", "COROMANDEL", "ICICIPRULI", "DEEPAKNTR", "ATUL", "CHOLAFIN",
    "MPHASIS", "PERSISTENT", "LTIM", "COFORGE", "ZYDUSLIFE", "ALKEM",
    "POLYCAB", "KEI", "CUMMINSIND", "TIINDIA", "IRFC", "RVNL",
    "NHPC", "SJVN", "HUDCO", "NBCC", "NMDC", "OIL",
    "GAIL", "HAL", "IOC", "HINDPETRO", "IDBI", "INDIAMART",
    "INDHOTEL", "INDUSTOWER", "NAUKRI", "JUBLFOOD", "KALYANKJIL", "KANSAINER",
    "KEC", "LTF", "MANAPPURAM", "MFSL", "MRF", "NATCOPHARM",
    "SBICARD", "SAIL", "TATACOMM", "CONCOR", "COLPAL", "JINDALSTEL",
    "JSWENERGY", "GODREJCP", "GODREJPROP", "GRANULES", "GUJGASLTD", "HDFCAMC",
    "AEGISLOG", "ANGELONE", "APARINDS", "AUBANK", "BALKRISIND", "BATAINDIA",
    "BHARATFORG", "BHEL", "BLUEDART", "CANFINHOME", "CHAMBLFERT", "CROMPTON",
    "DALBHARAT", "DELHIVERY", "DIXON", "LALPATHLAB", "ESCORTS", "EXIDEIND",
    "FORTIS", "GLAND", "GLENMARK", "IGL", "IPCALAB", "KPITTECH",
    "ABBOTINDIA", "SCHAEFFLER", "BLUESTARCO", "GRINDWELL", "RELAXO", "NYKAA",
    "POLICYBZR", "OBEROIRLTY", "PETRONET", "ROUTE", "MOTHERSON", "NAVINFLUOR",
    "SRF", "BAYERCROP", "ASTRAL", "CENTURYPLY", "GREENPANEL", "METROPOLIS",
    "NH", "KIMS", "RBLBANK", "DCBBANK", "EQUITASBNK", "SURYODAY",
    "JKCEMENT", "RAMCOCEM", "BIRLACORPN", "ORIENTCEM", "RENUKA", "BAJAJHIND",
    "TRIVENI", "AKZOINDIA", "AARTIIND", "AMBER", "APOLLOTYRE", "CARERATING",
    "CRAFTSMAN", "GODREJIND", "GRAPHITE", "FORCEMOT", "KENNAMET", "GOODYEAR",
    "ABB", "AMJLAND", "ARVIND", "BALRAMCHIN", "CESC", "EIHOTEL",
    "FINPIPE", "FMGOETZE", "GIPCL", "GLFL", "HARRMALAYA", "HIMATSEIDE",
    "KANORICHEM", "KSB", "LYKALABS", "MAHSCOOTER", "MARALOVER", "NAVNETEDUL",
    "PEL", "PENINLAND", "VINDHYATEL", "WATERBASE", "WILLAMAGOR", "FINCABLES",
    "JAYSREETEA", "RSWM", "CGPOWER", "SHREYAS", "BAJAJHLDNG", "HEIDELBERG",
    "ABAN", "DHAMPURSUG", "EVERESTIND", "IFBIND", "LAXMIMACH", "ORIENTPPR",
    "OSWALAGRO", "PCBL", "RUBYMILLS", "SESHAPAPER", "STYRENIX", "DCMSHRIRAM",
    "FACT", "ZUARIIND", "BOMDYEING", "HIL", "IFCI", "SHREECEM",
    "21STCENMGM", "ADORWELD", "CENTENKA", "EIDPARRY", "HEG", "JAYNECOIND",
    "MASTEK", "SURYAROSNI", "WEIZMANIND", "BASF", "GNFC", "UNIVCABLES",
    "COSMOFIRST", "DCW", "GFLLIMITED", "SUDARSCHEM", "VLSFINANCE", "BPL",
    "HEUBACHIND", "CRISIL", "SEAMECLTD", "SOTL", "BALMLAWRIE", "BANARBEADS",
    "3PLAND", "HLVLTD", "SUPREMEIND", "ASHIMASYN", "BSL", "TAINWALCHM",
    "GMBREW", "UFLEX", "DICIND", "INDIANHUME", "THERMAX", "APCOTEXIND",
    "GSFC", "KABRAEXTRU", "LUMAXIND", "PRECWIRE", "SAKHTISUG", "SIEMENS",
    "IVP", "STARPAPER", "LIBERTSHOE", "ZODIACLOTH", "BHARATRAS", "PEARLPOLY",
    "PRAJIND", "TOKYOPLAST", "RAYMOND", "ANDHRSUGAR", "ESABINDIA", "NILKAMAL",
    "ELGIEQUIP", "SOMANYCERA", "RIIL", "BIRLACABLE", "SWARAJENG", "KAKATCEM",
    "PREMIERPOL", "JAYBARMARU", "KREBSBIO", "MANGLMCEM", "SPIC", "DALMIASUG",
    "NOCIL", "SHANTIGEAR", "TNPL", "GREENPLY", "CORALFINAC", "BBTC",
    "BHARATGEAR", "CARBORUNIV", "IFBAGRO", "NAVA", "EIMCOELECO", "JAYAGROGN",
    "PANACEABIO", "CUBEXTUB", "PGHL", "GICHSGFIN", "HINDCOMPOS", "SILINV",
    "KAMATHOTEL", "BIOFILCHEM", "ZENITHEXPO", "SANGAMIND", "GODFRYPHLP", "OSWALGREEN",
    "VISAKAIND", "KOTHARIPET", "RAMCOIND", "ACC", "HERITGFOOD", "INDIANCARD",
    "SKMEGGPROD", "KCP", "BANKINDIA", "INDNIPPON", "INDSWFTLAB", "MADRASFERT",
    "SAREGAMA", "DENORA", "FDC", "GUJALKALI", "SCI", "ENGINERSIN",
    "RANEHOLDIN", "JAICORPLTD", "GMDCLTD", "RCF", "SUNDARMFIN", "MAHAPEXLTD",
    "AMBUJACEM", "ASAHIINDIA", "TRIGYN", "KOTHARIPRO", "TATAINVEST", "J&KBANK",
    "TATAMOTORS", "LICHSGFIN", "CUB", "ORIENTHOT", "ARCHIES", "ZEEL",
    "CYIENT", "SOUTHBANK", "CYBERTECH", "SKFINDIA", "ITDCEM", "MTNL",
    "RALLIS", "INGERRAND", "MIRZAINT", "NATIONALUM", "PFIZER", "KOPRAN",
    "LINDEINDIA", "SONATSOFTW", "ALEMBICLTD", "KOHINOOR", "TNPETRO", "BSOFT",
    "MOREPENLAB", "ACCELYA", "TTKPRESTIG", "JCHAC", "STAR", "RAJESHEXPO",
    "TATACOFFEE", "WOCKPHARMA", "JAGSNPHARM", "NRBBEARING", "WHEELS", "BIL",
    "RAMCOSYS", "GEPIL", "KTKBANK", "MAHLIFE", "AJANTPHARM", "AGI",
    "BLBLIMITED", "INDIACEM", "CHENNPETRO", "SUNDRMFAST", "CALSOFT", "KARURVYSYA",
    "TFCILTD", "NLCINDIA", "PNBGILTS", "GAEL", "USHAMART", "TTML",
    "MRO-TEK", "NOVARTIND", "TIPSINDLTD", "BALAJITELE", "IOB", "STLTECH",
    "PNC", "GLAXO", "KPIL", "CREATIVEYE", "TAJGVK", "AXISCADES",
    "WSTCSTPAPR", "KHANDSE", "UNICHEMLAB", "HIKAL", "TRIDENT", "ONWARDTEC",
    "NEXTMEDIA", "SMARTLINK", "ASTRAZEN", "JISLJALEQS", "MCDOWELL-N", "HITECHGEAR",
    "JINDALPOLY", "KRBL", "MARKSANS", "MPSLTD", "TCI", "EPL",
    "ASAL", "OLECTRA", "PONNIERODE", "SURANAT&P", "TIMESGTY", "OFSS",
    "APTECHT", "VESUVIUS", "TORNTPHARM", "SANDESH", "ATFL", "NOIDATOLL",
    "NUCLEUS", "VTL", "DEEPAKFERT", "GENESYS", "OMAXAUTO", "TATASTLLP",
    "HCC", "HONDAPOWER", "STCINDIA", "POLYPLEX", "JBCHEPHARM", "JINDALSAW",
    "FOSECOIND", "BOSCHLTD", "MUNJALSHOW", "RICOAUTO", "TIRUMALCHM", "SANOFI",
    "TATAMETALI", "BANARISUG", "NELCO", "AUTOAXLES", "JUBLPHARMA", "RADICO",
    "ELECTCAST", "INDORAMA", "CENTURYTEX", "ZENSARTECH", "HONAUT", "BBOX",
    "VENKEYS", "GILLETTE", "GHCL", "PRSMJOHNSN", "SMLISUZU", "VSTIND",
    "ARE&M", "ITI", "TATAELXSI", "AARTIDRUGS", "HINDOILEXP", "VARDHACRLC",
    "IGARASHI", "UCOBANK", "NCC", "SUVEN", "BEML", "JSL",
    "NSIL", "WELSPUNIND", "TVSELECT", "JTEKTINDIA", "DREDGECORP", "TVTODAY",
    "UNIENTER", "UPL", "MUKANDLTD", "DHANBANK", "VAIBHAVGBL", "BIOCON",
    "PTC", "MAHABANK", "POONAWALLA", "XPROINDIA", "RKFORGE", "DATAMATICS",
    "ANDHRAPAP", "NDTV", "MURUDCERA", "KAJARIACER", "CCL", "JPASSOCIAT",
    "EXCELINDUS", "PGHH", "3MINDIA", "NIITLTD", "MAHSEAMLES", "GREAVESCOT",
    "TEXINFRA", "HUHTAMAKI", "GUFICBIO", "ASTRAMICRO", "WELENT", "DWARKESH",
    "MRPL", "INDOCO", "SUPRAJIT", "PATELENG", "RANASUG", "VIPIND",
    "XCHANGING", "BEPL", "CONSOFINVT", "JINDALPHOT", "JPPOWER", "GABRIEL",
    "EVEREADY", "GOKEX", "ALLSEC", "SAKSOFT", "IIFL", "MANGALAM",
    "SHOPERSTOP", "WELCORP", "SSWL", "SURYALAXMI", "JKPAPER", "PRIMESECU",
    "63MOONS", "JSWHL", "MANINDS", "VHL", "GEOJITFSL", "INDIAGLYCO",
    "GENUSPOWER", "YESBANK", "SUBROS", "NECLIFE", "SPLIL", "IDFC",
    "PRECOT", "GOLDIAM", "RML", "HTMEDIA", "SASKEN", "ICIL",
    "FCSSOFT", "SUNFLAG", "IMPAL", "BASML", "AIAENG", "EKC",
    "NAHARINDUS", "ORIENTCER", "VIMTALABS", "REPRO", "PVRINOX", "CELEBRITY",
    "TINPLATE", "NITINSPIN", "STERTOOLS", "ROHLTD", "ENIL", "GSPL",
    "JAGRAN", "BLKASHYAP", "M&MFIN", "NITCO", "SOLARINDS", "VAKRANGEE",
    "GALLANTT", "MALUPAPER", "UTTAMSUGAR", "KKCL", "SUNTV", "GPIL",
    "RSYSTEMS", "EMKAY", "LOKESHMACH", "BALPHARMA", "KAMDHENU", "RATNAMANI",
    "SWELECTES", "JKLAKSHMI", "PFOCUS", "ALLCARGO", "MUNJALAU", "EMAMILTD",
    "WENDT", "GMRINFRA", "DYNAMATECH", "RAMANEWS", "VOLTAMP", "ACE",
    "SELAN", "ANANTRAJ", "HOVS", "ALICON", "ELECON", "TALBROAUTO",
    "GEECEE", "MADHUCON", "GATI", "JMFINANCIL", "FIEMIND", "JHS",
    "GLOBALVECT", "SHYAMTEL", "GTLINFRA", "AARVEEDEN", "BBL", "HINDZINC",
    "GESHIP", "MANALIPETC", "SUTLEJTEX", "DAAWAT", "DONEAR", "MMFL",
    "RUCHIRA", "SOBHA", "VENUSREM", "BANCOINDIA", "JINDRILL", "HIRECT",
    "CREST", "NFL", "PLASTIBLEN", "HBLPOWER", "TANLA", "AVTNPL",
    "ZEEMEDIA", "LUMAXTECH", "TIDEWATER", "SARLAPOLY", "SANGHVIMOV", "IGPL",
    "PTL", "SAGCEM", "SIYSIL", "GANDHITUBE", "NETWORK18", "PITTIENG",
    "UNOMINDA", "TIMKEN", "CTE", "HUBTOWN", "TV18BRDCST", "TIIL",
    "TVSSRICHAK", "ORIENTBELL", "REDINGTON", "FSL", "TTL", "TFL",
    "SMSPHARMA", "INDIANB", "GANESHHOUC", "HSCL", "ORIENTALTL", "PUNJABCHEM",
    "IDEA", "LAOPALA", "RAJTV", "AMDIND", "JKTYRE", "IBREALEST",
    "NAHARPOLY", "NAHARSPING", "THEMISMED", "GARFIBRES", "NCLIND", "BALAMINES",
    "BANSWRAS", "ICRA", "DISHTV", "PHOENIXLTD", "BFUTILITIE", "SRHHYPOLTD",
    "ALPHAGEO", "WEBELSOLAR", "TGBHOTELS", "HILTON", "INSECTICID", "KMSUGAR",
    "GOACARBON", "TIMETECHNO", "NAGREEKEXP", "HGS", "ADVANIHOTR", "NELCAST",
    "DLF", "TARMAT", "SPARC", "ADSL", "DECCANCE", "ALPA",
    "VIPCLOTHNG", "OMAXE", "CENTRALBK", "ASIANTILES", "ISMTLTD", "KPRMILL",
    "CIEINDIA", "PURVA", "MOTILALOFS", "KSCL", "MANGCHEFER", "BAJAJELEC",
    "CERA", "DELTACORP", "ENERGYDEV", "RELIGARE", "ALKYLAMINE", "EDELWEISS",
    "RGL", "KOLTEPATIL", "JYOTHYLAB", "HITECHCORP", "MADHAV", "TRIL",
    "BRIGADE", "ECLERX", "BGRENERGY", "BURNPUR", "MANAKSIA", "ARIES",
    "DVL", "PPAP", "AMBIKCO", "CEATLTD", "BIRLAMONEY", "RPOWER",
    "JKIL", "CORDSCABLE", "KNRCON", "HERCULES", "ONMOBILE", "BANG",
    "SEPC", "IRB", "RAIN", "SHALPAINTS", "GSS", "NAHARCAP",
    "VGUARD", "EIHAHOTELS", "NESCO", "DPSCLTD", "TITAGARH", "KIRIINDUS",
    "GOKUL", "RPGLIFE", "ALMONDZ", "RBL", "ARCHIDPLY", "VINYLINDIA",
    "UBL", "LGBFORGE", "ZFCVINDIA", "JOCIL", "NEULANDLAB", "ALKALI",
    "COUNCODOS", "MAWANASUG", "AJMERA", "MHRIL", "GKWLIMITED", "VINATIORGA",
    "ADANIPOWER", "GLOBUSSPR", "EXPLEOSOL", "RTNPOWER", "SUNTECK", "ZYDUSWELL",
    "DEN", "ASTEC", "REFEX", "SARDAEN", "SHILPAMED", "AHLUCONT",
    "DLINKINDIA", "DBCORP", "MBLINFRA", "VASCONEQ", "THANGAMAYL", "DBREALTY",
    "EMMBI", "HATHWAY", "KECL", "MANINFRA", "TRF", "LGBBROSLTD",
    "WHIRLPOOL", "ASIANHOTNR", "ISFT", "KIRLOSBROS", "KSL", "KIRLOSIND",
    "TI", "HMVL", "IMFA", "BLISSGVS", "EMAMIREAL", "AHLEAST",
    "IITL", "BAJAJCON", "UGARSUGAR", "GPPL", "ADFFOODS", "HINDCOPPER",
    "WELINV", "ORISSAMINE", "SASTASUNDR", "EROSMEDIA", "RAMKY", "CANTABIL",
    "WABAG", "ASHOKA", "JWL", "PRESTIGE", "SHAH", "CGCL",
    "RESPONIND", "IOLCP", "GRAVITA", "JINDWORLD", "PENIND", "JAMNAAUTO",
    "MOIL", "KIRLOSENG", "PSB", "BFINVEST", "KICL", "SUMMITSEC",
    "JUBLINDS", "HINDMOTORS", "TEXRAIL", "HFCL", "INDTERRAIN", "DHUNINV",
    "LOVABLE", "PFS", "SYMPHONY", "VADILALIND", "ESTER", "VSTTILLERS",
    "DELPHIFX", "ASHIANA", "DHANUKA", "MAITHANALL", "RUSHIL", "INVENTURE",
    "BODALCHEM", "WINSOME", "TREEHOUSE", "ASAHISONG", "FILATEX", "TDPOWERSYS",
    "APLLTD", "PGEL", "PANAMAPET", "TIJARIA", "ONELIFECAP", "PAISALO",
    "UJAAS", "TRITURBINE", "INDOTHAI", "DSSL", "POLYMED", "RUPA",
    "APLAPOLLO", "KANANIIND", "DBSTOCKBRO", "COMPUSOFT", "MCX", "RHIM",
    "SCHNEIDER", "NDL", "MMTC", "HEXATRADEX", "TBZ", "VSSL",
    "SWANENERGY", "SPECIALITY", "RTNINDIA", "PROZONER", "MORARJEE", "KITEX",
    "MAYURUNIQ", "ROSSELLIND", "TVSHLTD", "ZUARI", "PCJEWELLER", "VMART",
    "REPCOHOME", "VIVIDHA", "JUSTDIAL", "ATULAUTO", "ABFRL", "IBULHSGFIN",
    "PILITA", "ORBTEXP", "JPOLYINVST", "SREEL", "NATHBIOGEN", "SECURKLOUD",
    "CASTROLIND", "SDBL", "BUTTERFLY", "WONDERLA", "AGARIND", "HATSUN",
    "CAPLIPOINT", "GOCLCORP", "GULFOILLUB", "SNOWMAN", "SHARDACROP", "SUPERHOUSE",
    "SHEMAROO", "CIGNITITEC", "MINDACORP", "PDSL", "INTELLECT", "MONTECARLO",
    "AMRUTANJAN", "GOODLUCK", "FCL", "CAMLINFINE", "DTIL", "APOLSINHOT",
    "GULPOLY", "UNITEDTEA", "SHAKTIPUMP", "LAMBODHARA", "GRPLTD", "GENUSPAPER",
    "MOLDTKPAC", "GREENLAM", "VISHNU", "GANECOS", "LINC", "MENONBE",
    "HESTERBIO", "JMA", "MANAKALUCO", "MANAKSTEEL", "IMAGICAA", "INOXWIND",
    "AVANTIFEED", "LFIC", "VIPULLTD", "VETO",
]

# Symbols that Yahoo consistently 404s (delisted/merged/renamed). Skipped
# everywhere so they never waste a fetch and never slow the sweep.
# Add any symbol you see repeatedly logging "possibly delisted".
DEAD_TICKERS = {
    "LTIM",        # merged into LTIMINDTREE on Yahoo
    "TMPV",        # Tata Motors PV demerger ticker, not on Yahoo yet
    "ETERNAL",     # not resolvable on Yahoo
}
INTRA_SYMBOLS = [s for s in INTRA_SYMBOLS if s not in DEAD_TICKERS]

INTRA_NAMES = {
    "RELIANCE": "Reliance Industries", "TCS": "Tata Consultancy Services",
    "HDFCBANK": "HDFC Bank", "ICICIBANK": "ICICI Bank", "INFY": "Infosys",
    "BHARTIARTL": "Bharti Airtel", "SBIN": "State Bank of India", "ITC": "ITC",
    "LT": "Larsen & Toubro", "KOTAKBANK": "Kotak Mahindra Bank",
}


@dataclass
class FibLevel:
    ratio: float
    price: float
    color: str


@dataclass
class TradeSetup:
    symbol: str
    direction: str
    entry_price: float
    sl_price: float
    tp_price: float
    breakout_level_price: float
    breakout_ratio: float
    breakout_color: str
    risk: float
    reward: float
    level_x: float
    setup_candle_time: str
    entry_candle_time: str
    body_pct: float
    wick_pct: float
    trade_date: str
    status: str = "ACTIVE"
    exit_price: float = 0.0
    exit_time: str = ""
    mtm: float = 0.0
    mtm_pct: float = 0.0
    max_favorable: float = 0.0
    max_adverse: float = 0.0


@dataclass
class IntraStock:
    symbol: str
    name: str
    current_price: float = 0.0
    daily_high: float = 0.0
    daily_low: float = 0.0
    change_pct: float = 0.0
    fib_levels: List[dict] = field(default_factory=list)
    trade: Optional[TradeSetup] = None
    candles_scanned: int = 0
    last_candle_time: str = ""
    scan_state: str = "SCANNING"
    pending_info: str = ""
    last_update: str = ""
    error: str = ""


class IntradayEngine:
    FIB_RATIOS = [-1.618, -1.118, -0.618, -0.202, 0.214, 0.5, 0.786,
                  1.202, 1.618, 2.118, 2.618, 3.118, 3.618, 4.236]
    BLUE_RATIOS = [-1.618, -0.618, 0.214, 0.786, 1.618, 2.618, 3.618, 4.236]
    YELLOW_RATIOS = [-1.118, -0.202, 0.5, 1.202, 2.118, 3.118]
    MIN_BODY_PCT = 70.0
    MAX_WICK_PCT = 30.0
    SCAN_START = dt.time(9, 21, 59)
    MARKET_CLOSE = dt.time(15, 30, 0)
    MAX_FAILS = 3

    def __init__(self, max_workers=INTRA_MAX_WORKERS):
        self.stocks_data: Dict[str, IntraStock] = {}
        self.locked_trades: Dict[str, TradeSetup] = {}
        self.pending_setups: Dict[str, dict] = {}
        self.fail_counts: Dict[str, int] = {}
        self.dead_symbols: set = set()
        self.session_date: str = self._today_str()
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.scan_count = 0
        self.is_updating = False

    @staticmethod
    def _today_str():
        return dt.datetime.now(IST).strftime("%Y-%m-%d")

    def _rollover_if_new_day(self):
        today = self._today_str()
        if today != self.session_date:
            with self.lock:
                self.locked_trades.clear()
                self.pending_setups.clear()
                self.fail_counts.clear()
                self.dead_symbols.clear()
                for s in self.stocks_data.values():
                    s.trade = None
                    s.scan_state = "SCANNING"
                    s.pending_info = ""
                self.session_date = today

    def get_color(self, ratio):
        if ratio in self.BLUE_RATIOS:
            return "blue"
        if ratio in self.YELLOW_RATIOS:
            return "yellow"
        return "gray"

    def calculate_fib_levels(self, dh, dl):
        rng = dh - dl
        levels = [FibLevel(r, dl + r * rng, self.get_color(r)) for r in self.FIB_RATIOS]
        levels.sort(key=lambda x: x.price)
        return levels

    def _to_ist(self, ts):
        d = ts.to_pydatetime()
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(IST)

    def fetch_intraday_candles(self, symbol, interval="1m"):
        try:
            time.sleep(INTRA_REQ_DELAY)
            ticker = yf.Ticker(f"{symbol}.NS")
            df = ticker.history(period="1d", interval=interval, prepost=False)
            if df.empty:
                return []
            candles = []
            for index, row in df.iterrows():
                ct = self._to_ist(index)
                if not (dt.time(9, 15) <= ct.time() <= self.MARKET_CLOSE):
                    continue
                candles.append({
                    'time': ct, 'open': float(row['Open']), 'high': float(row['High']),
                    'low': float(row['Low']), 'close': float(row['Close']),
                    'volume': int(row['Volume']) if 'Volume' in row and row['Volume'] == row['Volume'] else 0
                })
            candles.sort(key=lambda c: c['time'])
            return candles
        except Exception as e:
            return []

    def fetch_daily_high_low(self, symbol):
        try:
            time.sleep(INTRA_REQ_DELAY)
            ticker = yf.Ticker(f"{symbol}.NS")
            hist = ticker.history(period="7d", interval="1d")
            if hist.empty or len(hist) < 2:
                return None, None
            prev = hist.iloc[-2]
            return float(prev['High']), float(prev['Low'])
        except Exception:
            return None, None

    def check_body_wick(self, o, h, l, c):
        total = h - l
        if total <= 0:
            return False, 0.0, 100.0
        body_pct = (abs(c - o) / total) * 100.0
        wick_pct = 100.0 - body_pct
        ok = (body_pct >= self.MIN_BODY_PCT) and (wick_pct <= self.MAX_WICK_PCT)
        return ok, body_pct, wick_pct

    def find_next_fib_level(self, fib_levels, price, direction):
        levels = sorted(fib_levels, key=lambda x: x.price)
        if direction == "LONG":
            for lv in levels:
                if lv.price > price:
                    return lv
        else:
            for lv in reversed(levels):
                if lv.price < price:
                    return lv
        return None

    def calculate_level_x(self, breakout, nxt, direction):
        if nxt is None:
            return breakout.price
        y = abs(nxt.price - breakout.price) / 4.0
        return breakout.price + y if direction == "LONG" else breakout.price - y

    def calculate_sl_tp(self, entry, breakout, direction, fib_levels):
        opp = "SHORT" if direction == "LONG" else "LONG"
        sl_next = self.find_next_fib_level(fib_levels, breakout.price, opp)
        if sl_next is None:
            sl_dist = abs(entry - breakout.price) / 2.0 or 0.05
        else:
            sl_dist = abs(breakout.price - sl_next.price) / 2.0
        if direction == "LONG":
            return breakout.price - sl_dist, entry + sl_dist, sl_dist
        return breakout.price + sl_dist, entry - sl_dist, sl_dist

    def find_crossed_level(self, o, c, fib_levels):
        crossed = []
        for lv in fib_levels:
            if o < lv.price < c:
                crossed.append((abs(lv.price - o), lv, "LONG"))
            elif c < lv.price < o:
                crossed.append((abs(lv.price - o), lv, "SHORT"))
        if not crossed:
            return None, None
        crossed.sort(key=lambda t: t[0])
        _, lv, direction = crossed[0]
        return lv, direction

    def process_candles(self, symbol, candles, fib_levels):
        pending = self.pending_setups.get(symbol)
        today = self._today_str()
        for candle in candles:
            ct = candle['time']
            if ct.time() < self.SCAN_START:
                continue
            o, h, l, c = candle['open'], candle['high'], candle['low'], candle['close']
            if pending is not None:
                if ct.strftime("%H:%M:%S") == pending['setup_time']:
                    continue
                breakout = pending['level']
                direction = pending['direction']
                tp_next = self.find_next_fib_level(fib_levels, breakout.price, direction)
                level_x = self.calculate_level_x(breakout, tp_next, direction)
                if direction == "LONG":
                    in_range = breakout.price < o < level_x
                else:
                    in_range = level_x < o < breakout.price
                if in_range:
                    sl, tp, risk = self.calculate_sl_tp(o, breakout, direction, fib_levels)
                    trade = TradeSetup(
                        symbol=symbol, direction=direction, entry_price=round(o, 2),
                        sl_price=round(sl, 2), tp_price=round(tp, 2),
                        breakout_level_price=round(breakout.price, 2),
                        breakout_ratio=breakout.ratio, breakout_color=breakout.color,
                        risk=round(risk, 2), reward=round(risk, 2), level_x=round(level_x, 2),
                        setup_candle_time=pending['setup_time'],
                        entry_candle_time=ct.strftime("%H:%M:%S"),
                        body_pct=pending['body_pct'], wick_pct=pending['wick_pct'],
                        trade_date=today, status="ACTIVE",
                        max_favorable=round(o, 2), max_adverse=round(o, 2))
                    self.pending_setups.pop(symbol, None)
                    return trade
                self.pending_setups.pop(symbol, None)
                pending = None
            level, direction = self.find_crossed_level(o, c, fib_levels)
            if level is None:
                continue
            ok, body_pct, wick_pct = self.check_body_wick(o, h, l, c)
            if not ok:
                continue
            pending = {
                'direction': direction, 'level': level,
                'setup_time': ct.strftime("%H:%M:%S"),
                'body_pct': round(body_pct, 2), 'wick_pct': round(wick_pct, 2)}
            self.pending_setups[symbol] = pending
        return None

    def monitor_trade(self, trade, candles, ltp):
        if trade.status != "ACTIVE":
            return
        entry_t = trade.entry_candle_time
        started = False
        for candle in candles:
            hhmmss = candle['time'].strftime("%H:%M:%S")
            if not started:
                if hhmmss == entry_t:
                    started = True
                continue
            hi, lo = candle['high'], candle['low']
            if trade.direction == "LONG":
                trade.max_favorable = max(trade.max_favorable, hi)
                trade.max_adverse = min(trade.max_adverse, lo)
                if lo <= trade.sl_price:
                    trade.status = "SL_HIT"; trade.exit_price = trade.sl_price
                    trade.exit_time = hhmmss; break
                if hi >= trade.tp_price:
                    trade.status = "TP_HIT"; trade.exit_price = trade.tp_price
                    trade.exit_time = hhmmss; break
            else:
                trade.max_favorable = min(trade.max_favorable, lo)
                trade.max_adverse = max(trade.max_adverse, hi)
                if hi >= trade.sl_price:
                    trade.status = "SL_HIT"; trade.exit_price = trade.sl_price
                    trade.exit_time = hhmmss; break
                if lo <= trade.tp_price:
                    trade.status = "TP_HIT"; trade.exit_price = trade.tp_price
                    trade.exit_time = hhmmss; break
        mark = ltp if trade.status == "ACTIVE" else trade.exit_price
        if trade.direction == "LONG":
            trade.mtm = round(mark - trade.entry_price, 2)
        else:
            trade.mtm = round(trade.entry_price - mark, 2)
        trade.mtm_pct = round((trade.mtm / trade.entry_price) * 100, 2) if trade.entry_price else 0.0
        trade.max_favorable = round(trade.max_favorable, 2)
        trade.max_adverse = round(trade.max_adverse, 2)

    def update_stock(self, symbol):
        self._rollover_if_new_day()
        stock = self.stocks_data.get(symbol) or IntraStock(symbol, INTRA_NAMES.get(symbol, symbol))
        stock.error = ""
        candles = self.fetch_intraday_candles(symbol, "1m")
        if not candles:
            with self.lock:
                self.fail_counts[symbol] = self.fail_counts.get(symbol, 0) + 1
                if self.fail_counts[symbol] >= self.MAX_FAILS:
                    self.dead_symbols.add(symbol)
            stock.scan_state = "NO_DATA"
            n = self.fail_counts.get(symbol, 0)
            stock.error = (f"delisted/bad ticker ({n} fails)"
                           if symbol in self.dead_symbols else f"no intraday candles ({n})")
            stock.last_update = dt.datetime.now(IST).strftime("%H:%M:%S")
            with self.lock:
                self.stocks_data[symbol] = stock
            return
        with self.lock:
            self.fail_counts.pop(symbol, None)
        ltp = candles[-1]['close']
        stock.current_price = round(ltp, 2)
        stock.candles_scanned = len(candles)
        stock.last_candle_time = candles[-1]['time'].strftime("%H:%M:%S")
        stock.change_pct = round(((ltp - candles[0]['open']) / candles[0]['open']) * 100, 2)
        if not stock.fib_levels or stock.daily_high == 0:
            dh, dl = self.fetch_daily_high_low(symbol)
            if dh is None:
                stock.scan_state = "NO_DATA"; stock.error = "no daily data"
                with self.lock:
                    self.stocks_data[symbol] = stock
                return
            stock.daily_high, stock.daily_low = round(dh, 2), round(dl, 2)
            levels = self.calculate_fib_levels(dh, dl)
            stock.fib_levels = [{"ratio": f.ratio, "price": round(f.price, 2), "color": f.color}
                                for f in levels]
        fib_levels = [FibLevel(d["ratio"], d["price"], d["color"]) for d in stock.fib_levels]
        locked = self.locked_trades.get(symbol)
        if locked is not None:
            self.monitor_trade(locked, candles, ltp)
            stock.trade = locked; stock.scan_state = "LOCKED"
            stock.pending_info = f"{locked.status} | LTP {ltp:.2f}"
        else:
            trade = self.process_candles(symbol, candles, fib_levels)
            if trade is not None:
                with self.lock:
                    self.locked_trades[symbol] = trade
                self.monitor_trade(trade, candles, ltp)
                stock.trade = trade; stock.scan_state = "LOCKED"
            else:
                p = self.pending_setups.get(symbol)
                if p:
                    stock.scan_state = "PENDING_ENTRY"
                    stock.pending_info = (f"{p['direction']} breakout @ fib {p['level'].ratio} "
                                          f"({p['setup_time']}) -- awaiting next candle open")
                else:
                    stock.scan_state = "SCANNING"
                    stock.pending_info = "monitoring every 1-min candle"
        stock.last_update = dt.datetime.now(IST).strftime("%H:%M:%S")
        with self.lock:
            self.stocks_data[symbol] = stock

    def update_all(self):
        self.is_updating = True
        self.scan_count += 1
        live = [s for s in INTRA_SYMBOLS if s not in self.dead_symbols]
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self.update_stock, s): s for s in live}
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception:
                    pass
        self.is_updating = False

    def get_dashboard_data(self):
        with self.lock:
            stocks = list(self.stocks_data.values())
        out = []
        for s in stocks:
            send_fibs = bool(s.trade) or s.scan_state == "PENDING_ENTRY"
            out.append({
                "symbol": s.symbol, "name": s.name, "price": s.current_price,
                "high": s.daily_high, "low": s.daily_low, "change": s.change_pct,
                "fib_levels": s.fib_levels if send_fibs else [],
                "trade": asdict(s.trade) if s.trade else None,
                "scan_state": s.scan_state, "pending_info": s.pending_info,
                "candles_scanned": s.candles_scanned,
                "last_candle_time": s.last_candle_time,
                "last_update": s.last_update, "error": s.error})
        out.sort(key=lambda x: (x["trade"] is None, x["symbol"]))
        trades = list(self.locked_trades.values())
        return {
            "stocks": out, "session_date": self.session_date,
            "last_update": dt.datetime.now(IST).strftime("%H:%M:%S"),
            "scan_count": self.scan_count, "is_updating": self.is_updating,
            "universe": len(INTRA_SYMBOLS), "dead": len(self.dead_symbols),
            "summary": {
                "total_trades": len(trades),
                "active": sum(1 for t in trades if t.status == "ACTIVE"),
                "tp_hit": sum(1 for t in trades if t.status == "TP_HIT"),
                "sl_hit": sum(1 for t in trades if t.status == "SL_HIT"),
                "scanning": sum(1 for s in stocks if s.scan_state == "SCANNING"),
                "pending": sum(1 for s in stocks if s.scan_state == "PENDING_ENTRY"),
                "net_mtm": round(sum(t.mtm for t in trades), 2)}}

    def loop(self):
        while True:
            try:
                self.update_all()
            except Exception as e:
                print(f"[intraday] loop error: {e}")
            time.sleep(INTRA_REFRESH_SEC)


INTRADAY = IntradayEngine()


# =============================================================================
# =============================================================================
#   ENGINE 2 : SWING  (1M levels / 1D candles Fibonacci, yfinance)
#   -- core logic identical to your monthly Fib bot
# =============================================================================
# =============================================================================

SWING_FIB = [-1.618, -1.118, -0.618, -0.202, 0.214, 0.5, 0.786,
             1.202, 1.618, 2.118, 2.618, 3.118, 3.618, 4.236]
SWING_REFRESH = 1800
SWING_MAX_WORKERS = 20
SWING_MARKET_CLOSE = dt.time(15, 30)

# Same universe as intraday (NSE symbol -> yahoo ticker). Extend as needed.
SWING_STOCKS = {s: f"{s}.NS" for s in INTRA_SYMBOLS}


@dataclass
class SwCandle:
    timestamp: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float = 0

    @property
    def is_green(self): return self.close_price >= self.open_price
    @property
    def is_red(self): return self.close_price < self.open_price

    def to_dict(self):
        return {"timestamp": self.timestamp, "open": round(self.open_price, 2),
                "high": round(self.high_price, 2), "low": round(self.low_price, 2),
                "close": round(self.close_price, 2), "volume": int(self.volume),
                "color": "GREEN" if self.is_green else "RED"}


@dataclass
class SwSignal:
    direction: str
    fib_level: float
    fib_price: float
    timestamp: str
    candle_close: float
    candle_open: float
    week_start_date: str = ""

    def to_dict(self):
        return {"direction": self.direction, "fib_level": self.fib_level,
                "fib_price": self.fib_price, "timestamp": self.timestamp,
                "candle_close": self.candle_close, "candle_open": self.candle_open,
                "week_start_date": self.week_start_date}


@dataclass
class SwTrade:
    trade_id: str
    symbol: str
    direction: str
    entry_price: float
    sl_price: float
    tp_price: float
    fib_level: float
    fib_price: float
    status: str
    open_time: str
    close_time: Optional[str] = None
    close_price: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    week_start_date: str = ""

    def to_dict(self):
        return {"trade_id": self.trade_id, "symbol": self.symbol,
                "direction": self.direction, "entry_price": self.entry_price,
                "sl_price": self.sl_price, "tp_price": self.tp_price,
                "fib_level": self.fib_level, "fib_price": self.fib_price,
                "status": self.status, "open_time": self.open_time,
                "close_time": self.close_time, "close_price": self.close_price,
                "pnl": self.pnl, "pnl_percent": self.pnl_percent,
                "week_start_date": self.week_start_date}


@dataclass
class SwStock:
    symbol: str
    yf_ticker: str = ""
    company_name: str = ""
    weekly_high: float = 0
    weekly_low: float = 0
    weekly_open: float = 0
    weekly_close: float = 0
    current_price: float = 0
    prev_close: float = 0
    day_high: float = 0
    day_low: float = 0
    fib_levels: Dict[str, float] = field(default_factory=dict)
    last_signal: Optional[SwSignal] = None
    active_trade: Optional[SwTrade] = None
    pending_signal: Optional[SwSignal] = None
    closed_trades: List[SwTrade] = field(default_factory=list)
    last_closed_trade: Optional[SwTrade] = None
    all_trades_history: List[SwTrade] = field(default_factory=list)
    hourly_candles: List[SwCandle] = field(default_factory=list)
    last_candle: Optional[SwCandle] = None
    prev_candle: Optional[SwCandle] = None
    last_updated: str = ""
    change_percent: float = 0
    total_pnl: float = 0
    signals_this_week: int = 0
    data_source: str = ""
    fetch_error: str = ""
    current_week_start: str = ""
    prev_week_start: str = ""
    signalled_levels: set = field(default_factory=set)

    def to_dict(self):
        return {"symbol": self.symbol, "company_name": self.company_name,
                "weekly_high": self.weekly_high, "weekly_low": self.weekly_low,
                "weekly_open": self.weekly_open, "weekly_close": self.weekly_close,
                "current_price": self.current_price, "prev_close": self.prev_close,
                "day_high": self.day_high, "day_low": self.day_low,
                "fib_levels": self.fib_levels,
                "last_signal": self.last_signal.to_dict() if self.last_signal else None,
                "pending_signal": self.pending_signal.to_dict() if self.pending_signal else None,
                "active_trade": self.active_trade.to_dict() if self.active_trade else None,
                "last_closed_trade": self.last_closed_trade.to_dict() if self.last_closed_trade else None,
                "closed_trades": [t.to_dict() for t in self.closed_trades],
                "week_trade_count": len(self.closed_trades) + (1 if self.active_trade else 0),
                "last_candle": self.last_candle.to_dict() if self.last_candle else None,
                "last_updated": self.last_updated,
                "change_percent": round(self.change_percent, 2),
                "total_pnl": round(self.total_pnl, 2),
                "signals_this_week": self.signals_this_week,
                "data_source": self.data_source, "fetch_error": self.fetch_error,
                "current_week_start": self.current_week_start,
                "prev_week_start": self.prev_week_start}


class SwFetcher:
    def __init__(self):
        self.tickers = {}
        for symbol, yf_sym in SWING_STOCKS.items():
            try:
                self.tickers[symbol] = yf.Ticker(yf_sym)
            except Exception:
                pass

    def fetch(self, symbol):
        result = {"lastPrice": 0, "previousClose": 0, "open": 0, "dayHigh": 0,
                  "dayLow": 0, "change": 0, "pChange": 0, "weeklyHigh": 0,
                  "weeklyLow": 0, "weeklyOpen": 0, "weeklyClose": 0,
                  "companyName": symbol, "symbol": symbol, "weekStart": "",
                  "prevWeekStart": "", "hourly_candles": [], "source": "", "error": ""}
        try:
            ticker = self.tickers.get(symbol)
            if not ticker:
                result["error"] = "Ticker not initialized"; return result
            try:
                hist_d = ticker.history(period="1y", interval="1d")
                if not hist_d.empty:
                    didx = hist_d.index
                    try:
                        if didx.tz is None:
                            hist_d.index = didx.tz_localize("UTC").tz_convert(IST_FIXED)
                        else:
                            hist_d.index = didx.tz_convert(IST_FIXED)
                    except Exception:
                        pass
                    months = {}
                    for ts, row in hist_d.iterrows():
                        d = ts.date()
                        months.setdefault((d.year, d.month), []).append((d, row))
                    keys = sorted(months.keys())
                    if len(keys) >= 2:
                        last_key = keys[-2]; run_key = keys[-1]
                        rows = sorted(months[last_key], key=lambda x: x[0])
                        result["weeklyHigh"] = float(max(r["High"] for _, r in rows))
                        result["weeklyLow"] = float(min(r["Low"] for _, r in rows))
                        result["weeklyOpen"] = float(rows[0][1]["Open"])
                        result["weeklyClose"] = float(rows[-1][1]["Close"])
                        result["prevWeekStart"] = rows[0][0].strftime("%Y-%m-%d")
                        run_rows = sorted(months[run_key], key=lambda x: x[0])
                        result["weekStart"] = run_rows[0][0].strftime("%Y-%m-%d")
                    candles = []
                    for ts, row in hist_d.iterrows():
                        candles.append(SwCandle(
                            timestamp=ts.strftime("%Y-%m-%d %H:%M"),
                            open_price=float(row["Open"]), high_price=float(row["High"]),
                            low_price=float(row["Low"]), close_price=float(row["Close"]),
                            volume=float(row["Volume"])))
                    result["hourly_candles"] = candles
                    if candles:
                        last = candles[-1]
                        result["lastPrice"] = last.close_price
                        result["open"] = last.open_price
                        result["dayHigh"] = last.high_price
                        result["dayLow"] = last.low_price
                        if len(candles) >= 2:
                            result["previousClose"] = candles[-2].close_price
                        if result["previousClose"] > 0:
                            result["change"] = result["lastPrice"] - result["previousClose"]
                            result["pChange"] = (result["change"] / result["previousClose"]) * 100
            except Exception:
                pass
            result["source"] = "yahoo_finance"
            return result
        except Exception as e:
            result["error"] = str(e); return result


class SwFib:
    def calculate(self, high, low):
        if high <= low or high == 0 or low == 0:
            return {}
        rv = high - low
        levels = {str(m): round(low + rv * m, 2) for m in SWING_FIB}
        return dict(sorted(levels.items(), key=lambda x: x[1]))

    def get_previous_level(self, levels, level_price, direction):
        prices = sorted(levels.values())
        if direction == "BUY":
            c = [p for p in prices if p < level_price]
            return c[-1] if c else None
        else:
            c = [p for p in prices if p > level_price]
            return c[0] if c else None


class SwingEngine:
    def __init__(self):
        self.fetcher = SwFetcher()
        self.fib_calc = SwFib()
        self.stocks: Dict[str, SwStock] = {}
        self.trade_counter = 0
        for symbol, yf_ticker in SWING_STOCKS.items():
            self.stocks[symbol] = SwStock(symbol=symbol, yf_ticker=yf_ticker)

    def _cur_week(self):
        today = dt.datetime.now()
        monday = today - dt.timedelta(days=today.weekday())
        return monday.strftime("%Y-%m-%d")

    def _is_new_week(self, stock, week_start):
        return stock.current_week_start != "" and stock.current_week_start != week_start

    def _close_weekly(self, symbol):
        stock = self.stocks[symbol]
        if stock.active_trade:
            trade = stock.active_trade
            trade.close_price = stock.current_price if stock.current_price > 0 else trade.entry_price
            trade.status = "CLOSED_WEEKLY"
            trade.close_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if trade.direction == "BUY":
                trade.pnl = round(trade.close_price - trade.entry_price, 2)
            else:
                trade.pnl = round(trade.entry_price - trade.close_price, 2)
            trade.pnl_percent = round((trade.pnl / trade.entry_price) * 100, 2)
            if not any(t.trade_id == trade.trade_id for t in stock.all_trades_history):
                stock.all_trades_history.append(trade)
            stock.active_trade = None
        stock.signals_this_week = 0
        stock.last_signal = None
        stock.pending_signal = None
        stock.closed_trades = []
        stock.last_closed_trade = None
        stock.signalled_levels = set()

    def _apply(self, symbol, data):
        """Process one symbol's fetched data into engine state (progressive)."""
        if symbol not in self.stocks:
            self.stocks[symbol] = SwStock(symbol=symbol)
        try:
            if data.get("weeklyHigh", 0) > 0 and data.get("weeklyLow", 0) > 0:
                stock = self.stocks[symbol]
                week_start = data.get("weekStart", "") or self._cur_week()
                if self._is_new_week(stock, week_start):
                    self._close_weekly(symbol)
                stock.current_week_start = week_start
                stock.prev_week_start = data.get("prevWeekStart", "")
                self._update_stock(symbol, data)
            else:
                self.stocks[symbol].fetch_error = data.get("error", "No monthly data")
        except Exception as e:
            self.stocks[symbol].fetch_error = str(e)

    def update_all(self):
        # Swing uses monthly levels / daily candles -> re-fetching the whole
        # universe every round is wasteful and starves Intraday. Full sweep
        # only every 15 min; other rounds return instantly.
        now = time.time()
        last = getattr(self, "_last_sweep_ts", 0)
        if getattr(self, "_swept_once", False) and (now - last) < 900:
            return
        # Progressive: process each symbol the moment its fetch returns, so the
        # Swing tab fills in live instead of waiting for the whole universe.
        symbols = [s for s in SWING_STOCKS.keys() if s not in DEAD_TICKERS]
        with ThreadPoolExecutor(max_workers=SWING_MAX_WORKERS) as pool:
            futures = {pool.submit(self.fetcher.fetch, sym): sym for sym in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    data = fut.result()
                except Exception as e:
                    data = {"error": str(e), "weeklyHigh": 0, "weeklyLow": 0}
                self._apply(sym, data)
        self._last_sweep_ts = time.time()
        self._swept_once = True

    def _update_stock(self, symbol, data):
        stock = self.stocks[symbol]
        stock.company_name = data.get("companyName", symbol)
        stock.current_price = data.get("lastPrice", 0)
        stock.prev_close = data.get("previousClose", 0)
        stock.day_high = data.get("dayHigh", 0)
        stock.day_low = data.get("dayLow", 0)
        stock.change_percent = data.get("pChange", 0)
        stock.data_source = data.get("source", "")
        stock.fetch_error = ""
        stock.weekly_high = data.get("weeklyHigh", 0)
        stock.weekly_low = data.get("weeklyLow", 0)
        stock.weekly_open = data.get("weeklyOpen", 0)
        stock.weekly_close = data.get("weeklyClose", 0)
        if stock.weekly_high > stock.weekly_low:
            stock.fib_levels = self.fib_calc.calculate(stock.weekly_high, stock.weekly_low)
        all_candles = data.get("hourly_candles", [])
        try:
            ws = dt.datetime.strptime(stock.current_week_start, "%Y-%m-%d").date()
        except Exception:
            ws = None
        week_candles = []
        if ws:
            for c in all_candles:
                try:
                    cd = dt.datetime.strptime(c.timestamp[:10], "%Y-%m-%d").date()
                except Exception:
                    continue
                if cd >= ws:
                    week_candles.append(c)
        else:
            week_candles = all_candles
        week_candles.sort(key=lambda c: c.timestamp)
        closed_candles = self._only_closed(week_candles)
        stock.hourly_candles = week_candles if week_candles else all_candles[-3:]
        stock.active_trade = None
        stock.pending_signal = None
        stock.last_signal = None
        stock.closed_trades = []
        stock.last_closed_trade = None
        stock.signalled_levels = set()
        stock.signals_this_week = 0
        stock.prev_candle = None
        stock.last_candle = None
        for candle in closed_candles:
            self._process_candle(symbol, candle)
        if closed_candles:
            stock.last_candle = closed_candles[-1]
            stock.prev_candle = closed_candles[-2] if len(closed_candles) >= 2 else None
        elif week_candles:
            stock.last_candle = week_candles[-1]
        stock.last_updated = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _only_closed(self, candles):
        if not candles:
            return []
        now = dt.datetime.now(IST_FIXED)
        out = []
        for c in candles:
            try:
                ct = dt.datetime.strptime(c.timestamp, "%Y-%m-%d %H:%M").replace(tzinfo=IST_FIXED)
            except Exception:
                continue
            close_at = ct.replace(hour=SWING_MARKET_CLOSE.hour, minute=SWING_MARKET_CLOSE.minute,
                                  second=0, microsecond=0)
            if now >= close_at:
                out.append(c)
        return out

    def _process_candle(self, symbol, candle):
        stock = self.stocks[symbol]
        if stock.pending_signal and not stock.active_trade:
            self._execute_trade(symbol, stock.pending_signal, candle)
            stock.pending_signal = None
        exited_here = False
        if stock.active_trade:
            before = stock.active_trade
            self._check_status(symbol, candle)
            exited_here = (stock.active_trade is None and before is not None)
        if not stock.active_trade and not stock.pending_signal and not exited_here:
            self._check_signals(symbol, candle)
        stock.prev_candle = stock.last_candle
        stock.last_candle = candle

    def _check_signals(self, symbol, candle):
        stock = self.stocks[symbol]
        if not stock.fib_levels:
            return
        best = None
        for level_str, level_price in stock.fib_levels.items():
            level = float(level_str)
            if level in stock.signalled_levels:
                continue
            direction = None
            if candle.is_green and candle.open_price <= level_price < candle.close_price:
                direction = "BUY"
            elif candle.is_red and candle.close_price < level_price <= candle.open_price:
                direction = "SELL"
            if direction:
                dist = abs(level_price - candle.open_price)
                if best is None or dist > best[0]:
                    best = (dist, level, level_price, direction)
        if best is None:
            return
        _, level, level_price, direction = best
        stock.last_signal = SwSignal(direction=direction, fib_level=level,
            fib_price=level_price, timestamp=candle.timestamp,
            candle_close=candle.close_price, candle_open=candle.open_price,
            week_start_date=stock.current_week_start)
        stock.signalled_levels.add(level)
        stock.signals_this_week += 1
        stock.pending_signal = stock.last_signal

    def _execute_trade(self, symbol, signal, entry_candle):
        stock = self.stocks[symbol]
        if stock.active_trade:
            return
        self.trade_counter += 1
        levels = stock.fib_levels
        sl_price = self.fib_calc.get_previous_level(levels, signal.fib_price, signal.direction)
        if sl_price is None:
            sl_price = signal.fib_price * (0.985 if signal.direction == "BUY" else 1.015)
        entry = entry_candle.open_price
        risk = abs(entry - sl_price)
        tp_price = entry + risk if signal.direction == "BUY" else entry - risk
        trade = SwTrade(
            trade_id=f"{symbol}_{entry_candle.timestamp.replace('-','').replace(' ','_').replace(':','')}",
            symbol=symbol, direction=signal.direction, entry_price=round(entry, 2),
            sl_price=round(sl_price, 2), tp_price=round(tp_price, 2),
            fib_level=signal.fib_level, fib_price=signal.fib_price, status="OPEN",
            open_time=entry_candle.timestamp + ":00" if len(entry_candle.timestamp) == 16 else entry_candle.timestamp,
            week_start_date=stock.current_week_start)
        stock.active_trade = trade

    def _check_status(self, symbol, candle):
        stock = self.stocks[symbol]
        trade = stock.active_trade
        if not trade or trade.status != "OPEN":
            return
        high, low = candle.high_price, candle.low_price
        if trade.direction == "BUY":
            if low <= trade.sl_price:
                self._close_trade(symbol, trade.sl_price, "CLOSED_SL", candle.timestamp)
            elif high >= trade.tp_price:
                self._close_trade(symbol, trade.tp_price, "CLOSED_TP", candle.timestamp)
        else:
            if high >= trade.sl_price:
                self._close_trade(symbol, trade.sl_price, "CLOSED_SL", candle.timestamp)
            elif low <= trade.tp_price:
                self._close_trade(symbol, trade.tp_price, "CLOSED_TP", candle.timestamp)

    def _close_trade(self, symbol, close_price, status, close_ts=""):
        stock = self.stocks[symbol]
        trade = stock.active_trade
        if not trade:
            return
        trade.close_price = round(close_price, 2)
        trade.status = status
        trade.close_time = (close_ts + ":00" if close_ts and len(close_ts) == 16
                            else close_ts or dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if trade.direction == "BUY":
            trade.pnl = round(close_price - trade.entry_price, 2)
        else:
            trade.pnl = round(trade.entry_price - close_price, 2)
        trade.pnl_percent = round((trade.pnl / trade.entry_price) * 100, 2)
        stock.closed_trades.append(trade)
        stock.last_closed_trade = trade
        stock.total_pnl = round(sum(t.pnl or 0 for t in stock.closed_trades), 2)
        if not any(t.trade_id == trade.trade_id for t in stock.all_trades_history):
            stock.all_trades_history.append(trade)
        stock.active_trade = None

    def get_dashboard_data(self):
        total_pnl = sum(s.total_pnl for s in self.stocks.values())
        total_signals = sum(s.signals_this_week for s in self.stocks.values())
        active_trades = len([s for s in self.stocks.values() if s.active_trade])
        total_closed = sum(len(s.closed_trades) for s in self.stocks.values())
        real_count = len([s for s in self.stocks.values()
                          if s.data_source == "yahoo_finance" and s.weekly_high > 0])
        pending_trades = []
        for symbol, stock in self.stocks.items():
            if stock.active_trade:
                pending_trades.append({
                    "symbol": symbol, "direction": stock.active_trade.direction,
                    "entry": stock.active_trade.entry_price, "sl": stock.active_trade.sl_price,
                    "tp": stock.active_trade.tp_price, "fib": stock.active_trade.fib_level,
                    "open_time": stock.active_trade.open_time})
        return {
            "stocks": {s: self.stocks[s].to_dict() for s in SWING_STOCKS.keys()},
            "summary": {"total_stocks": len(SWING_STOCKS), "total_signals": total_signals,
                        "active_trades": active_trades, "pending_trades": pending_trades,
                        "total_closed": total_closed, "total_pnl": round(total_pnl, 2),
                        "real_data_count": real_count, "current_week": self._cur_week(),
                        "last_update": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}

    def loop(self):
        while True:
            try:
                self.update_all()
            except Exception as e:
                print(f"[swing] loop error: {e}")
            time.sleep(SWING_REFRESH)


SWING = SwingEngine()


# =============================================================================
# =============================================================================
#   ENGINE 3 : INVESTMENT  (weekly investment-level scanner, yfinance)
#   -- core logic identical to your investment scanner
# =============================================================================
# =============================================================================

INV_FIB = [-1.618, -1.118, -0.618, -0.202, 0.214, 0.5, 0.786,
           1.202, 1.618, 2.118, 2.618, 3.118, 3.618, 4.236]
INV_STOCKS = [f"{s}.NS" for s in INTRA_SYMBOLS]


class InvestmentEngine:
    def __init__(self):
        self.data: List[dict] = []
        self.lock = threading.Lock()
        self.done = False
        self.total = 0
        self.progress = 0

    def _calc_levels(self, high, low):
        r = high - low
        return {ratio: round(low + r * ratio, 2) for ratio in INV_FIB}

    def _all_levels(self, high, low):
        return sorted(self._calc_levels(high, low).values())

    def _current_level(self, levels, ltp):
        below = [l for l in levels if l <= ltp]
        return max(below) if below else levels[0]

    def _upcoming_level(self, levels, ltp):
        above = [l for l in levels if l > ltp]
        return min(above) if above else levels[-1]

    def _next5(self, levels, ltp):
        return [l for l in levels if l > ltp][:5]

    def _fetch(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            d = stock.history(period="2y", interval="1wk")
            if d.empty:
                return None
            y2025 = d[d.index.year == 2025]
            if y2025.empty:
                y2025 = d
            closes = d["Close"].dropna()
            price = round(float(closes.iloc[-1]), 2)

            # ---- TREND (Bullish / Bearish / Neutral) ----
            # Two signals combined:
            #   1) price vs its own moving averages (fast 10w, slow 30w)
            #   2) recent momentum (last close vs 12 weeks ago)
            trend = "NEUTRAL"
            try:
                ma_fast = float(closes.tail(10).mean())
                ma_slow = float(closes.tail(30).mean()) if len(closes) >= 30 else ma_fast
                mom_ref = float(closes.iloc[-13]) if len(closes) >= 13 else float(closes.iloc[0])
                mom_pct = ((price - mom_ref) / mom_ref) * 100 if mom_ref else 0.0
                bull = (price > ma_fast) and (ma_fast >= ma_slow) and (mom_pct > 2)
                bear = (price < ma_fast) and (ma_fast <= ma_slow) and (mom_pct < -2)
                if bull:
                    trend = "BULLISH"
                elif bear:
                    trend = "BEARISH"
                trend_pct = round(mom_pct, 2)
            except Exception:
                trend_pct = 0.0

            return {"name": ticker.replace(".NS", ""),
                    "price": price,
                    "high": round(float(y2025["High"].max()), 2),
                    "low": round(float(y2025["Low"].min()), 2),
                    "trend": trend, "trend_pct": trend_pct}
        except Exception:
            return None

    def _analyze(self, ticker):
        d = self._fetch(ticker)
        if not d:
            return None
        levels = self._all_levels(d["high"], d["low"])
        current = self._current_level(levels, d["price"])
        upcoming = self._upcoming_level(levels, d["price"])
        next5 = self._next5(levels, d["price"])
        diff_current = abs(d["price"] - current)
        return {"name": d["name"], "price": d["price"], "current": current,
                "upcoming": upcoming, "next5": next5, "diff": round(diff_current, 2),
                "show_current": diff_current <= 15,
                "trend": d.get("trend", "NEUTRAL"), "trend_pct": d.get("trend_pct", 0.0)}

    def scan(self):
        live = [t for t in INV_STOCKS if t.replace(".NS", "") not in DEAD_TICKERS]
        self.total = len(live)
        self.progress = 0
        with self.lock:
            self.data = []
        done = 0
        # parallel fetch -> 500 stocks in ~1-2 min instead of ~25 min
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(self._analyze, t): t for t in live}
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                except Exception:
                    r = None
                done += 1
                self.progress = done
                if r:
                    with self.lock:
                        self.data.append(r)
        # stable alphabetical order so cards don't jump around
        with self.lock:
            self.data.sort(key=lambda x: x["name"])
        self.done = True

    def get_dashboard_data(self):
        with self.lock:
            data = list(self.data)
        return {"stocks": data, "progress": self.progress, "total": self.total,
                "done": self.done,
                "pct": int((self.progress / self.total) * 100) if self.total else 0}

    def update_all(self):
        """Called by the shared master loop. Investment levels barely move
        intraday, so we do a full scan only every 30 min; other rounds return
        instantly so Intraday/Swing aren't starved waiting on 500 weekly
        fetches every cycle."""
        now = time.time()
        last = getattr(self, "_last_scan_ts", 0)
        if self.done and (now - last) < 1800:
            return   # fresh enough, skip
        self.done = False
        self.scan()
        self._last_scan_ts = time.time()

    def loop(self):
        while True:
            try:
                self.update_all()
            except Exception as e:
                print(f"[investment] loop error: {e}")
            time.sleep(60)


INVESTMENT = InvestmentEngine()


# =============================================================================
# =============================================================================
#   ENGINE 4 : OPTIONS  (Dhan multi-index bot + backtest, Dhan API)
#   -- core logic identical to your Dhan bot
# =============================================================================
# =============================================================================

opt_log_queue = queue.Queue()
bt_events = queue.Queue()
options_bot_running = False
backtest_running = False

INDEX_CONFIG = {
    "NIFTY": {"security_id": "13", "spot_exchange": "IDX_I", "opt_exchange": "NSE_FNO",
              "strike_interval": 50, "zone_range": 500, "levels": [120, 150],
              "labels": ["CALL_120", "CALL_150", "PUT_120", "PUT_150"],
              "sl_pct": 0.10, "tp_pct": 0.10, "min_body_pct": 70.0},
    "BANKNIFTY": {"security_id": "25", "spot_exchange": "IDX_I", "opt_exchange": "NSE_FNO",
                  "strike_interval": 100, "zone_range": 2000, "levels": [450, 550],
                  "labels": ["CALL_450", "CALL_550", "PUT_450", "PUT_550"],
                  "sl_pct": 0.10, "tp_pct": 0.10, "min_body_pct": 70.0},
    "SENSEX": {"security_id": "51", "spot_exchange": "IDX_I", "opt_exchange": "BSE_FNO",
               "strike_interval": 100, "zone_range": 2000, "levels": [400, 500],
               "labels": ["CALL_400", "CALL_500", "PUT_400", "PUT_500"],
               "sl_pct": 0.10, "tp_pct": 0.10, "min_body_pct": 70.0},
}
FIBO_RATIOS = [-1.618, -1.118, -0.618, -0.202, 0.214, 0.500, 0.786,
               1.202, 1.618, 2.118, 2.618, 3.118, 3.618, 4.236]
SEC_COL = "SEM_SMST_SECURITY_ID"

MARKET_OPEN_MIN = 9 * 60 + 15
MARKET_CLOSE_MIN = 15 * 60 + 30
SCAN_START_TIME = (9, 21, 59)

NSE_HOLIDAYS = {
    "2026-01-26": "Republic Day", "2026-03-03": "Holi", "2026-03-21": "Id-Ul-Fitr",
    "2026-03-26": "Ram Navami", "2026-03-31": "Mahavir Jayanti", "2026-04-03": "Good Friday",
    "2026-04-14": "Ambedkar Jayanti", "2026-05-01": "Maharashtra Day", "2026-05-27": "Bakri Id",
    "2026-06-26": "Muharram", "2026-08-15": "Independence Day", "2026-08-26": "Ganesh Chaturthi",
    "2026-10-02": "Gandhi Jayanti", "2026-10-20": "Dussehra", "2026-11-09": "Diwali Laxmi Pujan",
    "2026-11-10": "Diwali Balipratipada", "2026-11-24": "Guru Nanak Jayanti", "2026-12-25": "Christmas",
}


def dash_log(msg, log_type="i", index=None, card=None, bot_status=None):
    ts = dt.datetime.now(IST).strftime("%H:%M:%S")
    print(f"[opt {ts}] {msg}")
    try:
        os.makedirs("logs", exist_ok=True)
        today = dt.datetime.now(IST).strftime("%Y-%m-%d")
        with open(os.path.join("logs", f"trades_{today}.jsonl"), "a") as f:
            f.write(json.dumps({"ts": ts, "msg": msg, "type": log_type}) + "\n")
    except Exception:
        pass
    payload = {"log": msg, "log_type": log_type}
    if index and card:
        payload["index"] = index; payload["card"] = card
    if bot_status:
        payload["bot_status"] = bot_status
    try:
        opt_log_queue.put(payload, timeout=1)
    except Exception:
        pass


def is_market_holiday(d):
    if d.weekday() == 5:
        return True, "Saturday"
    if d.weekday() == 6:
        return True, "Sunday"
    key = d.strftime("%Y-%m-%d")
    if key in NSE_HOLIDAYS:
        return True, NSE_HOLIDAYS[key]
    return False, ""


def verify_token(headers):
    dash_log("=" * 60, "i")
    dash_log("STEP 1: VERIFYING TOKEN", "i")
    try:
        r = requests.get("https://api.dhan.co/v2/fundlimit", headers=headers, timeout=10)
        funds = r.json()
        if r.status_code == 200 and isinstance(funds, dict) and (
                "availabelBalance" in funds or "availableBalance" in funds):
            bal = funds.get("availabelBalance", funds.get("availableBalance", 0))
            dash_log("TOKEN CONNECTED! Balance: Rs." + str(bal), "s")
            return True
        dash_log("Token error: " + str(funds), "e")
        return False
    except Exception as e:
        dash_log("Token verify failed: " + str(e), "e")
        return False


WARMUP_MIN = 8 * 60 + 30   # 08:30 -> prep work starts before open

# shared status the dashboard can read
options_status = {"phase": "IDLE", "detail": "", "next_session": ""}


def _market_phase(now):
    """Return one of: HOLIDAY, PRE (before 8:30), WARMUP (8:30-9:15),
    OPEN (9:15-15:30), CLOSED (after 15:30). Weekend/holiday -> HOLIDAY."""
    holiday, reason = is_market_holiday(now)
    if holiday:
        return "HOLIDAY", reason
    cur = now.hour * 60 + now.minute
    if cur < WARMUP_MIN:
        return "PRE", ""
    if cur < MARKET_OPEN_MIN:
        return "WARMUP", ""
    if cur < MARKET_CLOSE_MIN:
        return "OPEN", ""
    return "CLOSED", ""


def _next_session_str(now):
    nxt = now
    # if already past close today, roll to next day
    if now.hour * 60 + now.minute >= MARKET_CLOSE_MIN:
        nxt = now + dt.timedelta(days=1)
    while is_market_holiday(nxt)[0]:
        nxt += dt.timedelta(days=1)
    return nxt.strftime("%d-%b-%Y (%a)") + " 09:15"


def wait_until_open(headers):
    """Silent-background wait until market opens. Logs sparingly so the
    terminal doesn't flood on weekends/nights. Returns True when OPEN,
    False if the bot was stopped."""
    last_note = None
    while options_bot_running:
        now = dt.datetime.now(IST)
        phase, reason = _market_phase(now)
        options_status["phase"] = phase
        options_status["next_session"] = _next_session_str(now)
        if phase == "OPEN":
            dash_log("Market OPEN - scanning start", "s")
            return True
        # sparse, de-duplicated status logs (once per state change)
        if phase == "HOLIDAY":
            note = "Market band (" + reason + "). Next: " + options_status["next_session"]
            options_status["detail"] = note
            if note != last_note:
                dash_log(note + " | background silent mode", "w"); last_note = note
            time.sleep(600)
        elif phase == "PRE":
            note = "Before 8:30 - silent. Session: " + options_status["next_session"]
            options_status["detail"] = note
            if note != last_note:
                dash_log(note, "i"); last_note = note
            time.sleep(300)
        elif phase == "WARMUP":
            note = "Warm-up (8:30-9:15) - prepping for open"
            options_status["detail"] = note
            if note != last_note:
                dash_log(note, "i"); last_note = note
            time.sleep(20)
        elif phase == "CLOSED":
            note = "Market closed for today. Next: " + options_status["next_session"]
            options_status["detail"] = note
            if note != last_note:
                dash_log(note + " | background silent mode", "w"); last_note = note
            time.sleep(600)
    return False


def wait_for_scan_start():
    dash_log("SCAN LOCK: 9:21:59 tak koi trade nahi", "w")
    logged = set()
    while options_bot_running:
        now = dt.datetime.now(IST)
        if (now.hour, now.minute, now.second) >= SCAN_START_TIME:
            dash_log("9:21:59 - SCANNING UNLOCKED!", "s")
            return True
        rem = (SCAN_START_TIME[0] * 3600 + SCAN_START_TIME[1] * 60 + SCAN_START_TIME[2]) \
              - (now.hour * 3600 + now.minute * 60 + now.second)
        if rem > 0 and rem % 60 == 0 and rem not in logged:
            logged.add(rem)
            dash_log("Scan start tak " + str(rem // 60) + "m " + str(rem % 60) + "s...", "w")
        time.sleep(0.3)
    return False


def get_last_trading_day(config, headers):
    today = pd.Timestamp.today().normalize()
    for i in range(1, 7):
        d = today - pd.Timedelta(days=i)
        if d.weekday() >= 5:
            continue
        ds = d.strftime("%Y-%m-%d")
        try:
            r = requests.post("https://api.dhan.co/v2/charts/intraday", headers=headers,
                json={"securityId": config["security_id"], "exchangeSegment": config["spot_exchange"],
                      "instrument": "INDEX", "expiryCode": 0, "fromDate": ds, "toDate": ds,
                      "interval": "15", "oi": False}, timeout=10)
            data = r.json()
            if data.get("close") and len(data.get("close")) > 0:
                return ds
        except Exception:
            pass
    return today.strftime("%Y-%m-%d")


def fetch_scrip_master():
    dash_log("Scrip master download...", "i")
    for attempt in range(3):
        try:
            df = pd.read_csv("https://images.dhan.co/api-data/api-scrip-master.csv", low_memory=False)
            df.columns = df.columns.str.strip()
            if "SEM_SMST_SECURITY_ID" not in df.columns and "SEM_SECURITY_ID" in df.columns:
                df["SEM_SMST_SECURITY_ID"] = df["SEM_SECURITY_ID"]
            dash_log("Scrip master ready!", "s")
            return df
        except Exception as e:
            dash_log("Attempt " + str(attempt + 1) + ": " + str(e), "w")
            time.sleep(5)
    return None


def fetch_spot(index_name, config, headers):
    for _ in range(5):
        time.sleep(2)
        try:
            seg = config["spot_exchange"]; sec_id = int(config["security_id"])
            r = requests.post("https://api.dhan.co/v2/marketfeed/ltp", headers=headers,
                json={seg: [sec_id]}, timeout=10)
            data = r.json()
            if "data" in data and seg in data["data"]:
                sd = data["data"][seg]
                if str(sec_id) in sd:
                    spot = sd[str(sec_id)]["last_price"]
                elif sec_id in sd:
                    spot = sd[sec_id]["last_price"]
                dash_log(index_name + " Spot: Rs." + str(spot), "s")
                return float(spot)
        except Exception:
            pass
    fallbacks = {"NIFTY": 24000, "BANKNIFTY": 50000, "SENSEX": 80000}
    spot = fallbacks.get(index_name, 24000)
    dash_log(index_name + " Spot fallback: Rs." + str(spot), "w")
    return float(spot)


def filter_options(df, index_name, config, spot):
    interval = config["strike_interval"]; zone = config["zone_range"]
    atm = round(spot / interval) * interval
    lower = atm - zone; upper = atm + zone
    opt = df[(df["SEM_INSTRUMENT_NAME"] == "OPTIDX") &
             (df["SEM_TRADING_SYMBOL"].str.contains(index_name, case=False, na=False))].copy()
    if len(opt) == 0:
        return None, None
    opt["EXPIRY_DT"] = pd.to_datetime(opt["SEM_EXPIRY_DATE"], errors="coerce")
    nearest = opt["EXPIRY_DT"].min()
    opt = opt[opt["EXPIRY_DT"] == nearest].copy()
    opt["STRIKE"] = pd.to_numeric(opt["SEM_STRIKE_PRICE"], errors="coerce")
    opt = opt[opt["STRIKE"].between(lower, upper)]
    return opt, nearest


def get_ltp_batch(ids, opt_exchange, headers):
    rows = []
    for i in range(0, len(ids), 20):
        batch = ids[i:i + 20]
        time.sleep(1)
        try:
            r = requests.post("https://api.dhan.co/v2/marketfeed/ohlc", headers=headers,
                json={opt_exchange: batch}, timeout=10)
            data = r.json()
            if "data" in data and opt_exchange in data["data"]:
                for sec_id, val in data["data"][opt_exchange].items():
                    ltp = val.get("last_price", 0)
                    if ltp <= 0 and "ohlc" in val:
                        ltp = val["ohlc"].get("close", 0)
                    rows.append({SEC_COL: int(sec_id), "LTP": float(ltp)})
        except Exception:
            pass
    return pd.DataFrame(rows)


def add_ltp(x, opt_exchange, headers):
    ids = x[SEC_COL].astype(int).tolist()
    qdf = get_ltp_batch(ids, opt_exchange, headers)
    return x.merge(qdf, on=SEC_COL, how="left").fillna({"LTP": 0})


def find_above(df, level):
    x = df[df["LTP"] > level].sort_values("LTP")
    return x.iloc[0] if len(x) > 0 else None


def fetch_day_hl(sec_id, target_date, opt_exchange, headers):
    try:
        r = requests.post("https://api.dhan.co/v2/charts/intraday", headers=headers,
            json={"securityId": str(sec_id), "exchangeSegment": opt_exchange,
                  "instrument": "OPTIDX", "expiryCode": 0,
                  "fromDate": target_date, "toDate": target_date,
                  "interval": "15", "oi": False}, timeout=10)
        data = r.json()
        highs, lows = data.get("high", []), data.get("low", [])
        if highs and lows:
            return {"HIGH": max(highs), "LOW": min(lows)}
    except Exception as e:
        dash_log("H/L error: " + str(e), "w")
    return {"HIGH": None, "LOW": None}


def select_strikes_for_index(df, index_name, config, target_date, headers):
    dash_log("PROCESSING: " + index_name, "i")
    spot = fetch_spot(index_name, config, headers)
    opt, nearest = filter_options(df, index_name, config, spot)
    if opt is None or len(opt) == 0:
        dash_log("No options for " + index_name, "e")
        return []
    levels, labels, opt_exchange = config["levels"], config["labels"], config["opt_exchange"]
    calls = add_ltp(opt[opt["SEM_OPTION_TYPE"] == "CE"].copy(), opt_exchange, headers)
    puts = add_ltp(opt[opt["SEM_OPTION_TYPE"] == "PE"].copy(), opt_exchange, headers)
    targets = [(labels[0], find_above(calls, levels[0]), "CE"),
               (labels[1], find_above(calls, levels[1]), "CE"),
               (labels[2], find_above(puts, levels[0]), "PE"),
               (labels[3], find_above(puts, levels[1]), "PE")]
    selected = []
    for label, row, opt_type in targets:
        if row is None:
            dash_log(label + " - nahi mila!", "w"); continue
        strike = int(row["STRIKE"]); sec_id = int(row[SEC_COL]); ltp = round(float(row["LTP"]), 2)
        hl = fetch_day_hl(sec_id, target_date, opt_exchange, headers)
        selected.append({"index": index_name, "label": label, "security_id": str(sec_id),
            "strike": float(strike), "option_type": opt_type, "ltp": ltp,
            "day_high": hl["HIGH"], "day_low": hl["LOW"], "expiry": str(nearest.date()),
            "opt_exchange": opt_exchange, "sl_pct": config["sl_pct"],
            "tp_pct": config["tp_pct"], "min_body_pct": config["min_body_pct"]})
    dash_log(index_name + ": " + str(len(selected)) + " strikes selected!", "s")
    return selected


def create_fib_levels(all_strikes):
    all_levels = {}
    for s in all_strikes:
        sec_id = str(s["security_id"]); high, low = s["day_high"], s["day_low"]
        if high is None or low is None or high <= low:
            dash_log(s['label'] + ": Invalid H/L, skip", "w"); continue
        rng = high - low
        fibs = sorted(set(round(low + (rng * r), 2) for r in FIBO_RATIOS))
        all_levels[sec_id] = {"index": s["index"], "label": s["label"], "strike": s["strike"],
            "type": s["option_type"], "levels": fibs, "opt_exchange": s["opt_exchange"],
            "sl_pct": s["sl_pct"], "tp_pct": s["tp_pct"], "min_body_pct": s["min_body_pct"]}
    return all_levels


def get_latest_candle(sec_id, opt_exchange, headers):
    today = dt.datetime.now(IST).strftime("%Y-%m-%d")
    try:
        r = requests.post("https://api.dhan.co/v2/charts/intraday", headers=headers,
            json={"securityId": sec_id, "exchangeSegment": opt_exchange,
                  "instrument": "OPTIDX", "expiryCode": 0, "fromDate": today,
                  "toDate": today, "interval": "1", "oi": False}, timeout=10)
        data = r.json()
        opens, highs, lows, closes, times = (data.get("open", []), data.get("high", []),
            data.get("low", []), data.get("close", []), data.get("timestamp", []))
        min_len = min(len(opens), len(highs), len(lows), len(closes), len(times))
        if min_len < 2:
            return None
        i = min_len - 2
        return {"time": times[i], "open": float(opens[i]), "high": float(highs[i]),
                "low": float(lows[i]), "close": float(closes[i])}
    except Exception:
        return None


def get_ltp(sec_id, opt_exchange, headers):
    try:
        r = requests.post("https://api.dhan.co/v2/marketfeed/ltp", headers=headers,
            json={opt_exchange: [int(sec_id)]}, timeout=10)
        data = r.json()
        if "data" in data and opt_exchange in data["data"]:
            sd = data["data"][opt_exchange]
            if str(sec_id) in sd:
                return float(sd[str(sec_id)]["last_price"])
            elif int(sec_id) in sd:
                return float(sd[int(sec_id)]["last_price"])
    except Exception:
        pass
    return None


def valid_body(candle, min_body_pct):
    body = abs(candle["close"] - candle["open"]); total = candle["high"] - candle["low"]
    if total <= 0:
        return False
    body_pct = (body / total) * 100
    return body_pct >= min_body_pct and (100 - body_pct) <= 30


def breakout_level(candle, fibs, min_body_pct):
    o, c = candle["open"], candle["close"]
    if c <= o:
        return None
    for lvl in fibs:
        if o < lvl and c > lvl:
            dash_log("LEVEL TOUCHED: " + str(lvl), "tr")
            if valid_body(candle, min_body_pct):
                dash_log("VALID BREAKOUT", "s")
                return lvl
            dash_log("BODY FILTER FAILED", "w")
    return None


# ---- backtest ----
def prev_trading_day(date_str):
    d = dt.datetime.strptime(date_str, "%Y-%m-%d") - dt.timedelta(days=1)
    while is_market_holiday(d)[0]:
        d -= dt.timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def bt_candles(sec_id, opt_exchange, date_str, headers, interval="1", instrument="OPTIDX"):
    try:
        r = requests.post("https://api.dhan.co/v2/charts/intraday", headers=headers,
            json={"securityId": str(sec_id), "exchangeSegment": opt_exchange,
                  "instrument": instrument, "expiryCode": 0, "fromDate": date_str,
                  "toDate": date_str, "interval": interval, "oi": False}, timeout=15)
        d = r.json()
        o, h, l, c, t = (d.get("open", []), d.get("high", []), d.get("low", []),
                         d.get("close", []), d.get("timestamp", []))
        n = min(len(o), len(h), len(l), len(c), len(t))
        return [{"time": t[i], "open": float(o[i]), "high": float(h[i]),
                 "low": float(l[i]), "close": float(c[i])} for i in range(n)]
    except Exception as e:
        bt_log("Candle error " + str(sec_id) + ": " + str(e), "w")
        return []


def ts_to_hhmm(ts):
    try:
        return dt.datetime.fromtimestamp(int(ts), IST).strftime("%H:%M")
    except Exception:
        return "--:--"


def bt_valid_body(c, min_body_pct):
    body = abs(c["close"] - c["open"]); total = c["high"] - c["low"]
    if total <= 0:
        return False
    bp = (body / total) * 100
    return bp >= min_body_pct and (100 - bp) <= 30


def bt_breakout(c, fibs, min_body_pct):
    o, cl = c["open"], c["close"]
    if cl <= o:
        return None
    for lvl in fibs:
        if o < lvl and cl > lvl:
            if bt_valid_body(c, min_body_pct):
                return lvl
    return None


def bt_log(msg, t="i"):
    bt_events.put({"log": msg, "log_type": t})


def run_backtest(date_str):
    global backtest_running
    backtest_running = True
    trades = []
    try:
        headers = {"client-id": bot_credentials["client_id"],
                   "access-token": bot_credentials["access_token"],
                   "Content-Type": "application/json"}
        hol, reason = is_market_holiday(dt.datetime.strptime(date_str, "%Y-%m-%d"))
        if hol:
            bt_log(date_str + " par market band tha (" + reason + ")", "e")
            bt_events.put({"done": True, "trades": [], "date": date_str})
            backtest_running = False
            return
        prev = prev_trading_day(date_str)
        bt_log("Backtest date: " + date_str + " | Prev day (H/L): " + prev, "i")
        df = fetch_scrip_master()
        if df is None:
            bt_log("Scrip master fail", "e")
            bt_events.put({"done": True, "trades": [], "date": date_str})
            backtest_running = False
            return
        for index_name in ["NIFTY", "BANKNIFTY", "SENSEX"]:
            if not backtest_running:
                break
            cfg = INDEX_CONFIG[index_name]
            bt_log("-" * 45, "i")
            bt_log(index_name, "tr")
            spot_c = bt_candles(cfg["security_id"], cfg["spot_exchange"], date_str, headers, "15", "INDEX")
            if not spot_c:
                bt_log(index_name + ": us din ka data nahi", "w"); continue
            spot = spot_c[0]["open"]
            opt, nearest = filter_options(df, index_name, cfg, spot)
            if opt is None or len(opt) == 0:
                bt_log(index_name + ": options nahi mile", "w"); continue
            oe = cfg["opt_exchange"]
            calls = add_ltp(opt[opt["SEM_OPTION_TYPE"] == "CE"].copy(), oe, headers)
            puts = add_ltp(opt[opt["SEM_OPTION_TYPE"] == "PE"].copy(), oe, headers)
            L, LB = cfg["levels"], cfg["labels"]
            targets = [(LB[0], find_above(calls, L[0]), "CE"),
                       (LB[1], find_above(calls, L[1]), "CE"),
                       (LB[2], find_above(puts, L[0]), "PE"),
                       (LB[3], find_above(puts, L[1]), "PE")]
            index_done = False
            for label, row, otype in targets:
                if index_done or row is None or not backtest_running:
                    continue
                sec_id = int(row[SEC_COL]); strike = int(row["STRIKE"])
                hl = fetch_day_hl(sec_id, prev, oe, headers)
                dh, dl = hl["HIGH"], hl["LOW"]
                if not dh or not dl or dh <= dl:
                    bt_log("   " + label + ": prev H/L invalid", "w"); continue
                rng = dh - dl
                fibs = sorted(set(round(dl + rng * r, 2) for r in FIBO_RATIOS))
                candles = bt_candles(sec_id, oe, date_str, headers, "1")
                if len(candles) < 2:
                    bt_log("   " + label + ": 1-min candles nahi", "w"); continue
                pos = None
                for c in candles:
                    if pos is None:
                        try:
                            cd = dt.datetime.fromtimestamp(int(c["time"]), IST)
                            if (cd.hour, cd.minute, cd.second) < SCAN_START_TIME:
                                continue
                        except Exception:
                            pass
                    if pos is None:
                        lvl = bt_breakout(c, fibs, cfg["min_body_pct"])
                        if lvl:
                            entry = c["close"]
                            pos = {"entry": entry, "lvl": lvl,
                                   "sl": round(entry * (1 - cfg["sl_pct"]), 2),
                                   "tp": round(entry * (1 + cfg["tp_pct"]), 2),
                                   "in_t": ts_to_hhmm(c["time"])}
                    else:
                        hit = None
                        if c["low"] <= pos["sl"]:
                            hit = ("SL", pos["sl"])
                        if c["high"] >= pos["tp"]:
                            hit = ("TARGET", pos["tp"])
                        if hit:
                            pnl = round((hit[1] - pos["entry"]) * 1, 2)
                            trades.append({"index": index_name, "label": label, "strike": strike,
                                "type": otype, "level": pos["lvl"], "entry": round(pos["entry"], 2),
                                "sl": pos["sl"], "tp": pos["tp"], "exit": hit[1], "status": hit[0],
                                "in_time": pos["in_t"], "out_time": ts_to_hhmm(c["time"]), "pnl": pnl})
                            bt_log("   " + ("TGT" if hit[0] == "TARGET" else "SL") + " " + label +
                                   " " + pos["in_t"] + "->" + ts_to_hhmm(c["time"]) + " | Rs." + str(pnl),
                                   "s" if hit[0] == "TARGET" else "e")
                            pos = None; index_done = True; break
                if pos is not None:
                    last = candles[-1]["close"]
                    pnl = round((last - pos["entry"]) * 1, 2)
                    trades.append({"index": index_name, "label": label, "strike": strike,
                        "type": otype, "level": pos["lvl"], "entry": round(pos["entry"], 2),
                        "sl": pos["sl"], "tp": pos["tp"], "exit": round(last, 2), "status": "EOD",
                        "in_time": pos["in_t"], "out_time": "15:30", "pnl": pnl})
                    bt_log("   " + label + " EOD square-off | Rs." + str(pnl), "w")
                    index_done = True
            if not index_done:
                bt_log("   " + index_name + ": koi trade nahi", "w")
        tot = round(sum(t["pnl"] for t in trades), 2)
        wins = len([t for t in trades if t["pnl"] > 0])
        bt_log("BACKTEST DONE | Trades: " + str(len(trades)) + " | Win: " + str(wins) +
               " | Net: Rs." + str(tot), "s" if tot >= 0 else "e")
        bt_events.put({"done": True, "trades": trades, "date": date_str})
    except Exception as e:
        bt_log("Backtest crash: " + str(e), "e")
        bt_events.put({"done": True, "trades": trades, "date": date_str})
    finally:
        backtest_running = False


def options_bot_main():
    """24x7 supervisor. Runs forever once started: waits silently when the
    market is shut, runs one full trading session when it opens, then loops
    to wait for the next session. Never needs a manual RUN."""
    global options_bot_running
    cid = bot_credentials["client_id"]; tok = bot_credentials["access_token"]
    headers = {"client-id": cid, "access-token": tok, "Content-Type": "application/json"}
    dash_log("DHAN MULTI-INDEX BOT STARTED (24x7)", "s")
    dash_log("NIFTY + BANKNIFTY + SENSEX | One Trade Per Index", "i")
    if not verify_token(headers):
        dash_log("Bot stopped: token error.", "e")
        options_bot_running = False
        return

    # outer 24x7 loop -- one iteration == one trading session
    while options_bot_running:
        if not wait_until_open(headers):
            return
        try:
            _run_one_session(headers)
        except Exception as e:
            dash_log("Session error: " + str(e), "e")
        # session done for today -> loop back, wait_until_open will sleep
        # silently until the next trading day's 9:15
        options_status["phase"] = "CLOSED"
        time.sleep(30)


def _run_one_session(headers):
    """One 9:15 -> 15:30 trading session (the original bot body)."""
    target_date = get_last_trading_day(INDEX_CONFIG["NIFTY"], headers)
    dash_log("Last Trading Day: " + target_date, "i")
    df = fetch_scrip_master()
    if df is None:
        dash_log("Scrip master fail. Skipping this session.", "e")
        return
    all_strikes = []
    for index_name in ["NIFTY", "BANKNIFTY", "SENSEX"]:
        if not options_bot_running:
            return
        all_strikes.extend(select_strikes_for_index(df, index_name, INDEX_CONFIG[index_name],
                                                     target_date, headers))
    dash_log("TOTAL STRIKES: " + str(len(all_strikes)), "s")
    all_levels = create_fib_levels(all_strikes)
    if not wait_for_scan_start():
        return
    if not options_bot_running:
        return
    dash_log("Monitoring Started for ALL 3 INDICES", "s")
    options_status["phase"] = "SCANNING"
    options_status["detail"] = "Live scanning NIFTY / BANKNIFTY / SENSEX (9:15-15:30)"
    trade_states = {i: {"taken": False, "data": None, "last_candle_time": None}
                    for i in ["NIFTY", "BANKNIFTY", "SENSEX"]}
    active_indices = ["NIFTY", "BANKNIFTY", "SENSEX"]
    for idx in active_indices:
        dash_log("[" + idx + "] Searching...", "i", index=idx, card={"status": "SEARCHING"})
    while options_bot_running:
        now = dt.datetime.now(IST)
        hhmm = (now.hour, now.minute)
        if hhmm >= (15, 30):
            dash_log("Market Closed (3:30 PM)", "e"); break
        all_done = all(trade_states[idx]["taken"] and trade_states[idx]["data"].get("exited", False)
                       for idx in active_indices)
        if all_done:
            dash_log("All indices completed!", "s"); break
        for index_name in active_indices:
            state = trade_states[index_name]
            if state["taken"] and not state["data"].get("exited", False):
                ltp = get_ltp(state["data"]["sec_id"], state["data"]["opt_exchange"], headers)
                if ltp is None:
                    continue
                entry = state["data"]["entry"]; pnl = (ltp - entry) * 65
                sl, tp = state["data"]["sl"], state["data"]["tp"]
                dash_log("[" + index_name + "] " + state['data']['label'] + " | LTP: " + str(ltp) +
                         " | P&L: Rs." + str(round(pnl, 2)), "tr", index=index_name,
                         card={"status": "ACTIVE", "strike": state["data"]["strike"],
                               "type": state["data"].get("type", ""), "entry": entry, "ltp": ltp,
                               "sl": sl, "tp": tp, "pnl": pnl})
                if ltp >= tp:
                    dash_log("[" + index_name + "] TARGET HIT! Rs." + str(round(pnl, 2)), "s")
                    state["data"]["exited"] = True
                    dash_log("Trade completed", "s", index=index_name,
                        card={"status": "TARGET", "strike": state["data"]["strike"],
                              "type": state["data"].get("type", ""), "entry": entry, "ltp": ltp,
                              "sl": sl, "tp": tp, "pnl": pnl})
                elif ltp <= sl:
                    dash_log("[" + index_name + "] STOPLOSS HIT! Rs." + str(round(pnl, 2)), "e")
                    state["data"]["exited"] = True
                    dash_log("Trade stopped", "e", index=index_name,
                        card={"status": "SL", "strike": state["data"]["strike"],
                              "type": state["data"].get("type", ""), "entry": entry, "ltp": ltp,
                              "sl": sl, "tp": tp, "pnl": pnl})
        for sec_id, info in all_levels.items():
            index_name = info["index"]; state = trade_states[index_name]
            if state["taken"] or hhmm < (9, 22):
                continue
            candle = get_latest_candle(sec_id, info["opt_exchange"], headers)
            if candle is None:
                continue
            try:
                c_dt = dt.datetime.fromtimestamp(int(candle["time"]), IST)
                if (c_dt.hour, c_dt.minute, c_dt.second) < SCAN_START_TIME:
                    continue
            except Exception:
                pass
            level = breakout_level(candle, info["levels"], info["min_body_pct"])
            if level:
                if candle["time"] == state["last_candle_time"]:
                    continue
                state["last_candle_time"] = candle["time"]
                entry = candle["close"]
                sl = round(entry * (1 - info["sl_pct"]), 2)
                tp = round(entry * (1 + info["tp_pct"]), 2)
                dash_log("[" + index_name + "] VALID BREAKOUT FOUND", "tr")
                dash_log("[" + index_name + "] PAPER TRADE EXECUTED", "s")
                state["data"] = {"index": index_name, "sec_id": sec_id, "label": info["label"],
                    "strike": info["strike"], "type": info["type"], "entry": entry, "sl": sl,
                    "tp": tp, "opt_exchange": info["opt_exchange"], "exited": False}
                state["taken"] = True
                dash_log("[" + index_name + "] MONITORING", "s", index=index_name,
                    card={"status": "ACTIVE", "strike": info["strike"], "type": info["type"],
                          "entry": entry, "ltp": entry, "sl": sl, "tp": tp, "pnl": 0})
                break
        time.sleep(5)
    dash_log("Session done (15:30). Waiting for next trading day...", "s",
             bot_status="WAITING")
    # NOTE: do NOT set options_bot_running=False here -- the 24x7 outer loop
    # in options_bot_main() takes over and silently waits for the next open.


# =============================================================================
# =============================================================================
#   FLASK ROUTES
# =============================================================================
# =============================================================================

def _master_scan_loop():
    """One sequential loop that scans the three yfinance engines in order,
    over and over: Intraday -> Swing -> Investment -> repeat.

    The Options bot runs in its OWN thread (started first, 24x7) because it
    must react to 1-min candles in real time; it can't wait behind a full
    stock sweep. The three scanners share this single loop so they never
    fight each other for Yahoo bandwidth (which is what made everything
    crawl before). Each engine pushes results to the site progressively as
    it goes, so cards fill in live instead of appearing all at once.
    """
    time.sleep(1)   # let the Options thread grab the network first
    while True:
        for name, engine in (("intraday", INTRADAY),
                             ("swing", SWING),
                             ("investment", INVESTMENT)):
            try:
                engine.update_all()   # progressive: writes state as it fetches
            except Exception as e:
                print(f"[master] {name} sweep error: {e}")
        time.sleep(3)


def _start_engine_threads():
    """After login: start Options (24x7, own thread) + the shared scan loop."""
    global options_bot_running
    with _start_lock:
        # 1) Options bot auto-starts (no manual RUN button anymore)
        if not _started["options"]:
            options_bot_running = True
            threading.Thread(target=options_bot_main, daemon=True).start()
            _started["options"] = True
        # 2) single sequential scanner loop for the 3 yfinance engines
        if not _started["intraday"]:
            threading.Thread(target=_master_scan_loop, daemon=True).start()
            _started["intraday"] = True
            _started["swing"] = True
            _started["investment"] = True


@app.route('/')
def login_page():
    return render_template_string(LOGIN_HTML)


@app.route('/dashboard')
def dashboard_page():
    if not logged_in.is_set():
        return render_template_string(LOGIN_HTML)
    return render_template_string(DASHBOARD_HTML)


@app.route('/login', methods=['POST'])
def do_login():
    global bot_credentials
    data = request.get_json() or {}
    cid = data.get('client_id', '').strip()
    tok = data.get('access_token', '').strip()
    if not cid or not tok:
        return jsonify({"success": False, "error": "Client ID aur Token dono zaroori hain!"})
    # verify against Dhan
    headers = {"client-id": cid, "access-token": tok, "Content-Type": "application/json"}
    try:
        r = requests.get("https://api.dhan.co/v2/fundlimit", headers=headers, timeout=10)
        funds = r.json()
        ok = r.status_code == 200 and isinstance(funds, dict) and (
            "availabelBalance" in funds or "availableBalance" in funds)
    except Exception as e:
        return jsonify({"success": False, "error": "Dhan connect fail: " + str(e)})
    if not ok:
        return jsonify({"success": False, "error": "Token/ID galat ya expired. " + str(funds)[:120]})
    bot_credentials = {"client_id": cid, "access_token": tok}
    logged_in.set()
    _start_engine_threads()   # start yfinance engines now
    bal = funds.get("availabelBalance", funds.get("availableBalance", 0))
    return jsonify({"success": True, "balance": bal})


# ---- Intraday API ----
@app.route('/api/intraday')
def api_intraday():
    if not logged_in.is_set():
        return jsonify({"error": "not_logged_in"}), 401
    return jsonify(INTRADAY.get_dashboard_data())


# ---- Swing API ----
@app.route('/api/swing')
def api_swing():
    if not logged_in.is_set():
        return jsonify({"error": "not_logged_in"}), 401
    return jsonify(SWING.get_dashboard_data())


# ---- Investment API ----
@app.route('/api/investment')
def api_investment():
    if not logged_in.is_set():
        return jsonify({"error": "not_logged_in"}), 401
    return jsonify(INVESTMENT.get_dashboard_data())


# ---- Options control ----
@app.route('/api/options/start', methods=['POST'])
def api_options_start():
    global options_bot_running
    if not logged_in.is_set():
        return jsonify({"success": False, "error": "Pehle login karo"})
    if options_bot_running:
        return jsonify({"success": False, "error": "Options bot already running!"})
    options_bot_running = True
    threading.Thread(target=options_bot_main, daemon=True).start()
    return jsonify({"success": True})


@app.route('/api/options/stop', methods=['POST'])
def api_options_stop():
    global options_bot_running
    options_bot_running = False
    dash_log("Bot stop requested", "w")
    return jsonify({"success": True})


@app.route('/api/options/stream')
def api_options_stream():
    def gen():
        while True:
            try:
                msg = opt_log_queue.get(timeout=1)
                yield "data: " + json.dumps(msg) + "\n\n"
            except queue.Empty:
                yield "data: " + json.dumps({"keepalive": True}) + "\n\n"
    return Response(gen(), mimetype="text/event-stream")


@app.route('/api/options/backtest', methods=['POST'])
def api_options_backtest():
    global backtest_running
    if not logged_in.is_set():
        return jsonify({"success": False, "error": "Pehle login karo"})
    if backtest_running:
        return jsonify({"success": False, "error": "Backtest already chal raha!"})
    d = (request.get_json() or {}).get("date", "").strip()
    try:
        dt.datetime.strptime(d, "%Y-%m-%d")
    except Exception:
        return jsonify({"success": False, "error": "Date format galat (YYYY-MM-DD)"})
    if dt.datetime.strptime(d, "%Y-%m-%d").date() > dt.datetime.now(IST).date():
        return jsonify({"success": False, "error": "Future date nahi chalegi"})
    while not bt_events.empty():
        bt_events.get_nowait()
    threading.Thread(target=run_backtest, args=(d,), daemon=True).start()
    return jsonify({"success": True})


@app.route('/api/options/backtest_stream')
def api_options_backtest_stream():
    def gen():
        while True:
            try:
                msg = bt_events.get(timeout=1)
                yield "data: " + json.dumps(msg) + "\n\n"
            except queue.Empty:
                yield "data: " + json.dumps({"keepalive": True}) + "\n\n"
    return Response(gen(), mimetype="text/event-stream")


@app.route('/api/options/status')
def api_options_status():
    return jsonify({"running": options_bot_running,
                    "phase": options_status.get("phase", "IDLE"),
                    "detail": options_status.get("detail", ""),
                    "next_session": options_status.get("next_session", "")})


# =============================================================================
# =============================================================================
#   HTML : LOGIN PAGE
# =============================================================================
# =============================================================================

LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Master Bot - Login</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;font-family:'Segoe UI',system-ui,sans-serif}
body{background:#0a0e1a;color:#e2e8f0;display:flex;justify-content:center;align-items:center;min-height:100vh;padding:20px}
.card{background:#111827;border:1px solid #1e293b;border-radius:18px;padding:38px;width:100%;max-width:440px;box-shadow:0 24px 60px rgba(0,0,0,.5)}
.logo{text-align:center;margin-bottom:8px;font-size:26px;font-weight:800;color:#38bdf8}
.tag{text-align:center;color:#64748b;font-size:12px;margin-bottom:26px}
.grp{margin-bottom:18px}
label{display:block;color:#94a3b8;font-size:12px;margin-bottom:7px}
input{width:100%;padding:13px;background:#0a0e1a;border:1px solid #1e293b;border-radius:9px;color:#fff;font-size:14px}
input:focus{outline:none;border-color:#38bdf8}
.btn{width:100%;padding:14px;background:linear-gradient(135deg,#0ea5e9,#0369a1);border:none;border-radius:11px;color:#fff;font-size:15px;font-weight:700;cursor:pointer;margin-top:6px}
.btn:hover{opacity:.92}.btn:disabled{background:#334155;cursor:not-allowed}
.msg{padding:11px;border-radius:8px;margin-bottom:16px;font-size:13px;display:none;line-height:1.5}
.err{background:rgba(239,68,68,.14);color:#fca5a5;border:1px solid rgba(239,68,68,.35)}
.ok{background:rgba(34,197,94,.14);color:#86efac;border:1px solid rgba(34,197,94,.35)}
.help{color:#38bdf8;font-size:11px;cursor:pointer;margin-top:5px;display:inline-block}
.tabs-preview{margin-top:24px;padding:16px;background:rgba(56,189,248,.06);border-radius:10px}
.tabs-preview h3{font-size:12px;color:#38bdf8;margin-bottom:9px}
.tabs-preview ul{color:#94a3b8;font-size:12px;padding-left:18px;line-height:1.9}
.note{font-size:11px;color:#64748b;margin-top:14px;text-align:center;line-height:1.6}
</style>
</head>
<body>
<div class="card">
  <div class="logo">MASTER TRADING BOT</div>
  <div class="tag">4 bots in 1 &middot; Intraday &middot; Swing &middot; Investment &middot; Options</div>
  <div id="err" class="msg err"></div>
  <div id="ok" class="msg ok"></div>
  <div class="grp">
    <label>Dhan Client ID</label>
    <input type="text" id="cid" placeholder="e.g. 1101614171" autocomplete="off">
  </div>
  <div class="grp">
    <label>Dhan Access Token</label>
    <input type="password" id="tok" placeholder="Paste JWT token here">
    <span class="help" onclick="alert('Dhan app -> Settings -> API Access -> Generate Token')">Token kaise lein?</span>
  </div>
  <button class="btn" id="btn" onclick="login()">LOGIN &amp; OPEN DASHBOARD</button>
  <div class="tabs-preview">
    <h3>Dashboard Tabs</h3>
    <ul>
      <li>Intraday &mdash; 1-min Fibonacci breakout scanner (yfinance)</li>
      <li>Swing &mdash; 1M levels / 1D candles (yfinance)</li>
      <li>Investment &mdash; weekly investment levels (yfinance)</li>
      <li>Options &mdash; NIFTY/BANKNIFTY/SENSEX bot + backtest (Dhan)</li>
    </ul>
  </div>
  <div class="note">Login zaroori hai. Options tab Dhan API par chalta hai;<br>baaki 3 tabs yfinance par (free) chalte hain.</div>
</div>
<script>
async function login(){
  var cid=document.getElementById('cid').value.trim();
  var tok=document.getElementById('tok').value.trim();
  var btn=document.getElementById('btn');
  var err=document.getElementById('err'), ok=document.getElementById('ok');
  err.style.display='none'; ok.style.display='none';
  if(!cid||!tok){err.textContent='Client ID aur Token dono daalo!';err.style.display='block';return;}
  btn.disabled=true; btn.textContent='Verifying...';
  try{
    var res=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({client_id:cid,access_token:tok})});
    var d=await res.json();
    if(d.success){
      ok.textContent='Connected! Balance: Rs.'+d.balance+'. Redirecting...';
      ok.style.display='block';
      setTimeout(function(){window.location='/dashboard';},900);
    }else{
      err.textContent=d.error||'Login failed';err.style.display='block';
      btn.disabled=false;btn.textContent='LOGIN & OPEN DASHBOARD';
    }
  }catch(e){
    err.textContent='Error: '+e.message;err.style.display='block';
    btn.disabled=false;btn.textContent='LOGIN & OPEN DASHBOARD';
  }
}
document.getElementById('tok').addEventListener('keypress',function(e){if(e.key==='Enter')login();});
</script>
</body>
</html>"""


# =============================================================================
# =============================================================================
#   HTML : DASHBOARD (4 tabs)
# =============================================================================
# =============================================================================

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Master Trading Bot - Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;font-family:'Segoe UI',system-ui,sans-serif}
body{background:#0a0e1a;color:#e2e8f0;min-height:100vh}
.top{background:linear-gradient(135deg,#0f2744,#0a0e1a);padding:14px 22px;border-bottom:2px solid #38bdf8;
  display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;flex-wrap:wrap;gap:10px}
.top h1{color:#38bdf8;font-size:19px}
.top .sub{font-size:11px;color:#64748b;margin-top:2px}
.clock{font-size:12px;color:#94a3b8}
.tabbar{display:flex;gap:6px;padding:12px 22px 0;flex-wrap:wrap;background:#0a0e1a;position:sticky;top:54px;z-index:99}
.tab{background:#111827;border:1px solid #1e293b;color:#94a3b8;padding:10px 20px;border-radius:10px 10px 0 0;
  font-size:13px;font-weight:600;cursor:pointer;transition:.15s}
.tab:hover{border-color:#38bdf8}
.tab.on{background:#38bdf8;color:#0a0e1a;border-color:#38bdf8}
.panel{display:none;padding:18px 22px 40px}
.panel.on{display:block}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin-bottom:16px}
.stat{background:#111827;border:1px solid #1e293b;border-radius:10px;padding:11px 14px}
.stat .l{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.5px}
.stat .v{font-size:20px;font-weight:700;margin-top:3px}
.c-blue{color:#38bdf8}.c-green{color:#22c55e}.c-red{color:#ef4444}.c-amber{color:#f59e0b}.c-slate{color:#94a3b8}
.filterbar{display:flex;gap:6px;margin-bottom:14px;flex-wrap:wrap;align-items:center}
.fb{background:#111827;border:1px solid #1e293b;color:#94a3b8;padding:6px 13px;border-radius:16px;font-size:12px;cursor:pointer}
.fb.on{background:#38bdf8;color:#0a0e1a;border-color:#38bdf8;font-weight:600}
.search{margin-left:auto;background:#111827;border:1px solid #1e293b;color:#e2e8f0;padding:6px 12px;
  border-radius:16px;font-size:12px;outline:none;min-width:150px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
.card{background:#111827;border:1px solid #1e293b;border-radius:12px;padding:13px;transition:.15s}
.card:hover{border-color:#38bdf8}
.card.locked{border-left:4px solid #38bdf8}.card.tp{border-left:4px solid #22c55e}
.card.sl{border-left:4px solid #ef4444}.card.pending{border-left:4px solid #f59e0b}
.card.buy{border-left:4px solid #22c55e}.card.sell{border-left:4px solid #ef4444}
.ctop{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px}
.sym{font-size:15px;font-weight:700;color:#f1f5f9}
.nm{font-size:10px;color:#64748b}
.px{text-align:right}.px .p{font-size:15px;font-weight:600}.px .ch{font-size:11px}
.badge{display:inline-block;font-size:9px;font-weight:700;padding:3px 8px;border-radius:9px;text-transform:uppercase}
.b-scan{background:#1e293b;color:#94a3b8}.b-pend{background:#78350f;color:#fbbf24}
.b-act{background:#0c4a6e;color:#7dd3fc}.b-tp{background:#14532d;color:#4ade80}
.b-sl{background:#7f1d1d;color:#fca5a5}.b-nd{background:#334155;color:#64748b}
.b-buy{background:#14532d;color:#4ade80}.b-sell{background:#7f1d1d;color:#fca5a5}
.tbox{background:#0a0e1a;border:1px solid #1e293b;border-radius:8px;padding:9px;margin-top:8px}
.trow{display:flex;justify-content:space-between;font-size:12px;padding:2px 0}
.trow .k{color:#64748b}.trow .v{font-weight:600}
.meta{font-size:10px;color:#475569;margin-top:7px;display:flex;justify-content:space-between}
.pend-txt{font-size:11px;color:#fbbf24;margin-top:7px;line-height:1.4}
.scan-txt{font-size:11px;color:#475569;margin-top:7px}
.empty{color:#475569;text-align:center;padding:40px 20px;font-size:13px;line-height:1.8;grid-column:1/-1}
.prog{max-width:600px;margin:0 auto 18px;background:#111827;padding:14px;border-radius:10px;border:1px solid #1e293b}
.prog-t{display:flex;justify-content:space-between;font-size:12px;color:#94a3b8;margin-bottom:7px}
.prog-bar{height:14px;background:#1e293b;border-radius:7px;overflow:hidden}
.prog-fill{height:100%;background:linear-gradient(90deg,#38bdf8,#22c55e);transition:width .5s}
.inv-card{background:#111827;border:1px solid #1e293b;border-radius:12px;margin-bottom:12px;overflow:hidden}
.inv-head{padding:13px 16px;cursor:pointer;display:flex;justify-content:space-between;align-items:center}
.inv-head:hover{background:#0f1a2e}
.inv-title{display:flex;align-items:center;gap:12px}.inv-title h3{font-size:16px;color:#f1f5f9}
.ltp{background:#22c55e;color:#0a0e1a;padding:3px 11px;border-radius:13px;font-size:13px;font-weight:700}
.inv-body{display:none;padding:16px;border-top:1px solid #1e293b}.inv-body.open{display:block}
.boxes{display:grid;grid-template-columns:1fr 1fr;gap:12px}
@media(max-width:640px){.boxes{grid-template-columns:1fr}}
.box{background:#0a0e1a;border-radius:10px;padding:15px;border:2px solid;text-align:center}
.box.cur{border-color:#22c55e}.box.up{border-color:#f59e0b}.box.nx{border-color:#3b82f6}
.box-l{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.box-p{font-size:24px;font-weight:700;color:#fff;margin-bottom:6px}
.box-s{font-size:12px;font-weight:600;color:#22c55e}
.box-note{font-size:11px;color:#cbd5e1;margin-top:8px;padding:8px;background:rgba(245,158,11,.1);border-radius:6px;
  border-left:3px solid #f59e0b;text-align:left;line-height:1.5}
.lvl-item{background:#111827;padding:7px 11px;margin:5px 0;border-radius:6px;font-size:14px;text-align:center}
/* options tab */
.opt-controls{display:flex;gap:12px;align-items:center;margin-bottom:16px;flex-wrap:wrap}
.opt-btn{padding:11px 22px;border:none;border-radius:9px;font-size:13px;font-weight:700;cursor:pointer}
.opt-run{background:linear-gradient(135deg,#22c55e,#16a34a);color:#fff}
.opt-stop{background:#c62828;color:#fff}
.opt-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin-bottom:16px}
.oc-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid #1e293b}
.oc-title{font-size:15px;font-weight:700}
.nifty{color:#f59e0b}.banknifty{color:#ef4444}.sensex{color:#22c55e}
.bw{background:#334155;color:#94a3b8}.bs{background:#0c4a6e;color:#7dd3fc}.ba{background:#14532d;color:#4ade80}
.bt2{background:#14532d;color:#4ade80}.bsl2{background:#7f1d1d;color:#fca5a5}
.term-wrap{background:#000;border-radius:10px;border:1px solid #1e293b;overflow:hidden;margin-bottom:16px}
.term-head{background:#0f1a2e;padding:8px 12px;font-size:11px;color:#94a3b8;border-bottom:1px solid #1e293b;
  display:flex;justify-content:space-between}
.term{padding:10px 12px;height:280px;overflow-y:auto;font-family:Consolas,monospace;font-size:11px;line-height:1.5}
.line{padding:1px 0}.t{color:#546e7a;margin-right:6px}
.i{color:#38bdf8}.s{color:#4ade80}.w{color:#fbbf24}.e{color:#fca5a5}.tr{color:#c4b5fd;font-weight:bold}
.bt-box{background:#0a0e1a;border:1px solid #1e293b;border-radius:10px;padding:14px;min-height:100px;max-height:340px;overflow-y:auto}
.dot{width:8px;height:8px;border-radius:50%;background:#22c55e;display:inline-block;margin-right:5px}
.dot.upd{background:#f59e0b;animation:pulse 1s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
::-webkit-scrollbar{width:6px}::-webkit-scrollbar-track{background:#0a0e1a}::-webkit-scrollbar-thumb{background:#1e293b;border-radius:3px}
</style>
</head>
<body>
<div class="top">
  <div><h1>MASTER TRADING BOT</h1>
    <div class="sub">Intraday &middot; Swing &middot; Investment &middot; Options</div></div>
  <div class="clock"><span class="dot" id="livedot"></span><span id="clock">--:--:--</span></div>
</div>

<div class="tabbar">
  <div class="tab on" data-tab="intraday" onclick="switchTab('intraday')">Intraday</div>
  <div class="tab" data-tab="swing" onclick="switchTab('swing')">Swing</div>
  <div class="tab" data-tab="investment" onclick="switchTab('investment')">Investment</div>
  <div class="tab" data-tab="options" onclick="switchTab('options')">Options</div>
</div>

<!-- INTRADAY -->
<div class="panel on" id="p-intraday">
  <div class="stats" id="intra-stats"></div>
  <div class="filterbar">
    <div class="fb on" data-f="trade" onclick="intraFilter('trade')">With Trade</div>
    <div class="fb" data-f="all" onclick="intraFilter('all')">All</div>
    <div class="fb" data-f="ACTIVE" onclick="intraFilter('ACTIVE')">Active</div>
    <div class="fb" data-f="TP_HIT" onclick="intraFilter('TP_HIT')">TP Hit</div>
    <div class="fb" data-f="SL_HIT" onclick="intraFilter('SL_HIT')">SL Hit</div>
    <div class="fb" data-f="PENDING_ENTRY" onclick="intraFilter('PENDING_ENTRY')">Pending</div>
    <div class="fb" data-f="SCANNING" onclick="intraFilter('SCANNING')">Scanning</div>
    <input class="search" id="intra-q" placeholder="search symbol..." oninput="intraSearch(this.value)">
  </div>
  <div class="grid" id="intra-grid"></div>
</div>

<!-- SWING -->
<div class="panel" id="p-swing">
  <div class="stats" id="swing-stats"></div>
  <div class="filterbar">
    <div class="fb on" data-f="all" onclick="swingFilter('all')">All</div>
    <div class="fb" data-f="signals" onclick="swingFilter('signals')">Signals</div>
    <div class="fb" data-f="trades" onclick="swingFilter('trades')">Active Trades</div>
    <div class="fb" data-f="gainers" onclick="swingFilter('gainers')">Gainers</div>
    <div class="fb" data-f="losers" onclick="swingFilter('losers')">Losers</div>
    <input class="search" id="swing-q" placeholder="search symbol..." oninput="swingSearch(this.value)">
  </div>
  <div class="grid" id="swing-grid"></div>
</div>

<!-- INVESTMENT -->
<div class="panel" id="p-investment">
  <div class="prog" id="inv-prog"></div>
  <div class="filterbar">
    <input class="search" id="inv-q" placeholder="search stock..." oninput="invSearch(this.value)" style="margin-left:0">
  </div>
  <div id="inv-list"></div>
</div>

<!-- OPTIONS -->
<div class="panel" id="p-options">
  <div class="stats" id="opt-stats"></div>
  <div class="opt-controls">
    <span class="badge ba" id="opt-phase">AUTO-RUNNING 24x7</span>
    <span id="opt-status" style="color:#94a3b8;font-size:12px">Bot login ke saath hi chalu ho gaya. Market 9:15-15:30 me scan, baaki time silent background.</span>
  </div>
  <div class="opt-cards" id="opt-cards"></div>
  <div class="term-wrap">
    <div class="term-head"><span>Live Logs</span><span style="color:#546e7a">auto-scroll</span></div>
    <div class="term" id="opt-term"></div>
  </div>
  <div class="inv-card" style="margin-bottom:16px">
    <div style="padding:13px 16px;display:flex;gap:10px;align-items:flex-end;flex-wrap:wrap;border-bottom:1px solid #1e293b">
      <div><div style="color:#94a3b8;font-size:11px;margin-bottom:4px">Backtest Date</div>
        <input type="date" id="bt-date" style="padding:9px;background:#0a0e1a;border:1px solid #1e293b;border-radius:7px;color:#fff;font-size:13px"></div>
      <button class="opt-btn" style="background:linear-gradient(135deg,#7c3aed,#5b21b6);color:#fff" id="bt-run" onclick="runBT()">RUN BACKTEST</button>
      <div id="bt-summary" style="color:#94a3b8;font-size:12px;margin-left:auto"></div>
    </div>
    <div style="padding:14px"><div class="bt-box" id="bt-box">
      <div style="color:#546e7a;font-size:12px;text-align:center;padding:26px 0">Date select karke RUN BACKTEST dabao</div>
    </div></div>
  </div>
</div>
"""

DASHBOARD_HTML += r"""
<script>
var ACTIVE='intraday';
var money=function(n){return 'Rs.'+Number(n).toFixed(2);};
var sign=function(n){return (n>=0?'+':'')+Number(n).toFixed(2);};

function clock(){document.getElementById('clock').textContent=new Date().toLocaleTimeString('en-IN',{hour12:false});}
setInterval(clock,1000);clock();

function switchTab(t){
  ACTIVE=t;
  document.querySelectorAll('.tab').forEach(function(x){x.classList.toggle('on',x.dataset.tab===t);});
  document.querySelectorAll('.panel').forEach(function(x){x.classList.remove('on');});
  document.getElementById('p-'+t).classList.add('on');
  poll();
}

/* ============ INTRADAY ============ */
var intraData=null, intraF='trade', intraQ='';
function intraFilter(f){intraF=f;document.querySelectorAll('#p-intraday .fb').forEach(function(x){x.classList.toggle('on',x.dataset.f===f);});renderIntra();}
function intraSearch(v){intraQ=v.trim().toLowerCase();renderIntra();}
function intraPass(s){
  if(intraQ && !(s.symbol+' '+s.name).toLowerCase().includes(intraQ))return false;
  if(intraF==='all')return true;
  if(intraF==='trade')return !!s.trade;
  if(['ACTIVE','TP_HIT','SL_HIT'].includes(intraF))return s.trade&&s.trade.status===intraF;
  return s.scan_state===intraF&&!s.trade;
}
function intraBadge(s){
  if(s.trade){if(s.trade.status==='TP_HIT')return '<span class="badge b-tp">TP Hit</span>';
    if(s.trade.status==='SL_HIT')return '<span class="badge b-sl">SL Hit</span>';
    return '<span class="badge b-act">Active</span>';}
  if(s.scan_state==='PENDING_ENTRY')return '<span class="badge b-pend">Pending</span>';
  if(s.scan_state==='NO_DATA')return '<span class="badge b-nd">No Data</span>';
  return '<span class="badge b-scan">Scanning</span>';
}
function intraCls(s){
  if(s.trade){if(s.trade.status==='TP_HIT')return 'card tp';if(s.trade.status==='SL_HIT')return 'card sl';return 'card locked';}
  if(s.scan_state==='PENDING_ENTRY')return 'card pending';return 'card';
}
function renderIntra(){
  if(!intraData)return;var d=intraData,su=d.summary;
  document.getElementById('intra-stats').innerHTML=
    stat('Trades',su.total_trades,'c-blue')+stat('Active',su.active,'c-amber')+
    stat('TP Hit',su.tp_hit,'c-green')+stat('SL Hit',su.sl_hit,'c-red')+
    stat('Pending',su.pending,'c-amber')+stat('Scanning',su.scanning,'c-slate')+
    stat('Net MTM',sign(su.net_mtm),su.net_mtm>=0?'c-green':'c-red');
  var g=document.getElementById('intra-grid');
  if(!d.stocks.length){g.innerHTML='<div class="empty"><b>First scan in progress...</b><br>Fetching 1-min candles for '+d.universe+' symbols. Cards appear as stocks are scanned.</div>';return;}
  var list=d.stocks.filter(intraPass);
  if(!list.length){g.innerHTML='<div class="empty">No stocks match this filter.<br>Scanned '+d.stocks.length+' of '+(d.universe-d.dead)+' symbols. Try the All tab.</div>';return;}
  g.innerHTML=list.map(function(s){
    var chCls=s.change>=0?'c-green':'c-red',inner='';
    if(s.trade){var t=s.trade,pnl=t.mtm>=0?'c-green':'c-red',dc=t.direction==='LONG'?'c-green':'c-red';
      inner='<div class="tbox"><div class="trow"><span class="k">Direction</span><span class="v '+dc+'">'+t.direction+'</span></div>'+
        '<div class="trow"><span class="k">Entry</span><span class="v">'+money(t.entry_price)+'</span></div>'+
        '<div class="trow"><span class="k">SL</span><span class="v c-red">'+money(t.sl_price)+'</span></div>'+
        '<div class="trow"><span class="k">Target</span><span class="v c-green">'+money(t.tp_price)+'</span></div>'+
        '<div class="trow"><span class="k">'+(t.status==='ACTIVE'?'Live P&L':'Final P&L')+'</span><span class="v '+pnl+'">'+sign(t.mtm)+' ('+sign(t.mtm_pct)+'%)</span></div></div>'+
        '<div class="meta"><span>Setup '+t.setup_candle_time+'</span><span>Entry '+t.entry_candle_time+'</span></div>';
    }else if(s.scan_state==='PENDING_ENTRY'){inner='<div class="pend-txt">'+s.pending_info+'</div>';}
    else if(s.scan_state==='NO_DATA'){inner='<div class="scan-txt">'+(s.error||'no data')+'</div>';}
    else{inner='<div class="scan-txt">'+(s.pending_info||'monitoring')+' &middot; '+s.candles_scanned+' candles</div>';}
    return '<div class="'+intraCls(s)+'"><div class="ctop"><div><div class="sym">'+s.symbol+'</div><div class="nm">'+s.name+'</div></div>'+
      '<div class="px"><div class="p">'+money(s.price)+'</div><div class="ch '+chCls+'">'+sign(s.change)+'%</div></div></div>'+
      intraBadge(s)+inner+'<div class="meta"><span>last '+(s.last_candle_time||'--')+'</span><span>upd '+s.last_update+'</span></div></div>';
  }).join('');
}

/* ============ SWING ============ */
var swingData=null, swingF='all', swingQ='';
function swingFilter(f){swingF=f;document.querySelectorAll('#p-swing .fb').forEach(function(x){x.classList.toggle('on',x.dataset.f===f);});renderSwing();}
function swingSearch(v){swingQ=v.trim().toLowerCase();renderSwing();}
function renderSwing(){
  if(!swingData)return;var d=swingData,s=d.summary;
  document.getElementById('swing-stats').innerHTML=
    stat('Stocks',s.total_stocks,'c-blue')+stat('Real Data',s.real_data_count,'c-slate')+
    stat('Signals',s.total_signals,'c-amber')+stat('Active',s.active_trades,'c-green')+
    stat('Closed',s.total_closed,'c-slate')+stat('Total P&L',sign(s.total_pnl),s.total_pnl>=0?'c-green':'c-red');
  var entries=Object.entries(d.stocks);
  if(swingF==='signals')entries=entries.filter(function(e){return e[1].last_signal;});
  else if(swingF==='trades')entries=entries.filter(function(e){return e[1].active_trade;});
  else if(swingF==='gainers')entries=entries.filter(function(e){return e[1].change_percent>0;}).sort(function(a,b){return b[1].change_percent-a[1].change_percent;});
  else if(swingF==='losers')entries=entries.filter(function(e){return e[1].change_percent<0;}).sort(function(a,b){return a[1].change_percent-b[1].change_percent;});
  if(swingQ)entries=entries.filter(function(e){return e[0].toLowerCase().includes(swingQ);});
  var g=document.getElementById('swing-grid');
  var vis=entries.filter(function(e){return e[1].current_price!==0||e[1].fetch_error;});
  if(!vis.length){g.innerHTML='<div class="empty">No stocks match. Data still loading (swing rescans every 30 min).</div>';return;}
  g.innerHTML=vis.map(function(e){
    var sym=e[0],st=e[1];
    var cls='card';if(st.active_trade)cls+=(st.active_trade.direction==='BUY'?' buy':' sell');
    var chCls=st.change_percent>=0?'c-green':'c-red';
    var inner='<div class="tbox"><div class="trow"><span class="k">Last 1M H/L</span><span class="v">'+
      (st.weekly_high>0?st.weekly_high.toFixed(2):'--')+' / '+(st.weekly_low>0?st.weekly_low.toFixed(2):'--')+'</span></div></div>';
    if(st.active_trade){var t=st.active_trade;
      inner+='<div class="tbox" style="border-color:#38bdf8"><div class="trow"><span class="k">ACTIVE</span><span class="badge '+(t.direction==='BUY'?'b-buy':'b-sell')+'">'+t.direction+'</span></div>'+
        '<div class="trow"><span class="k">Entry</span><span class="v">'+t.entry_price+'</span></div>'+
        '<div class="trow"><span class="k">SL</span><span class="v c-red">'+t.sl_price+'</span></div>'+
        '<div class="trow"><span class="k">TP</span><span class="v c-green">'+t.tp_price+'</span></div>'+
        '<div class="trow"><span class="k">Fib</span><span class="v">'+t.fib_level+'</span></div>'+
        '<div class="meta"><span>opened '+t.open_time+'</span></div></div>';
    }else if(st.pending_signal){var p=st.pending_signal;
      inner+='<div class="pend-txt">'+p.direction+' breakout @ fib '+p.fib_level+' ('+p.fib_price+') - entry next candle open</div>';
    }
    if(st.last_closed_trade){var lc=st.last_closed_trade;var isTP=lc.status==='CLOSED_TP';
      var badge=isTP?'TP':(lc.status==='CLOSED_WEEKLY'?'WK END':'SL');
      inner+='<div class="tbox" style="opacity:.8"><div class="trow"><span class="k">Last Closed</span><span class="badge '+(isTP?'b-tp':'b-sl')+'">'+badge+'</span></div>'+
        '<div class="trow"><span class="k">Entry/Exit</span><span class="v">'+lc.entry_price+' / '+lc.close_price+'</span></div>'+
        '<div class="trow"><span class="k">P&L</span><span class="v '+((lc.pnl||0)>=0?'c-green':'c-red')+'">'+sign(lc.pnl||0)+' ('+(lc.pnl_percent||0)+'%)</span></div></div>';
    }
    return '<div class="'+cls+'"><div class="ctop"><div><div class="sym">'+sym+'</div><div class="nm">'+(st.company_name||'NSE')+'</div></div>'+
      '<div class="px"><div class="p '+chCls+'">'+(st.current_price>0?st.current_price.toFixed(2):'N/A')+'</div><div class="ch '+chCls+'">'+sign(st.change_percent)+'%</div></div></div>'+
      inner+'</div>';
  }).join('');
}

/* ============ INVESTMENT ============ */
var invData=null, invQ='', invOpen={};
function invSearch(v){invQ=v.trim().toUpperCase();renderInv();}
function invToggle(n){invOpen[n]=!invOpen[n];renderInv();}
function renderInv(){
  if(!invData)return;var d=invData;
  document.getElementById('inv-prog').innerHTML=
    '<div class="prog-t"><span>'+(d.done?'Scan Complete':'Scanning...')+' '+d.progress+'/'+d.total+'</span><span>'+d.pct+'%</span></div>'+
    '<div class="prog-bar"><div class="prog-fill" style="width:'+d.pct+'%"></div></div>';
  var list=d.stocks;if(invQ)list=list.filter(function(s){return s.name.includes(invQ);});
  var box=document.getElementById('inv-list');
  if(!list.length){box.innerHTML='<div class="empty">Scanning stocks... please wait.</div>';return;}
  box.innerHTML=list.map(function(s){
    var open=invOpen[s.name];
    var box1='';
    if(s.show_current){box1='<div class="box cur"><div class="box-l">Current Investment Level</div><div class="box-p">Rs.'+s.current+'</div><div class="box-s">Near LTP (Rs.'+s.price+')</div></div>';}
    else if(s.upcoming>s.price){box1='<div class="box up"><div class="box-l">Upcoming Level</div><div class="box-p">Rs.'+s.upcoming+'</div><div class="box-note"><b>Note:</b> Price wapas Rs.'+s.upcoming+' aaye to chhoti qty invest karo. Weekly confirmation ka wait.</div></div>';}
    else{box1='<div class="box up"><div class="box-l">Upcoming Level</div><div class="box-p">Rs.'+s.upcoming+'</div></div>';}
    var n5=s.next5.map(function(l){return '<div class="lvl-item">Rs.'+l+'</div>';}).join('');
    var box2='<div class="box nx"><div class="box-l">Next Investment Levels</div>'+n5+'</div>';
    var tr=s.trend||'NEUTRAL';
    var trCls=tr==='BULLISH'?'b-buy':tr==='BEARISH'?'b-sell':'b-scan';
    var trTxt=tr==='BULLISH'?'BULLISH':tr==='BEARISH'?'BEARISH':'NEUTRAL';
    var trBadge='<span class="badge '+trCls+'">'+trTxt+((s.trend_pct!==undefined)?(' '+(s.trend_pct>=0?'+':'')+s.trend_pct+'%'):'')+'</span>';
    return '<div class="inv-card"><div class="inv-head" onclick="invToggle(\''+s.name+'\')">'+
      '<div class="inv-title"><h3>'+s.name+'</h3><span class="ltp">Rs.'+s.price+'</span>'+trBadge+'</div>'+
      '<span>'+(open?'&#9650;':'&#9660;')+'</span></div>'+
      '<div class="inv-body'+(open?' open':'')+'"><div class="boxes">'+box1+box2+'</div></div></div>';
  }).join('');
}

/* ============ OPTIONS ============ */
var optTd={nifty:{},banknifty:{},sensex:{}};
var optES=null, btES=null, optStarted=false;
function stat(l,v,cls){return '<div class="stat"><div class="l">'+l+'</div><div class="v '+(cls||'')+'">'+v+'</div></div>';}
function renderOptStats(){
  var a=0,c=0,pnl=0;
  ['nifty','banknifty','sensex'].forEach(function(i){var d=optTd[i];
    if(d.status==='ACTIVE')a++;if(d.status==='TARGET'||d.status==='SL'){c++;pnl+=(d.pnl||0);}});
  document.getElementById('opt-stats').innerHTML=
    stat('Active',a+'/3','c-amber')+stat('Done',c,'c-slate')+
    stat('P&L','Rs.'+pnl.toFixed(2),pnl>=0?'c-green':'c-red')+
    stat('Bot',optStarted?'RUNNING':'IDLE',optStarted?'c-green':'c-slate');
}
function renderOptCards(){
  var idx=[['NIFTY','nifty','nifty'],['BANKNIFTY','banknifty','banknifty'],['SENSEX','sensex','sensex']];
  document.getElementById('opt-cards').innerHTML=idx.map(function(x){
    var d=optTd[x[1]]||{};var map={'WAITING':'bw','SEARCHING':'bs','ACTIVE':'ba','TARGET':'bt2','SL':'bsl2'};
    var st=d.status||'WAITING';
    return '<div class="card"><div class="oc-head"><span class="oc-title '+x[2]+'">'+x[0]+'</span>'+
      '<span class="badge '+(map[st]||'bw')+'">'+st+'</span></div>'+
      row('Strike',d.strike||'--')+row('Type',d.type||'--')+
      row('Entry',d.entry?'Rs.'+d.entry:'--')+row('LTP',d.ltp?'Rs.'+d.ltp:'--')+
      row('SL / TP',(d.sl&&d.tp)?'Rs.'+d.sl+' / Rs.'+d.tp:'--')+
      row('P&L',d.pnl!==undefined?(d.pnl>=0?'+':'')+'Rs.'+Number(d.pnl).toFixed(2):'--',d.pnl>=0?'c-green':'c-red')+'</div>';
  }).join('');
}
function row(k,v,cls){return '<div class="trow"><span class="k">'+k+'</span><span class="v '+(cls||'')+'">'+v+'</span></div>';}
function optLog(msg,type){
  var term=document.getElementById('opt-term');type=type||'i';
  var line=document.createElement('div');line.className='line';
  var t='['+new Date().toLocaleTimeString('en-IN',{hour12:false})+'] ';
  line.innerHTML='<span class="t">'+t+'</span><span class="'+type+'">'+msg.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')+'</span>';
  term.appendChild(line);if(term.children.length>250)term.removeChild(term.firstChild);term.scrollTop=term.scrollHeight;
}
function connectOptStream(){
  if(optES)return;
  optES=new EventSource('/api/options/stream');
  optES.onmessage=function(e){try{var d=JSON.parse(e.data);
    if(d.keepalive)return;
    if(d.log)optLog(d.log,d.log_type);
    if(d.index&&d.card){optTd[d.index.toLowerCase()]=d.card;renderOptCards();renderOptStats();}
    if(d.bot_status==='FINISHED'){optStarted=false;renderOptStats();}
  }catch(err){}};
}
async function pollOptStatus(){
  // Options bot auto-runs 24x7. Just reflect its phase (RUNNING/WAITING/
  // CLOSED/BEFORE_OPEN) and detail text on the page.
  try{
    var r=await fetch('/api/options/status');var d=await r.json();
    optStarted=!!d.running;
    var ph=document.getElementById('opt-phase');
    var phase=d.phase||(d.running?'RUNNING':'IDLE');
    ph.textContent=(d.running?'AUTO-RUNNING 24x7 · ':'')+phase;
    ph.className='badge '+(phase==='RUNNING'||phase==='SCANNING'?'ba':phase==='CLOSED'||phase==='WAITING'?'bw':'bs');
    if(d.detail)document.getElementById('opt-status').textContent=d.detail;
    renderOptStats();
  }catch(e){}
}
document.getElementById('bt-date').max=new Date().toISOString().split('T')[0];
function btLog(m,t){var box=document.getElementById('bt-box');
  if(box.dataset.cleared!=='1'){box.innerHTML='';box.dataset.cleared='1';}
  var c=t==='s'?'#4ade80':t==='e'?'#fca5a5':t==='w'?'#fbbf24':t==='tr'?'#c4b5fd':'#38bdf8';
  var d=document.createElement('div');d.style.cssText='font-family:Consolas,monospace;font-size:11px;padding:1px 0;color:'+c;
  d.textContent=m;box.appendChild(d);box.scrollTop=box.scrollHeight;}
function renderBT(trades,date){
  var box=document.getElementById('bt-box');box.innerHTML='';box.dataset.cleared='1';
  if(!trades.length){box.innerHTML='<div style="color:#fbbf24;text-align:center;padding:26px 0">'+date+' - koi trade nahi bana</div>';
    document.getElementById('bt-summary').textContent='';return;}
  var tot=0,win=0;
  var h='<table style="width:100%;border-collapse:collapse;font-size:12px"><tr style="color:#64748b;font-size:10px">'+
    '<th style="text-align:left;padding:6px 4px">Index</th><th style="text-align:left">Strike</th><th style="text-align:left">In</th>'+
    '<th style="text-align:left">Entry</th><th style="text-align:left">Out</th><th style="text-align:left">Exit</th>'+
    '<th style="text-align:left">Status</th><th style="text-align:right">P&L</th></tr>';
  trades.forEach(function(t){tot+=t.pnl;if(t.pnl>0)win++;
    var col=t.status==='TARGET'?'#4ade80':t.status==='SL'?'#fca5a5':'#fbbf24';
    h+='<tr style="border-top:1px solid #1e293b"><td style="padding:7px 4px;color:#38bdf8">'+t.index+'</td>'+
      '<td>'+t.strike+t.type+'</td><td>'+t.in_time+'</td><td>Rs.'+t.entry+'</td><td>'+t.out_time+'</td><td>Rs.'+t.exit+'</td>'+
      '<td style="color:'+col+';font-weight:bold">'+t.status+'</td>'+
      '<td style="text-align:right;font-weight:bold;color:'+(t.pnl>=0?'#4ade80':'#fca5a5')+'">'+(t.pnl>=0?'+':'')+'Rs.'+t.pnl.toFixed(2)+'</td></tr>';});
  h+='</table>';box.innerHTML=h;
  document.getElementById('bt-summary').innerHTML=date+' | Trades: <b>'+trades.length+'</b> Win: <b style="color:#4ade80">'+win+
    '</b> Net: <b style="color:'+(tot>=0?'#4ade80':'#fca5a5')+'">'+(tot>=0?'+':'')+'Rs.'+tot.toFixed(2)+'</b>';
}
async function runBT(){
  var d=document.getElementById('bt-date').value;
  var btn=document.getElementById('bt-run');
  if(!d){alert('Pehle date select karo!');return;}
  var box=document.getElementById('bt-box');box.innerHTML='';box.dataset.cleared='1';
  document.getElementById('bt-summary').textContent='';
  btn.disabled=true;btn.textContent='Running...';
  try{var r=await fetch('/api/options/backtest',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({date:d})});
    var j=await r.json();if(!j.success){btLog('Error: '+j.error,'e');btn.disabled=false;btn.textContent='RUN BACKTEST';return;}
  }catch(e){btLog('Error: '+e,'e');btn.disabled=false;btn.textContent='RUN BACKTEST';return;}
  if(btES)btES.close();
  btES=new EventSource('/api/options/backtest_stream');
  btES.onmessage=function(e){var m=JSON.parse(e.data);if(m.keepalive)return;
    if(m.log)btLog(m.log,m.log_type);
    if(m.done){btES.close();btES=null;renderBT(m.trades||[],m.date||d);btn.disabled=false;btn.textContent='RUN BACKTEST';}};
}

/* ============ POLLING ============ */
async function poll(){
  var dot=document.getElementById('livedot');dot.className='dot upd';
  try{
    if(ACTIVE==='intraday'){var r=await fetch('/api/intraday');intraData=await r.json();renderIntra();}
    else if(ACTIVE==='swing'){var r=await fetch('/api/swing');swingData=await r.json();renderSwing();}
    else if(ACTIVE==='investment'){var r=await fetch('/api/investment');invData=await r.json();renderInv();}
    else if(ACTIVE==='options'){renderOptStats();renderOptCards();}
    dot.className='dot';
  }catch(e){console.error('poll failed',e);}
}
// Options bot auto-runs from login -> connect its live log stream immediately
// and keep its phase badge fresh regardless of which tab is open.
connectOptStream();
pollOptStatus();
setInterval(pollOptStatus,10000);
poll();
setInterval(poll,8000);   // faster refresh so progressive loads show quickly
</script>
</body>
</html>"""


# =============================================================================
# =============================================================================
#   MAIN
# =============================================================================
# =============================================================================

def main():
    print("=" * 72)
    print("   MASTER TRADING BOT  --  4 bots in 1")
    print("=" * 72)
    print("   TAB 1  Intraday    : 1-min Fibonacci breakout   (yfinance)")
    print("   TAB 2  Swing       : 1M levels / 1D candles      (yfinance)")
    print("   TAB 3  Investment  : weekly investment levels    (yfinance)")
    print("   TAB 4  Options     : Dhan NIFTY/BANKNIFTY/SENSEX (Dhan API)")
    print("=" * 72)
    print(f"   Dashboard : http://localhost:{PORT}")
    print("   Login zaroori hai (Dhan Client ID + Access Token).")
    print("   Login ke baad yfinance engines apne aap start honge.")
    print("=" * 72)
    print()

    # ── AUTO-LOGIN from environment variables (for cloud / 24x7) ──
    # Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in Render env vars.
    env_cid = os.environ.get("DHAN_CLIENT_ID", "").strip()
    env_tok = os.environ.get("DHAN_ACCESS_TOKEN", "").strip()
    if env_cid and env_tok:
        global bot_credentials
        try:
            headers = {"client-id": env_cid, "access-token": env_tok,
                       "Content-Type": "application/json"}
            r = requests.get("https://api.dhan.co/v2/fundlimit",
                             headers=headers, timeout=10)
            funds = r.json()
            ok = r.status_code == 200 and isinstance(funds, dict) and (
                "availabelBalance" in funds or "availableBalance" in funds)
            if ok:
                bot_credentials = {"client_id": env_cid, "access_token": env_tok}
                logged_in.set()
                _start_engine_threads()
                print("[OK] Auto-login success — engines started (cloud mode)")
            else:
                print(f"[WARN] Auto-login failed (token expired?): {str(funds)[:120]}")
        except Exception as e:
            print(f"[WARN] Auto-login error: {e}")
    else:
        print("[INFO] No env credentials — waiting for manual login via web")

    # Local pe browser auto-open; cloud pe (RENDER set) skip
    if not os.environ.get("RENDER"):
        def open_browser():
            time.sleep(2)
            try:
                webbrowser.open(f"http://localhost:{PORT}")
            except Exception:
                pass
        threading.Thread(target=open_browser, daemon=True).start()

    app.run(host='0.0.0.0', port=PORT, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
