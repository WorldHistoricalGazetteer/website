import json
import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from django.core.management.base import BaseCommand

# Standardised language codes
LANG_MAP = {
    "ENG": "en",
    "CHN": "zh",
    "RUS": "ru",
    "FRA": "fr",
    "ESP": "es",
    "ARB": "ar",
}


def text_or_none(cell):
    return cell.get_text(strip=True) if cell else ""


def norm_code(s):
    s = (s or "").strip()
    return s.zfill(3) if s.isdigit() else s


def ensure_entry(store, code, level=None):
    if not code:
        return
    if code not in store:
        store[code] = {
            "m49": code,
            "level": level or "unknown",
            "labels": {},
            "parents": set(),
            "children": set(),
            "members": set(),
            "iso_alpha2": set(),
            "iso_alpha3": set(),
        }
    elif level and store[code]["level"] == "unknown":
        store[code]["level"] = level


class Command(BaseCommand):
    help = "Scrape UN M49 region data and save to JSON."

    def add_arguments(self, parser):
        parser.add_argument(
            "--outfile",
            type=str,
            help="Output JSON filename (default: regions/data/m49_regions.json)",
        )

    def handle(self, *args, **options):
        base_dir = Path(__file__).resolve().parents[2]  # points to 'regions' folder
        outfile = Path(options["outfile"]) if options.get("outfile") else base_dir / "data" / "m49_regions.json"

        self.stdout.write(f"Fetching UN M49 page ...")
        url = "https://unstats.un.org/unsd/methodology/m49/overview/"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        panes = soup.select("div.tab-pane")
        if not panes:
            self.stdout.write(self.style.WARNING("No tab panes found"))
            return

        store = {}

        for pane in panes:
            pane_id = pane.get("id", "")
            m = re.match(r"([A-Z]{3})_", pane_id)
            if not m:
                continue
            lang = LANG_MAP.get(m.group(1), m.group(1).lower())
            self.stdout.write(f"Processing language: {lang}")

            table = pane.find("table")
            if not table:
                continue

            headers = [th.get_text(strip=True) for th in table.select("thead tr th, thead tr td")]
            col_map = {}
            for idx, h in enumerate(headers):
                h_low = h.lower()
                if "global code" in h_low:
                    col_map["global_code"] = idx
                elif "global name" in h_low:
                    col_map["global_name"] = idx
                elif "region code" in h_low and "intermediate" not in h_low:
                    col_map["region_code"] = idx
                elif "region name" in h_low and "intermediate" not in h_low:
                    col_map["region_name"] = idx
                elif "sub-region code" in h_low or "subregion code" in h_low:
                    col_map["subregion_code"] = idx
                elif "sub-region name" in h_low or "subregion name" in h_low:
                    col_map["subregion_name"] = idx
                elif "intermediate region code" in h_low:
                    col_map["intermediate_code"] = idx
                elif "intermediate region name" in h_low:
                    col_map["intermediate_name"] = idx
                elif "country or area" in h_low:
                    col_map["country_name"] = idx
                elif "m49 code" in h_low:
                    col_map["country_m49"] = idx
                elif "iso-alpha2" in h_low:
                    col_map["iso2"] = idx
                elif "iso-alpha3" in h_low:
                    col_map["iso3"] = idx

            for tr in table.select("tbody tr"):
                tds = tr.find_all(["td", "th"])
                if len(tds) < 5:
                    continue  # skip malformed rows

                def c(name):
                    i = col_map.get(name)
                    return text_or_none(tds[i]) if i is not None and i < len(tds) else ""

                g_code, g_name = norm_code(c("global_code")), c("global_name")
                r_code, r_name = norm_code(c("region_code")), c("region_name")
                s_code, s_name = norm_code(c("subregion_code")), c("subregion_name")
                i_code, i_name = norm_code(c("intermediate_code")), c("intermediate_name")
                country_m49, country_name = norm_code(c("country_m49")), c("country_name")
                iso2, iso3 = c("iso2").strip(), c("iso3").strip()

                # create entries + labels
                if g_code:
                    ensure_entry(store, g_code, "global")
                    if g_name:
                        store[g_code]["labels"][lang] = g_name
                if r_code:
                    ensure_entry(store, r_code, "region")
                    if r_name:
                        store[r_code]["labels"][lang] = r_name
                if s_code:
                    ensure_entry(store, s_code, "sub-region")
                    if s_name:
                        store[s_code]["labels"][lang] = s_name
                if i_code:
                    ensure_entry(store, i_code, "intermediate")
                    if i_name:
                        store[i_code]["labels"][lang] = i_name
                if country_m49:
                    ensure_entry(store, country_m49, "country")
                    if country_name:
                        store[country_m49]["labels"][lang] = country_name
                    if iso2:
                        store[country_m49]["iso_alpha2"].add(iso2)
                    if iso3:
                        store[country_m49]["iso_alpha3"].add(iso3)

                # relationships
                if g_code and r_code:
                    store[g_code]["children"].add(r_code)
                    store[r_code]["parents"].add(g_code)
                if r_code and s_code:
                    store[r_code]["children"].add(s_code)
                    store[s_code]["parents"].add(r_code)
                if s_code and i_code:
                    store[s_code]["children"].add(i_code)
                    store[i_code]["parents"].add(s_code)

                parent = i_code or s_code or r_code or g_code
                if parent and country_m49:
                    store[parent]["children"].add(country_m49)
                    store[country_m49]["parents"].add(parent)

                # membership propagation
                if country_m49:
                    for anc in filter(None, [i_code, s_code, r_code, g_code]):
                        store[anc]["members"].add(country_m49)
                        if iso2:
                            store[anc]["iso_alpha2"].add(iso2)

        # tidy up sets into lists
        out = {}
        for code, v in store.items():
            out[code] = {
                "m49": v["m49"],
                "level": v["level"],
                "labels": v["labels"],
                "parents": sorted(v["parents"]),
                "children": sorted(v["children"]),
                "members": sorted(v["members"]),
                "iso_alpha2": sorted(v["iso_alpha2"]),
                "iso_alpha3": sorted(v["iso_alpha3"]),
            }

        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)

        self.stdout.write(self.style.SUCCESS(f"Wrote {outfile} with {len(out)} entries."))
