"""
Google Maps Multi-Query Scraper - Paralelo
Corre múltiples búsquedas en paralelo y consolida en un solo CSV.

Uso:
  python gmaps_scraper_multi.py
"""

import asyncio
import re
import pandas as pd
from playwright.async_api import async_playwright

# ── Configuración ─────────────────────────────────────────────────────────────
LIMIT_PER_QUERY = 10   # negocios por búsqueda
WORKERS = 8            # browsers en paralelo (subí a 8 si tu PC lo aguanta)
OUTPUT_FILE = "pymes_argentina.csv"

QUERIES = [
    "dietéticas Buenos Aires",
    "ferreterías Buenos Aires",
    "gimnasios Buenos Aires",
    "peluquerías Buenos Aires",
    "veterinarias Buenos Aires",
    "panaderías Buenos Aires",
    "librerías Buenos Aires",
    "cerrajerías Buenos Aires",
    "talleres mecánicos Buenos Aires",
    "inmobiliarias Buenos Aires",
    "farmacias Buenos Aires",
    "ópticas Buenos Aires",
    "cafeterías Buenos Aires",
    "restaurantes Buenos Aires",

    "dietéticas Córdoba",
    "ferreterías Córdoba",
    "gimnasios Córdoba",
    "peluquerías Córdoba",
    "veterinarias Córdoba",
    "panaderías Córdoba",
    "librerías Córdoba",
    "cerrajerías Córdoba",
    "talleres mecánicos Córdoba",
    "inmobiliarias Córdoba",
    "farmacias Córdoba",
    "ópticas Córdoba",
    "cafeterías Córdoba",
    "restaurantes Córdoba",

    "dietéticas Rosario",
    "ferreterías Rosario",
    "gimnasios Rosario",
    "peluquerías Rosario",
    "veterinarias Rosario",
    "panaderías Rosario",
    "librerías Rosario",
    "cerrajerías Rosario",
    "talleres mecánicos Rosario",
    "inmobiliarias Rosario",
    "farmacias Rosario",
    "ópticas Rosario",
    "cafeterías Rosario",
    "restaurantes Rosario",

    "dietéticas Mendoza",
    "ferreterías Mendoza",
    "gimnasios Mendoza",
    "peluquerías Mendoza",
    "veterinarias Mendoza",
    "panaderías Mendoza",
    "librerías Mendoza",
    "cerrajerías Mendoza",
    "talleres mecánicos Mendoza",
    "inmobiliarias Mendoza",
    "farmacias Mendoza",
    "ópticas Mendoza",
    "cafeterías Mendoza",
    "restaurantes Mendoza",

    "dietéticas La Plata",
    "ferreterías La Plata",
    "gimnasios La Plata",
    "peluquerías La Plata",
    "veterinarias La Plata",
    "panaderías La Plata",
    "librerías La Plata",
    "cerrajerías La Plata",
    "talleres mecánicos La Plata",
    "inmobiliarias La Plata",
    "farmacias La Plata",
    "ópticas La Plata",
    "cafeterías La Plata",
    "restaurantes La Plata",

    "dietéticas Mar del Plata",
    "ferreterías Mar del Plata",
    "gimnasios Mar del Plata",
    "peluquerías Mar del Plata",
    "veterinarias Mar del Plata",
    "panaderías Mar del Plata",
    "librerías Mar del Plata",
    "cerrajerías Mar del Plata",
    "talleres mecánicos Mar del Plata",
    "inmobiliarias Mar del Plata",
    "farmacias Mar del Plata",
    "ópticas Mar del Plata",
    "cafeterías Mar del Plata",
    "restaurantes Mar del Plata",

    "dietéticas Tucumán",
    "ferreterías Tucumán",
    "gimnasios Tucumán",
    "peluquerías Tucumán",
    "veterinarias Tucumán",
    "panaderías Tucumán",
    "librerías Tucumán",
    "cerrajerías Tucumán",
    "talleres mecánicos Tucumán",
    "inmobiliarias Tucumán",
    "farmacias Tucumán",
    "ópticas Tucumán",
    "cafeterías Tucumán",
    "restaurantes Tucumán",

    "dietéticas Salta",
    "ferreterías Salta",
    "gimnasios Salta",
    "peluquerías Salta",
    "veterinarias Salta",
    "panaderías Salta",
    "librerías Salta",
    "cerrajerías Salta",
    "talleres mecánicos Salta",
    "inmobiliarias Salta",
    "farmacias Salta",
    "ópticas Salta",
    "cafeterías Salta",
    "restaurantes Salta",

    "dietéticas Santa Fe",
    "ferreterías Santa Fe",
    "gimnasios Santa Fe",
    "peluquerías Santa Fe",
    "veterinarias Santa Fe",
    "panaderías Santa Fe",
    "librerías Santa Fe",
    "cerrajerías Santa Fe",
    "talleres mecánicos Santa Fe",
    "inmobiliarias Santa Fe",
    "farmacias Santa Fe",
    "ópticas Santa Fe",
    "cafeterías Santa Fe",
    "restaurantes Santa Fe",

    "dietéticas Bahía Blanca",
    "ferreterías Bahía Blanca",
    "gimnasios Bahía Blanca",
    "peluquerías Bahía Blanca",
    "veterinarias Bahía Blanca",
    "panaderías Bahía Blanca",
    "librerías Bahía Blanca",
    "cerrajerías Bahía Blanca",
    "talleres mecánicos Bahía Blanca",
    "inmobiliarias Bahía Blanca",
    "farmacias Bahía Blanca",
    "ópticas Bahía Blanca",
    "cafeterías Bahía Blanca",
    "restaurantes Bahía Blanca"
]
# ──────────────────────────────────────────────────────────────────────────────


async def scrape_business_detail(page, url) -> dict:
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)

    data = {
        "nombre": "", "direccion": "", "telefono": "",
        "web": "", "instagram": "", "facebook": "", "maps_url": url,
    }

    try:
        data["nombre"] = await page.locator("h1").first.inner_text()
    except Exception:
        pass

    try:
        buttons = await page.locator("button[data-item-id]").all()
        for btn in buttons:
            item_id = await btn.get_attribute("data-item-id") or ""
            text = (await btn.inner_text()).strip()
            if "address" in item_id:
                data["direccion"] = text
            elif "phone" in item_id:
                data["telefono"] = text
    except Exception:
        pass

    try:
        web_link = page.locator("a[data-item-id*='authority']").first
        data["web"] = await web_link.get_attribute("href") or ""
    except Exception:
        pass

    try:
        html = await page.content()
        ig = re.search(r'https?://(?:www\.)?instagram\.com/[^\s"\'<>]+', html)
        fb = re.search(r'https?://(?:www\.)?facebook\.com/[^\s"\'<>]+', html)
        if ig:
            data["instagram"] = ig.group(0).rstrip("/")
        if fb:
            data["facebook"] = fb.group(0).rstrip("/")
    except Exception:
        pass

    return data


async def get_listing_urls(page, query: str, limit: int) -> list[str]:
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    await page.goto(search_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)

    urls = []
    seen = set()

    try:
        panel = page.locator("div[role='feed']").first
        for _ in range(15):
            links = await page.locator("a[href*='/maps/place/']").all()
            for link in links:
                href = await link.get_attribute("href")
                if href and href not in seen:
                    seen.add(href)
                    urls.append(href)
            if len(urls) >= limit:
                break
            await panel.evaluate("el => el.scrollBy(0, 1000)")
            await page.wait_for_timeout(1200)
    except Exception:
        pass

    return list(dict.fromkeys(urls))[:limit]


async def worker(worker_id: int, queue: asyncio.Queue, results: list, lock: asyncio.Lock, playwright):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        locale="es-AR",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()

    while True:
        try:
            q_idx, query = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        print(f"[worker-{worker_id}] 🔍 ({q_idx}/{len(QUERIES)}) {query}")
        try:
            urls = await get_listing_urls(page, query, LIMIT_PER_QUERY)
            print(f"[worker-{worker_id}] → {len(urls)} URLs")
            for i, url in enumerate(urls, 1):
                try:
                    data = await scrape_business_detail(page, url)
                    data["query"] = query
                    async with lock:
                        results.append(data)
                    print(f"[worker-{worker_id}]   [{i}] {data['nombre'] or '(sin nombre)'}")
                except Exception as e:
                    print(f"[worker-{worker_id}]   [{i}] Error: {e}")
        except Exception as e:
            print(f"[worker-{worker_id}] Error query '{query}': {e}")

        queue.task_done()

    await browser.close()


async def main():
    queue = asyncio.Queue()
    for idx, query in enumerate(QUERIES, 1):
        await queue.put((idx, query))

    results = []
    lock = asyncio.Lock()

    async with async_playwright() as p:
        tasks = [
            asyncio.create_task(worker(i + 1, queue, results, lock, p))
            for i in range(min(WORKERS, len(QUERIES)))
        ]
        await asyncio.gather(*tasks)

    df = pd.DataFrame(results)
    df.drop_duplicates(subset="nombre", inplace=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n✅ {len(df)} negocios únicos guardados en: {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
