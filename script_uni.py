"""
Google Maps Scraper - Sin API key
Extrae: nombre, dirección, teléfono, web, redes sociales

Uso:
  pip install playwright pandas
  playwright install chromium
  python gmaps_scraper.py "restaurantes Mar del Plata" 30
"""

import asyncio
import sys
import re
import json
import pandas as pd
from playwright.async_api import async_playwright

QUERY = sys.argv[1] if len(sys.argv) > 1 else "restaurantes Mar del Plata"
LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else 30


async def scrape_business_detail(page, url) -> dict:
    """Abre el detalle de un negocio y extrae todos los campos."""
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)

    data = {
        "nombre": "",
        "direccion": "",
        "telefono": "",
        "web": "",
        "instagram": "",
        "facebook": "",
        "maps_url": url,
    }

    # Nombre
    try:
        data["nombre"] = await page.locator("h1").first.inner_text()
    except Exception:
        pass

    # Dirección, teléfono, web — están en botones con aria-label
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

    # Web — link con data-item-id que contiene "authority"
    try:
        web_link = page.locator("a[data-item-id*='authority']").first
        data["web"] = await web_link.get_attribute("href") or ""
    except Exception:
        pass

    # Redes sociales — busca links de instagram/facebook en toda la página
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
    """Busca en Google Maps y devuelve URLs de negocios."""
    search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    await page.goto(search_url, wait_until="domcontentloaded")
    await page.wait_for_timeout(3000)

    urls = []
    seen = set()

    # Scroll en el panel lateral para cargar más resultados
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
        await page.wait_for_timeout(1500)

    return list(dict.fromkeys(urls))[:limit]  # dedup + limit


async def main():
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="es-AR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        print(f"🔍 Buscando: {QUERY} (límite: {LIMIT})")
        urls = await get_listing_urls(page, QUERY, LIMIT)
        print(f"✅ {len(urls)} negocios encontrados, extrayendo detalles...\n")

        for i, url in enumerate(urls, 1):
            try:
                data = await scrape_business_detail(page, url)
                results.append(data)
                print(f"[{i}/{len(urls)}] {data['nombre'] or '(sin nombre)'} — {data['telefono'] or 'sin tel'}")
            except Exception as e:
                print(f"[{i}/{len(urls)}] Error: {e}")

        await browser.close()

    # Exportar a CSV
    df = pd.DataFrame(results)
    output_file = "negocios_outreach.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"\n📁 Guardado en: {output_file}")
    print(df[["nombre", "telefono", "web"]].to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
