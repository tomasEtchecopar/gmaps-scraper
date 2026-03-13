# gmaps-scraper

Scraper de Google Maps para extraer datos de negocios sin API key. Genera un CSV listo para outreach con nombre, dirección, teléfono, sitio web, Instagram y Facebook.

## Scripts

| Archivo | Descripción |
|---|---|
| `gmaps_scraper.py` | Una sola query por ejecución |
| `gmaps_scraper_multi.py` | Múltiples queries en paralelo |

## Requisitos

```bash
pip install playwright pandas
playwright install chromium
```

## Uso

### Query simple
```bash
python script_uni.py "restaurantes Mar del Plata" 30
```

### Múltiples queries en paralelo
Editá la lista `QUERIES` y la variable `WORKERS` en `gmaps_scraper_multi.py`:

```python
WORKERS = 5  # browsers en paralelo

QUERIES = [
    "dietéticas Buenos Aires",
    "ferreterías Córdoba",
    "gimnasios Rosario",
    # ...
]
```

Luego:
```bash
python script_multi.py
```

## Output

Ambos scripts generan un archivo `.csv` con las siguientes columnas:

| Columna | Descripción |
|---|---|
| `nombre` | Nombre del negocio |
| `direccion` | Dirección física |
| `telefono` | Teléfono |
| `web` | Sitio web |
| `instagram` | Perfil de Instagram (si está disponible) |
| `facebook` | Perfil de Facebook (si está disponible) |
| `maps_url` | URL del perfil en Google Maps |
| `query` | Query que originó el resultado *(solo multi)* |

## Configuración

| Variable | Default | Descripción |
|---|---|---|
| `LIMIT_PER_QUERY` | `10` | Resultados por búsqueda |
| `WORKERS` | `5` | Browsers en paralelo *(solo multi)* |
| `OUTPUT_FILE` | `pymes_argentina.csv` | Nombre del archivo de salida |

## Notas

- Instagram y Facebook solo aparecen si Google Maps los tiene linkeados en el perfil del negocio.
- Si Google devuelve captcha, bajá `WORKERS` o aumentá los tiempos de espera (`wait_for_timeout`).
- Con 130 queries y `WORKERS = 5` el tiempo estimado es ~20-30 minutos.
