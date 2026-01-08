#!/usr/bin/env python3
import asyncio
import json
import re
import random
import time
import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from playwright.async_api import async_playwright, Error, TimeoutError

# -------- CONFIGURACIÓN FACTORIZADA --------
# Forzar salida inmediata de logs en GitHub Actions
sys.stdout.reconfigure(line_buffering=True)

CONFIG = {
    # URLs
    "BASE_URL": "https://www.avianca.com/es/",
    "BOOK_URL": "https://www.avianca.com/es/booking/select/",
    
    # User Agent
    "USER_AGENT": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),

    # Tiempos (ajustables via argumentos si se quisiera)
    "MIN_DELAY_BETWEEN_RESULTS_MS": 2000,
    "MAX_DELAY_BETWEEN_RESULTS_MS": 5000,
    "DEST_COOLDOWN_RANGE_MS": (11000, 18000),
    "LONG_PAUSE_EVERY": 15,
    "LONG_PAUSE_SECONDS": 40,
    "DATE_COOLDOWN_SECONDS": 10,
    "CAPTCHA_BACKOFF_SECONDS": 180,
    "CAPTCHA_BOOST_SEARCHES": 20,
    "CAPTCHA_DEST_COOLDOWN_RANGE_MS": (30000, 60000),
    
    # Rutas
    "PROGRESS_PATH": Path("avianca_scrape_progress.json"),
    "LOCAL_PARQUET_PATH": Path("avianca_busquedas_local.parquet"),
    
    # Reintentos
    "MAX_NAVIGATION_RETRIES": 3,
    "NAVIGATION_RETRY_DELAY_SECONDS": 45,
    
    # Configuración de búsqueda por defecto
    "ORIGIN": "BOG",
    "DESTINOS": [
        "ADZ", "AEP", "AUA", "AUC", "ASU", "AXM", "BGA", "BAQ", "BCN", "BOS",
        "CDG", "CLO", "CCS", "CTG", "CUC", "CUN", "CUR", "CUZ", "DFW", "EJA",
        "EYP", "EZE", "FLL", "GEO", "GIG", "GRU", "GUA", "GYE", "HAV", "IAD",
        "IBE", "IPI", "JFK", "LET", "LHR", "LIM", "LPB", "MAD", "MAO", "MCO",
        "MDE", "MEX", "MIA", "MTR", "MVD", "NVA", "ORD", "PEI", "PPN", "PSO",
        "PTY", "PUJ", "RCH", "SAL", "SCL", "SDQ", "SJO", "SJU", "SMR", "TQO",
        "UIB", "UIO", "VUP", "VVC", "VVI", "XPL", "YUL", "YYZ", "TPA", "BSB"
    ],
    "SHUFFLE_DESTINOS": False
}

# -------- HELPERS --------

def daterange(start_date, end_date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)


def normalize_ts(s):
    try:
        s = s.replace("Z", "")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def extract_from_journeys(payload_json):
    """Extrae dicts normalizados de la respuesta de journeys/schedules."""
    rows = []
    journey_price_responses = payload_json.get("journeyPriceResponses", [])
    if not journey_price_responses:
         return rows

    for jpr in journey_price_responses:
        for sch in jpr.get("schedules", []):
            sch_date = sch.get("date")
            availability = sch.get("availability")
            for jy in sch.get("journeys", []):
                origin = jy.get("origin")
                dest = jy.get("destination")
                fares = jy.get("fares", [])
                total_amt = fares[0].get("totalAmount") if fares else None
                available_seats = fares[0].get("availableSeats") if fares else None

                segments = jy.get("segments", [])
                stops = max(0, len(segments) - 1)
                for seg in segments:
                    tr = seg.get("transport", {}) or {}
                    rows.append(
                        {
                            "fecha_programada": sch_date,
                            "origen": origin,
                            "destino": dest,
                            "num_vuelo": tr.get("number"),
                            "matricula": tr.get("registration"),
                            "capacidad": tr.get("capacity"),
                            "std": seg.get("std"),
                            "sta": seg.get("sta"),
                            "duracion": seg.get("duration"),
                            "precio_total": total_amt,
                            "sillas_disponibles": available_seats,
                            "stops": stops,
                            "availability": availability,
                            "raw_cabin": jy.get("cabin"),
                        }
                    )
    return rows


def load_progress(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_progress(progress, path):
    path.write_text(json.dumps(progress, indent=2, ensure_ascii=False))


def write_checkpoint_parquet(rows, path):
    if not rows:
        return
    df_new = pd.DataFrame(rows)
    if df_new.empty:
        return

    # Normalización de columnas clave para evitar tipos mixtos
    for col in ["std", "sta", "fecha_programada"]:
        if col in df_new.columns:
            df_new[col] = df_new[col].astype(str)

    if path.exists():
        try:
            df_existing = pd.read_parquet(path)
            # Asegurar consistencia en tipos del existente también
            for col in ["std", "sta", "fecha_programada"]:
                if col in df_existing.columns:
                    df_existing[col] = df_existing[col].astype(str)
            
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        except Exception as e:
            print(f"Error leyendo parquet existente: {e}. Creando uno nuevo.")
            df_combined = df_new
    else:
        df_combined = df_new

    df_combined = df_combined.drop_duplicates(
        subset=[
            "num_vuelo",
            "origen",
            "destino",
            "std",
            "sta",
            "fecha_programada",
        ]
    )
    
    try:
        df_combined.to_parquet(path, index=False)
        print(f"Checkpoint parquet actualizado ({len(df_combined)} filas totales)")
    except Exception as exc:
        print(f"Advertencia: no se pudo actualizar el parquet: {exc}")


async def goto_with_retry(page, url, wait_until="load", timeout=60000, max_attempts=4):
    """Intenta cargar una URL varias veces degradando espera y desactivando HTTP/2."""
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        current_wait = wait_until if attempt == 1 else "domcontentloaded"
        try:
            return await page.goto(url, wait_until=current_wait, timeout=timeout)
        except Error as exc:
            last_exc = exc
            if (
                ("ERR_HTTP2_PROTOCOL_ERROR" in str(exc) or "INTERNAL_ERROR" in str(exc))
                and attempt < max_attempts
            ):
                await page.wait_for_timeout(3000)
                continue
            # A veces ayuda un reload o simplemente reintentar
            if attempt < max_attempts:
                print(f"Error cargando {url}: {exc}. Reintentando ({attempt}/{max_attempts})...")
                await page.wait_for_timeout(3000)
                continue
            raise
        except TimeoutError as exc:
            last_exc = exc
            if attempt < max_attempts:
                print(f"Timeout cargando {url}. Reintentando ({attempt}/{max_attempts})...")
                await page.wait_for_timeout(3000)
                continue
            raise
    if last_exc:
        raise last_exc

def build_search_url(origin, dest, date_out, ida_vuelta=False, date_back=None):
    from urllib.parse import urlencode

    params = {
        "origin1": origin,
        "destination1": dest,
        "departure1": date_out.strftime("%Y-%m-%d"),
        "adt1": 1,
        "tng1": 0,
        "chd1": 0,
        "inf1": 0,
        "currency": "COP",
        "posCode": "CO",
        "tripType": "R" if ida_vuelta and date_back else "O",
        "lang": "es",
        "searchType": "normal",
    }
    if ida_vuelta and date_back:
        params.update(
            {
                "origin2": dest,
                "destination2": origin,
                "departure2": date_back.strftime("%Y-%m-%d"),
                "adt2": 1,
                "tng2": 0,
                "chd2": 0,
                "inf2": 0,
            }
        )
    return f"{CONFIG['BOOK_URL']}?{urlencode(params)}"

async def wait_if_verification(page, timeout_minutes=5):
    """Detecta verificación/captcha y espera (si no es headless) o reporta."""
    try:
        # Check rápido sin esperar mucho
        if await page.locator("text=Verificación necesaria").count() > 0 or \
           await page.locator("text=No soy un robot").count() > 0:
            
            print("[!] Se detectó verificación (captcha).")
            # En entorno headless/cloud no podemos resolverlo manualmente fácil.
            # Podríamos intentar esperar un poco por si es un check automático.
            try:
                await page.wait_for_function(
                    "() => !document.body || (!document.body.innerText.includes('Verificación necesaria') && !document.body.innerText.includes('No soy un robot'))",
                    timeout=timeout_minutes * 60 * 1000,
                )
                print("[ok] Verificación superada.")
                return True
            except TimeoutError:
                print(f"[!] Timeout esperando resolución de captcha ({timeout_minutes} min).")
                return True # Asumimos que hubo captcha y falló o timeout
        return False
    except Exception:
        return False

async def do_search(page, origin, dest, date_out, ida_vuelta=False, date_back=None):
    target_url = build_search_url(origin, dest, date_out, ida_vuelta=ida_vuelta, date_back=date_back)
    
    for nav_attempt in range(1, CONFIG["MAX_NAVIGATION_RETRIES"] + 1):
        try:
            await goto_with_retry(page, target_url, wait_until="domcontentloaded", timeout=60000)
            break
        except Error as exc:
            msg = str(exc)
            if "ERR_ABORTED" in msg or "105.1" in msg:
                print(f"Sin disponibilidad o error de carga: {origin}->{dest} {date_out} (aviso 105.1)")
                return False
            print(f"Error navegando: {msg}")
            if nav_attempt >= CONFIG["MAX_NAVIGATION_RETRIES"]:
                 return False
            await page.wait_for_timeout(CONFIG["NAVIGATION_RETRY_DELAY_SECONDS"] * 1000)
        except TimeoutError as exc:
            print(f"Timeout navegando {origin}->{dest} {date_out} (intento {nav_attempt})")
            if nav_attempt >= CONFIG["MAX_NAVIGATION_RETRIES"]:
                return False
            await page.wait_for_timeout(CONFIG["NAVIGATION_RETRY_DELAY_SECONDS"] * 1000)
    
    # Intentar cerrar cookies si aparecen, sin bloquear
    try:
        accept_btn = page.get_by_role("button", name=re.compile("Aceptar|Accept", re.I))
        if await accept_btn.count() > 0:
             await accept_btn.click(timeout=1000)
    except Exception:
        pass

    captcha_seen = await wait_if_verification(page)
    # Si hubo captcha, tal vez queramos recargar, pero por simplicidad retornamos el estado
    # En el loop principal se maneja el backoff
    
    # Pequeña espera para asegurar que lleguen respuestas XHR
    await page.wait_for_timeout(2500)
    return captcha_seen


async def scrape_flights(args):
    origin = args.origin
    destinos = args.destinos or CONFIG["DESTINOS"]
    start_date = args.start_date
    end_date = args.end_date
    ida_vuelta = False # Por ahora hardcoded en False, soportar True requiere más lógica en argumentos

    if args.test:
        print("--- MODO TEST ACTIVADO: Limitando a 2 destinos y rango corto ---")
        destinos = destinos[:2]
        
    print(f"Iniciando scrape: {origin} -> {len(destinos)} destinos. Fechas: {start_date} a {end_date}")

    # Cargar progreso
    progress = load_progress(CONFIG["PROGRESS_PATH"])
    all_rows = []

    # Configuración de Playwright
    async with async_playwright() as p:
        # Optimizaciones de argumentos
        browser_args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--disable-gpu",
        ]
        
        # Lanzar navegador (Firefox por evasión)
        browser = await p.firefox.launch(
            headless=args.headless,
            args=browser_args
        )
        
        context = await browser.new_context(
            user_agent=CONFIG["USER_AGENT"],
            viewport={"width": 1280, "height": 720}
        )

        # Bloqueo de recursos para velocidad (COMENTADO PORQUE PUEDE ROMPER AVIANCA)
        # await context.route("**/*", lambda route: asyncio.create_task(route.abort()) 
        #                     if route.request.resource_type in ["image", "media", "font", "stylesheet"] 
        #                     else asyncio.create_task(route.continue_()))

        # Forzar cierre de conexiones para evitar hanging
        async def _connection_close(route):
            headers = route.request.headers
            headers["Connection"] = "close"
            try:
                await route.continue_(headers=headers)
            except Exception:
                pass # A veces la request ya finalizó

        # Aplicar el connection close a todo lo demás
        # await context.route("**/*", _connection_close) # A veces conflictua con el route anterior, simplificamos

        page = await context.new_page()

        # Listener de respuestas
        captured = []
        async def handle_response(resp):
            try:
                # Filtrar solo lo que parece JSON útil de avianca (COMENTADO PARA MAYOR SEGURIDAD)
                # if "journeyPrice" not in resp.url and "availability" not in resp.url:
                #      return # Optimización simple
                
                if resp.status != 200:
                    return
                
                # Intentar parsear JSON
                try:
                    data = await resp.json()
                except Exception:
                    return

                if not isinstance(data, dict):
                    return
                
                # Verificar estructura clave
                if "journeyPriceResponses" in data:
                    rows = extract_from_journeys(data)
                    if rows:
                        captured.extend(rows)
            except Exception:
                pass

        page.on("response", lambda resp: asyncio.create_task(handle_response(resp)))

        # Ir a home para setear cookies/localización
        print("Cargando página de inicio para establecer sesión...")
        try:
            await goto_with_retry(page, CONFIG["BASE_URL"] + "?poscode=CO", wait_until="load")
            await page.wait_for_timeout(2000)
        except Exception as e:
            print(f"Advertencia al cargar home: {e}")

        # Generar fechas
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        dates = list(daterange(start_dt, end_dt))

        captcha_boost_remaining = 0

        # Loop principal
        for d in dates:
            date_iso = d.isoformat()
            destino_sequence = list(destinos)
            if CONFIG["SHUFFLE_DESTINOS"]:
                random.shuffle(destino_sequence)

            print(f"===== Procesando Fecha {date_iso} =====")
            
            for dest in destino_sequence:
                # Chequear progreso
                if progress.get(date_iso, {}).get(dest) is not None:
                    # Ya hecho
                    continue
                
                captured.clear()
                
                # Intentos
                attempts = 0
                while attempts <= CONFIG["MAX_NAVIGATION_RETRIES"]:
                    captcha_seen = await do_search(page, origin, dest, d, ida_vuelta=ida_vuelta)
                    
                    if captcha_seen:
                         captcha_boost_remaining = CONFIG["CAPTCHA_BOOST_SEARCHES"]

                    # Esperar resultados
                    # Avianca carga dinámicamente, esperamos un poco random
                    wait_ms = random.randint(CONFIG["MIN_DELAY_BETWEEN_RESULTS_MS"], CONFIG["MAX_DELAY_BETWEEN_RESULTS_MS"])
                    await page.wait_for_timeout(wait_ms)
                    
                    if captured:
                        print(f"[OK] {origin}->{dest} {date_iso}: {len(captured)} vuelos.")
                        break
                    else:
                        attempts += 1
                        print(f"[RETRY] {origin}->{dest} {date_iso}: Intento {attempts}/{CONFIG['MAX_NAVIGATION_RETRIES']} sin datos.")
                        if attempts <= CONFIG["MAX_NAVIGATION_RETRIES"]:
                             await page.wait_for_timeout(2000 * attempts)
                
                # Guardar resultados
                if captured:
                    batch = captured.copy()
                    for r in batch:
                        r["origen_solicitado"] = origin
                        r["destino_solicitado"] = dest
                        r["fecha_busqueda"] = date_iso
                        r["std_dt"] = normalize_ts(r.get("std") or "")
                        r["sta_dt"] = normalize_ts(r.get("sta") or "")
                        r["fecha_programada_dt"] = normalize_ts(r.get("fecha_programada") or "")
                    
                    all_rows.extend(batch)
                    progress.setdefault(date_iso, {})[dest] = len(batch)
                else:
                    # Marcar como 0 para no reintentar infinitamente si no hay vuelos
                    print(f"[EMPTY] {origin}->{dest} {date_iso}: No se encontraron vuelos.")
                    progress.setdefault(date_iso, {})[dest] = 0

                # Guardar checkpoint
                save_progress(progress, CONFIG["PROGRESS_PATH"])
                write_checkpoint_parquet(all_rows, CONFIG["LOCAL_PARQUET_PATH"])
                all_rows.clear() # Limpiar memoria, ya está en disco

                # Cooldowns
                if captcha_boost_remaining > 0:
                    cd = random.randint(*CONFIG["CAPTCHA_DEST_COOLDOWN_RANGE_MS"])
                    captcha_boost_remaining -= 1
                    print(f"Cooldown extendido (captcha): {cd/1000:.1f}s")
                else:
                    cd = random.randint(*CONFIG["DEST_COOLDOWN_RANGE_MS"])
                    print(f"Cooldown: {cd/1000:.1f}s")
                
                await page.wait_for_timeout(cd)

            # Pausa entre fechas
            if CONFIG["DATE_COOLDOWN_SECONDS"]:
                 print(f"Descanso de fecha: {CONFIG['DATE_COOLDOWN_SECONDS']}s")
                 await page.wait_for_timeout(CONFIG["DATE_COOLDOWN_SECONDS"] * 1000)

        await browser.close()

def main():
    parser = argparse.ArgumentParser(description="Avianca Flight Scraper")
    parser.add_argument("--origin", default=CONFIG["ORIGIN"], help="Código origen (e.g. BOG)")
    parser.add_argument("--destinos", nargs="+", help="Lista de códigos destino (e.g. MDE CLO)")
    # Default fechas: mañana hasta +60 días
    default_start = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    default_end = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")
    parser.add_argument("--start-date", default=default_start, help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--end-date", default=default_end, help="Fecha fin YYYY-MM-DD")
    parser.add_argument("--headless", action="store_true", help="Ejecutar sin ventana gráfica")
    parser.add_argument("--test", action="store_true", help="Modo prueba (menos destinos, rango corto)")
    
    args = parser.parse_args()
    
    asyncio.run(scrape_flights(args))

if __name__ == "__main__":
    main()
