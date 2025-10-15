import os, json, time
from datetime import datetime
from websocket import WebSocketApp
from kafka import KafkaProducer

# Configuración
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "10.150.0.2:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_stream")

# Producer de Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=5
)

# URL de Binance MiniTicker
BINANCE_WSS_URL = "wss://stream.binance.us:9443/ws/!miniTicker@arr"

# Para mostrar estadísticas
PRINT_INTERVAL_SEC = 0.25
_last_print_time = 0.0
_events_count = 0

def now():
    return f"[{datetime.utcnow().isoformat()}Z]"

def normalize_event(payload):
    """
    Normaliza el payload del MiniTicker para evitar duplicados en BQ y nombres consistentes.
    Convierte keys a minúsculas y renombra campos a nombres claros.
    """
    normalized = []
    for item in payload:
        symbol = item.get('s')
        close = item.get('c')
        raw = {}
        mapping = {
            'q': 'quantity',
            'v': 'volume',
            'h': 'high',
            'l': 'low',
            'o': 'open',
            'e': 'event_time'
        }
        for k, v in item.items():
            if k not in ['s','c']:
                raw[mapping.get(k, k.lower())] = v
        normalized.append({
            "symbol": symbol,
            "close": close,
            "raw": raw
        })
    return normalized

# Callbacks del WebSocket
def on_open(ws):
    print(f"{now()} ✅ Conectado -> {BINANCE_WSS_URL}", flush=True)

def on_message(ws, message):
    global _last_print_time, _events_count
    _events_count += 1
    timestamp_ms = int(time.time() * 1000)

    try:
        payload = json.loads(message)
        normalized_payload = normalize_event(payload)
    except Exception:
        normalized_payload = [{"raw": message}]

    event = {
        "t_ms": timestamp_ms,
        "payload": normalized_payload
    }

    try:
        producer.send(KAFKA_TOPIC, event)
    except Exception as e:
        print(f"{now()} ❌ Error enviando a Kafka: {e}", flush=True)

    # Estadísticas
    t = time.time()
    if t - _last_print_time >= PRINT_INTERVAL_SEC:
        eps = _events_count / max(1e-9, t - _last_print_time)
        _last_print_time = t
        _events_count = 0
        preview = ", ".join(f"{x['symbol']}:{x['close']}" for x in normalized_payload[:3])
        print(f"{now()} ~{eps:.0f} evt/s  |  {preview}", flush=True)

def on_error(ws, error):
    print(f"{now()} ❌ Error: {error}", flush=True)

def on_close(ws, code, msg):
    print(f"{now()} 🔌 Cerrado (code={code}, msg={msg})", flush=True)

def run_forever():
    backoff = 1
    while True:
        try:
            ws = WebSocketApp(
                BINANCE_WSS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print(f"{now()} 🛑 Interrumpido por el usuario")
            break
        except Exception as e:
            print(f"{now()} ⚠️ Excepción externa: {e}", flush=True)
        print(f"{now()} Reintentando en {backoff}s ...", flush=True)
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)

if __name__ == "__main__":
    print(f"{now()} Conectando y publicando a Kafka broker {KAFKA_BROKER} -> topic {KAFKA_TOPIC}", flush=True)
    run_forever()
