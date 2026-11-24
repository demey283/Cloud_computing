"""Weerstation-generator: simuleert een weerstation en publiceert de metingen via MQTT.

Topics (prefix instelbaar met MQTT_TOPIC_PREFIX, standaard "weer"):
  weer/temperatuur       {"station": "...", "waarde": 14.2, "eenheid": "°C", "ts": "...Z"}
  weer/luchtvochtigheid  {"station": "...", "waarde": 71, "eenheid": "%", "ts": "...Z"}
  weer/wind              {"station": "...", "snelheid": 18.5, "richting": 240, "eenheid": "km/h", "ts": "...Z"}
  weer/regen             {"station": "...", "mm_per_uur": 0.4, "ts": "...Z"}
  weer/status            "online" / "offline" (retained, offline via Last Will)

De waarden volgen een dag-nachtcyclus (warmst rond 15 u, koudst rond 4 u) zodat de
gemiddelden over 1 u en 24 u zinvol zijn. Met kans FOUT_KANS wordt een bewust foute
meting verstuurd (buiten bereik, verkeerd type of ontbrekend veld), zodat de function
node in Node-RED iets te valideren heeft.
"""
import json
import math
import os
import random
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt

BROKER = os.getenv("MQTT_HOST", "mqtt5")
PORT = int(os.getenv("MQTT_PORT", "1883"))
PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "weer")
STATION = os.getenv("STATION_ID", "kortrijk-01")
INTERVAL = float(os.getenv("INTERVAL", "5"))           # seconden tussen metingen
FOUT_KANS = float(os.getenv("FOUT_KANS", "0.05"))      # 0..1, per bericht
TIJDZONE = ZoneInfo(os.getenv("STATION_TZ", "Europe/Brussels"))
GEM_TEMP = float(os.getenv("GEM_TEMP", "10"))          # daggemiddelde in °C

TOPIC_STATUS = f"{PREFIX}/status"
verbonden = threading.Event()


def nu_utc():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def begrens(waarde, laag, hoog):
    return max(laag, min(hoog, waarde))


class Weer:
    """Houdt de toestand van het weer bij en laat die bij elke stap realistisch evolueren."""

    def __init__(self):
        self.drift = 0.0            # trage afwijking van de normale temperatuur (fronten)
        self.wind = 12.0            # km/h
        self.richting = 225.0       # graden, zuidwest is typisch voor België
        self.regent = False
        self.regen = 0.0            # mm/u
        self.vocht = 75.0

    def stap(self):
        lokaal = datetime.now(TIJDZONE)
        uur = lokaal.hour + lokaal.minute / 60

        # Temperatuur: sinus met piek om 15 u, plus trage drift en wat ruis.
        self.drift = begrens(self.drift + random.gauss(0, 0.05), -4, 4)
        dagcyclus = 5 * math.sin(2 * math.pi * (uur - 9) / 24)
        afkoeling = -1.5 if self.regent else 0
        temp = GEM_TEMP + dagcyclus + self.drift + afkoeling + random.gauss(0, 0.15)

        # Regen: af en toe een bui die een tijdje aanhoudt.
        if self.regent:
            if random.random() < 0.03:
                self.regent = False
        elif random.random() < 0.01:
            self.regent = True
        if self.regent:
            self.regen = begrens(self.regen + random.gauss(0, 0.4), 0.2, 12)
        else:
            self.regen = 0.0

        # Luchtvochtigheid: hoger als het koud is en als het regent.
        doel_vocht = 80 - 2.5 * (temp - GEM_TEMP) + (15 if self.regent else 0)
        self.vocht += 0.2 * (doel_vocht - self.vocht) + random.gauss(0, 1)
        self.vocht = begrens(self.vocht, 25, 100)

        # Wind: random walk met af en toe een windstoot, richting draait langzaam.
        self.wind = begrens(self.wind + random.gauss(0, 1.5) + 0.05 * (12 - self.wind), 0, 90)
        stoot = random.expovariate(1 / 8) if random.random() < 0.1 else 0
        self.richting = (self.richting + random.gauss(0, 6)) % 360

        return {
            "temperatuur": {"waarde": round(temp, 1), "eenheid": "°C"},
            "luchtvochtigheid": {"waarde": round(self.vocht), "eenheid": "%"},
            "wind": {"snelheid": round(self.wind + stoot, 1), "richting": round(self.richting), "eenheid": "km/h"},
            "regen": {"mm_per_uur": round(self.regen, 1)},
        }


# Per topic: welke velden een fout kunnen krijgen en welke waarden onmogelijk zijn.
ONGELDIG = {
    "temperatuur": {"waarde": (-999, 85.0)},
    "luchtvochtigheid": {"waarde": (-5, 140)},
    "wind": {"snelheid": (-10, 999), "richting": (-1, 400)},
    "regen": {"mm_per_uur": (-3, 999)},
}


# Per topic: welk veld een onrealistische sprong kan maken en hoe groot die sprong is.
# De waarde blijft binnen het geldige bereik; enkel een spike-filter kan dit herkennen.
SPRONG = {
    "temperatuur": ("waarde", 12),
    "luchtvochtigheid": ("waarde", 40),
    "wind": ("snelheid", 50),
    "regen": ("mm_per_uur", 20),
}


def maak_fout(meting, soort_topic):
    """Vervalst een geldige meting op een willekeurige manier."""
    veld = random.choice(list(ONGELDIG[soort_topic]))
    soort = random.choice(("buiten_bereik", "verkeerd_type", "ontbrekend", "sprong"))
    if soort == "sprong":
        veld, delta = SPRONG[soort_topic]
        waarde = meting[veld]
        if soort_topic == "luchtvochtigheid" and waarde > 50:
            delta = -delta  # blijf binnen 0..100 %
        meting[veld] = round(waarde + delta, 1)
    elif soort == "buiten_bereik":
        meting[veld] = random.choice(ONGELDIG[soort_topic][veld])
    elif soort == "verkeerd_type":
        meting[veld] = random.choice(("abc", None, "NaN"))
    else:
        meting.pop(veld, None)
    return f"{soort} ({veld})"


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Verbinding geweigerd door {BROKER}:{PORT} ({reason_code})")
        return
    print(f"Verbonden met {BROKER}:{PORT} als weerstation {STATION}")
    client.publish(TOPIC_STATUS, "online", qos=1, retain=True)
    verbonden.set()


def on_disconnect(client, userdata, flags, reason_code, properties):
    verbonden.clear()
    print(f"Verbinding verbroken ({reason_code}), paho probeert opnieuw...")


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"weerstation-{STATION}")
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.will_set(TOPIC_STATUS, "offline", qos=1, retain=True)
    client.reconnect_delay_set(min_delay=1, max_delay=10)

    while True:
        try:
            client.connect(BROKER, PORT, keepalive=30)
            break
        except OSError as e:
            print(f"Broker nog niet bereikbaar ({e}), opnieuw over 2 s...")
            time.sleep(2)

    client.loop_start()
    verbonden.wait()  # pas beginnen publiceren als de verbinding er is
    weer = Weer()

    while True:
        if verbonden.is_set():
            ts = nu_utc()
            for soort_topic, waarden in weer.stap().items():
                meting = {"station": STATION, **waarden, "ts": ts}
                label = ""
                if random.random() < FOUT_KANS:
                    label = f"  <-- FOUT: {maak_fout(meting, soort_topic)}"
                topic = f"{PREFIX}/{soort_topic}"
                payload = json.dumps(meting, ensure_ascii=False)
                client.publish(topic, payload)
                print(f"{topic} {payload}{label}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
