import asyncio
import json
import os
import re
import time

import aiohttp

WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]  # set as an environment variable, never hardcode
INTERVAL = 18          # seconds between checks of the same channel
MISS_LIMIT = 2         # consecutive "offline" results needed before announcing a stream ended
STATE_FILE = "state.json"

TIKTOK = [
    "riosarttt", "jiinkiii", "chuudddd", "toeji55", "insanjiii", "alexfwi",
    "lwkhlgh", "uveals", "dznieiie", "alanisgoodsniper123", "sprainfps", "7cyph",
]
YOUTUBE = [
    "7cyph", "swiftonrivals", "fwqzrivals", "speahr",
    "tripledFPS", "SubToIced", "SlingshotBwaii", "elixirblx",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


async def check_tiktok(session, name):
    """Return {"live": bool, "url": str, "title": str|None}, or None if the check failed."""
    async with session.get(f"https://www.tiktok.com/@{name}") as r:
        if r.status != 200:
            return None
        text = await r.text()
    m = re.search(
        r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>', text, re.S
    )
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
        user = data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]["user"]
    except (KeyError, json.JSONDecodeError):
        return None
    room_id = str(user.get("roomId") or "")
    return {
        "live": room_id not in ("", "0"),
        "url": f"https://www.tiktok.com/@{name}/live",
        "title": None,
    }


async def check_youtube(session, handle):
    """Same return shape as check_tiktok."""
    headers = {**HEADERS, "Cookie": "CONSENT=YES+1; SOCS=CAI"}
    async with session.get(f"https://www.youtube.com/@{handle}/live", headers=headers) as r:
        if r.status != 200:
            return None
        text = await r.text()
    canonical = re.search(r'<link rel="canonical" href="([^"]+)"', text)
    if not canonical:
        return None
    live = "/watch?v=" in canonical.group(1) and '"isLiveNow":true' in text
    title = re.search(r'<meta property="og:title" content="([^"]*)"', text)
    return {
        "live": live,
        "url": canonical.group(1) if live else f"https://www.youtube.com/@{handle}",
        "title": title.group(1) if (live and title) else None,
    }


async def send_discord(session, platform, name, went_live, info):
    embed = {
        "title": f"{name} is LIVE on {platform}" if went_live else f"{name} ended their {platform} stream",
        "url": info["url"],
        "color": 0x2ECC71 if went_live else 0xE74C3C,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if went_live and info.get("title"):
        embed["description"] = info["title"]
    for _ in range(3):
        async with session.post(WEBHOOK_URL, json={"embeds": [embed]}) as r:
            if r.status == 429:  # rate limited: wait as long as Discord says, then retry
                retry = (await r.json()).get("retry_after", 2)
                await asyncio.sleep(retry)
                continue
            return


async def watch(session, state, platform, name, start_delay):
    """Endless loop for ONE channel. Staggered start keeps requests spread out."""
    checker = check_tiktok if platform == "TikTok" else check_youtube
    key = f"{platform}:{name}"
    await asyncio.sleep(start_delay)
    while True:
        try:
            info = await checker(session, name)
        except Exception as e:  # network hiccup etc: skip this round, keep state unchanged
            print(f"[{key}] check failed: {e}")
            info = None

        if info is not None:
            if key not in state:  # first time seeing this channel this run: record status, don't announce
                state[key] = {"live": info["live"], "misses": 0}
                save_state(state)
                await asyncio.sleep(INTERVAL)
                continue
            s = state[key]
            if info["live"]:
                s["misses"] = 0
                if not s["live"]:
                    s["live"] = True
                    save_state(state)
                    await send_discord(session, platform, name, True, info)
            elif s["live"]:
                s["misses"] += 1
                if s["misses"] >= MISS_LIMIT:
                    s["live"] = False
                    s["misses"] = 0
                    save_state(state)
                    await send_discord(session, platform, name, False, info)
        await asyncio.sleep(INTERVAL)


async def main():
    state = load_state()
    channels = [("TikTok", n) for n in TIKTOK] + [("YouTube", n) for n in YOUTUBE]
    spacing = INTERVAL / len(channels)
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        run = asyncio.gather(
            *(watch(session, state, p, n, i * spacing) for i, (p, n) in enumerate(channels))
        )
        limit = int(os.environ.get("RUN_SECONDS", "0"))  # 0 = run forever
        try:
            await asyncio.wait_for(run, limit or None)
        except asyncio.TimeoutError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
