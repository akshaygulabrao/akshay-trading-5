#!.venv/bin/python
from playwright.async_api import async_playwright
import msgpack
import asyncio

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://sportsbook.draftkings.com/live?category=live-in-game&subcategory=tennis")

        def handle_ws(ws):
            print("WebSocket opened:", ws.url)

            def on_message(msg):
                try:
                    # msg is bytes from the WebSocket
                    decoded = msgpack.unpackb(msg, raw=False)
                    print("Decoded msgpack:", decoded)
                except Exception as e:
                    print("Failed to decode msgpack:", e)

            ws.on("framereceived", on_message)

        page.on("websocket", handle_ws)

        # Keep the browser open forever
        await asyncio.Event().wait()

asyncio.run(main())
