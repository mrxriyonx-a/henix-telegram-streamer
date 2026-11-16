import os
import asyncio
from aiohttp import web
from pyrogram import Client

# =====================================
# HENIX TELEGRAM STREAMER
# =====================================

API_ID = 33799155
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHANNEL_ID = -1004310348370

CHUNK_SIZE = 1024 * 1024
PORT = int(os.environ.get("PORT", 10000))

# =====================================
# TELEGRAM CLIENT
# =====================================

app = Client(
    "henix_streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)


# =====================================
# HEALTH CHECK
# =====================================

async def home(request):
    return web.json_response({
        "status": "online",
        "name": "HENIX Telegram Streamer",
        "version": "1.0",
        "stream_endpoint": "/stream/{message_id}"
    })


# =====================================
# VIDEO STREAMING
# =====================================

async def stream_video(request):

    try:
        msg_id = int(request.match_info["msg_id"])

        # Get Telegram message
        message = await app.get_messages(
            CHANNEL_ID,
            msg_id
        )

        if not message or message.empty:
            return web.Response(
                text="File not found",
                status=404
            )

        # Detect media
        media = (
            message.video
            or message.document
            or message.audio
            or message.animation
        )

        if not media:
            return web.Response(
                text="Unsupported media",
                status=415
            )

        file_size = media.file_size
        mime_type = (
            media.mime_type
            or "application/octet-stream"
        )

        if not file_size or file_size <= 0:
            return web.Response(
                text="Invalid file size",
                status=500
            )

        # =================================
        # HTTP RANGE SUPPORT
        # =================================

        range_header = request.headers.get("Range")

        start = 0
        end = file_size - 1

        if range_header:

            try:
                unit, value = range_header.split("=", 1)

                if unit.strip() != "bytes" or "," in value:
                    raise ValueError()

                first, last = value.split("-", 1)

                if first:
                    start = int(first)
                    end = int(last) if last else file_size - 1
                else:
                    suffix = int(last)

                    if suffix <= 0:
                        raise ValueError()

                    start = max(0, file_size - suffix)

                if start >= file_size or start > end:
                    raise ValueError()

                end = min(end, file_size - 1)

            except (ValueError, TypeError):
                return web.Response(
                    status=416,
                    headers={
                        "Content-Range": f"bytes */{file_size}",
                        "Accept-Ranges": "bytes"
                    }
                )

        content_length = end - start + 1

        # =================================
        # STREAM RESPONSE
        # =================================

        headers = {
            "Content-Type": mime_type,
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Disposition": "inline",
            "Cache-Control": "no-cache"
        }

        status = 200

        if range_header:
            status = 206
            headers["Content-Range"] = (
                f"bytes {start}-{end}/{file_size}"
            )

        response = web.StreamResponse(
            status=status,
            headers=headers
        )

        await response.prepare(request)

        # =================================
        # TELEGRAM CHUNK STREAMING (FIXED)
        # =================================

        offset = start // CHUNK_SIZE
        skip = start % CHUNK_SIZE
        remaining = content_length

        async for chunk in app.stream_media(
            media,
            offset=offset
        ):

            if skip:
                chunk = chunk[skip:]
                skip = 0

            if not chunk:
                continue

            chunk = chunk[:remaining]

            await response.write(chunk)

            remaining -= len(chunk)

            if remaining <= 0:
                break

        if remaining > 0:
            raise RuntimeError(
                "Telegram stream ended before requested bytes were sent"
            )

        await response.write_eof()

        return response

    except asyncio.CancelledError:
        raise

    except (ConnectionResetError, BrokenPipeError):
        print("Client disconnected")

    except Exception as error:
        print("STREAM ERROR:", repr(error))

        if "response" in locals() and response.prepared:
            raise

        return web.Response(
            text="Streaming error",
            status=500
        )


# =====================================
# APPLICATION STARTUP
# =====================================

async def main():

    print("Starting HENIX Telegram Streamer...")

    await app.start()

    print("Telegram connected successfully!")

    web_app = web.Application()

    # Routes
    web_app.router.add_get("/", home)

    web_app.router.add_get(
        "/stream/{msg_id}",
        stream_video
    )

    # HTTP Server
    runner = web.AppRunner(web_app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    print("--------------------------------")
    print("HENIX TELEGRAM STREAMER ONLINE")
    print(f"PORT: {PORT}")
    print("--------------------------------")

    try:
        await asyncio.Event().wait()

    finally:

        print("Stopping HENIX...")

        await runner.cleanup()

        await app.stop()


# =====================================
# RUN APPLICATION
# =====================================

if __name__ == "__main__":
    asyncio.run(main())
