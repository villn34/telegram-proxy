import asyncio, socket, struct, os

async def relay(r, w):
    try:
        while True:
            d = await r.read(65536)
            if not d: break
            w.write(d); await w.drain()
    except Exception: pass
    finally:
        try: w.close()
        except Exception: pass

async def handle(reader, writer):
    try:
        hdr = await reader.readexactly(2)
        if hdr[0] != 5: return
        n = hdr[1]
        await reader.readexactly(n)
        writer.write(b"\x05\x00"); await writer.drain()
        req = await reader.readexactly(4)
        if req[0] != 5 or req[1] != 1: return
        atyp = req[3]
        if atyp == 1:
            addr = socket.inet_ntoa(await reader.readexactly(4))
        elif atyp == 3:
            ln = (await reader.readexactly(1))[0]
            addr = (await reader.readexactly(ln)).decode()
        elif atyp == 4:
            addr = socket.inet_ntop(socket.AF_INET6, await reader.readexactly(16))
        else: return
        port = struct.unpack(">H", await reader.readexactly(2))[0]
        try:
            r2, w2 = await asyncio.open_connection(addr, port)
        except Exception:
            writer.write(b"\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00"); await writer.drain(); return
        writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00"); await writer.drain()
        await asyncio.gather(relay(reader, w2), relay(r2, writer))
    except Exception: pass
    finally:
        try: writer.close()
        except Exception: pass

async def main():
    port = int(os.environ.get("PORT", "8080"))
    srv = await asyncio.start_server(handle, "0.0.0.0", port)
    print(f"SOCKS5 listening on 0.0.0.0:{port}", flush=True)
    async with srv: await srv.serve_forever()

asyncio.run(main())
