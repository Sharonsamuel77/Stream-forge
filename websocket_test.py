import asyncio
import websockets

async def test():
    ws = await websockets.connect("ws://127.0.0.1:8002/ws/metrics")
    print("CONNECTED")
    message = await ws.recv()
    print("RECEIVED:")
    print(message)
    await ws.close()

asyncio.run(test())
