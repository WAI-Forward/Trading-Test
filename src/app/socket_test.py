import socket
try:
    socket.create_connection(("demo.ctraderapi.com", 5035), timeout=5)
    print("✅ Connection successful")
except Exception as e:
    print("❌ Connection failed:", e)
