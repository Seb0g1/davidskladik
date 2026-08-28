# -*- coding: utf-8 -*-
# Local port 3000 -> server's 127.0.0.1:3000 over SSH (for vite dev proxy).
import paramiko, socket, threading, socketserver, sys
sys.stdout.reconfigure(encoding='utf-8')

SSH_HOST, SSH_USER, SSH_PASS = "81.17.154.153", "root", "pm^e7-jVL_gAyM"
LOCAL_PORT, REMOTE_HOST, REMOTE_PORT = 3000, "127.0.0.1", 3000

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASS, timeout=30)
transport = client.get_transport()
transport.set_keepalive(15)

class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            chan = transport.open_channel("direct-tcpip", (REMOTE_HOST, REMOTE_PORT), self.request.getpeername())
        except Exception:
            return
        if chan is None:
            return
        def pump(src, dst):
            try:
                while True:
                    data = src.recv(65536)
                    if not data:
                        break
                    dst.sendall(data)
            except Exception:
                pass
            finally:
                try: dst.shutdown(socket.SHUT_WR)
                except Exception: pass
        t1 = threading.Thread(target=pump, args=(self.request, chan), daemon=True)
        t2 = threading.Thread(target=pump, args=(chan, self.request), daemon=True)
        t1.start(); t2.start(); t1.join(); t2.join()
        chan.close()

class Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

print(f"tunnel ready: localhost:{LOCAL_PORT} -> {SSH_HOST}:{REMOTE_PORT}", flush=True)
Server(("127.0.0.1", LOCAL_PORT), Handler).serve_forever()
