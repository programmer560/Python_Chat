import json
import socket
import threading
import time

import flet as ft

BUFFER = 4096


class Node:
    """TCP node (server or client). Messages are newline-delimited JSON."""

    def __init__(self, on_message, on_status):
        self.on_message = on_message
        self.on_status = on_status
        self.sock = None
        self.running = False
        self.lock = threading.Lock()

    def _read_lines(self, conn):
        buf = b""
        while self.running:
            chunk = conn.recv(BUFFER)
            if not chunk:
                return
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if line:
                    yield line.decode("utf-8", errors="replace")


class ServerNode(Node):
    def __init__(self, port, on_message, on_status):
        super().__init__(on_message, on_status)
        self.port = port
        self.clients = []

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(5)
        self.running = True
        self.on_status(f"Listening on port {self.port}")
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while self.running:
            try:
                conn, addr = self.sock.accept()
            except OSError:
                break
            with self.lock:
                self.clients.append(conn)
            self.on_status(f"Connected: {addr[0]}:{addr[1]} ({len(self.clients)} clients)")
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def _handle(self, conn, addr):
        try:
            for line in self._read_lines(conn):
                self.on_message(line)
                self.broadcast(line, exclude=conn)
        except OSError:
            pass
        self._drop(conn)
        self.on_status(f"Disconnected: {addr[0]}:{addr[1]}")

    def _drop(self, conn):
        with self.lock:
            if conn in self.clients:
                self.clients.remove(conn)
        try:
            conn.close()
        except OSError:
            pass

    def broadcast(self, message, exclude=None):
        data = (message + "\n").encode("utf-8")
        with self.lock:
            targets = [c for c in self.clients if c is not exclude]
        for c in targets:
            try:
                c.sendall(data)
            except OSError:
                self._drop(c)

    def send(self, message):
        self.broadcast(message)

    def stop(self):
        self.running = False
        with self.lock:
            clients, self.clients = self.clients, []
        for c in clients:
            try:
                c.close()
            except OSError:
                pass
        if self.sock:
            self.sock.close()


class ClientNode(Node):
    def __init__(self, host, port, on_message, on_status):
        super().__init__(on_message, on_status)
        self.host = host
        self.port = port

    def start(self):
        self.sock = socket.create_connection((self.host, self.port), timeout=5)
        self.sock.settimeout(None)
        self.running = True
        self.on_status(f"Connected to {self.host}:{self.port}")
        threading.Thread(target=self._receive, daemon=True).start()

    def _receive(self):
        try:
            for line in self._read_lines(self.sock):
                self.on_message(line)
        except OSError:
            pass
        was_running = self.running
        self.running = False
        if was_running:
            self.on_status("Disconnected from server")

    def send(self, message):
        self.sock.sendall((message + "\n").encode("utf-8"))

    def stop(self):
        self.running = False
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()


def main(page: ft.Page):
    page.title = "Flet P2P Messenger"
    page.theme_mode = ft.ThemeMode.DARK
    page.window.width = 450
    page.window.height = 700
    page.padding = 0

    state = {"node": None, "username": "User"}

    chat_list = ft.ListView(expand=True, spacing=10, padding=20, auto_scroll=True)
    status_text = ft.Text("Ready", size=12, color=ft.Colors.GREY_400)
    msg_input = ft.TextField(hint_text="Type a message...", expand=True)

    def set_status(text):
        status_text.value = f"Status: {text}"
        page.update()

    def add_message(raw, is_me=False):
        try:
            data = json.loads(raw)
            sender = data.get("user", "Unknown")
            text = data.get("text", "")
            stamp = data.get("time", "")
        except (ValueError, AttributeError):
            sender, text, stamp = "System", raw, time.strftime("%H:%M")

        chat_list.controls.append(
            ft.Column(
                [
                    ft.Text(f"{sender} • {stamp}", size=10, color=ft.Colors.GREY_500),
                    ft.Container(
                        content=ft.Text(text, color=ft.Colors.WHITE, selectable=True),
                        bgcolor=ft.Colors.BLUE_700 if is_me else ft.Colors.GREY_800,
                        padding=12,
                        border_radius=15,
                    ),
                ],
                horizontal_alignment=(
                    ft.CrossAxisAlignment.END if is_me else ft.CrossAxisAlignment.START
                ),
                spacing=2,
            )
        )
        page.update()

    def send_click(e):
        node = state["node"]
        text = (msg_input.value or "").strip()
        if not text or not node or not node.running:
            return
        raw = json.dumps(
            {"user": state["username"], "text": text, "time": time.strftime("%H:%M")}
        )
        try:
            node.send(raw)
        except OSError as ex:
            set_status(f"Send error: {ex}")
            return
        add_message(raw, is_me=True)
        msg_input.value = ""
        msg_input.focus()
        page.update()

    msg_input.on_submit = send_click

    def disconnect(e=None):
        if state["node"]:
            state["node"].stop()
            state["node"] = None
        chat_list.controls.clear()
        show_setup_view()

    def show_chat_view(title):
        page.clean()
        page.add(
            ft.Column(
                [
                    ft.AppBar(
                        title=ft.Text(f"{state['username']} ({title})"),
                        leading=ft.IconButton(ft.Icons.ARROW_BACK, on_click=disconnect),
                    ),
                    chat_list,
                    ft.Container(status_text, padding=ft.Padding.symmetric(horizontal=20)),
                    ft.Container(
                        ft.Row(
                            [msg_input, ft.IconButton(ft.Icons.SEND, on_click=send_click)]
                        ),
                        padding=10,
                    ),
                ],
                expand=True,
                spacing=0,
            )
        )
        page.update()

    def show_setup_view():
        page.clean()
        name_field = ft.TextField(label="Your Name", value=state["username"])
        host_field = ft.TextField(label="Host/IP", value="127.0.0.1")
        port_field = ft.TextField(label="Port", value="5000")
        error_text = ft.Text("", color=ft.Colors.RED_400)

        def launch(node_factory, title):
            try:
                port = int(port_field.value)
            except ValueError:
                error_text.value = "Port must be a number"
                page.update()
                return
            state["username"] = name_field.value.strip() or "User"
            node = node_factory(port)
            try:
                node.start()
            except OSError as ex:
                error_text.value = f"Error: {ex}"
                page.update()
                return
            state["node"] = node
            show_chat_view(title)

        def start_server(e):
            launch(lambda p: ServerNode(p, add_message, set_status), "Server")

        def start_client(e):
            host = host_field.value.strip()
            launch(
                lambda p: ClientNode(host, p, add_message, set_status), f"Client → {host}"
            )

        page.add(
            ft.Container(
                ft.Column(
                    [
                        ft.Text("Messenger Setup", size=30, weight=ft.FontWeight.BOLD),
                        name_field,
                        ft.Divider(),
                        ft.Text("Connection Settings", size=16),
                        host_field,
                        port_field,
                        error_text,
                        ft.Row(
                            [
                                ft.FilledButton(
                                    "Start as Server",
                                    icon=ft.Icons.DNS,
                                    on_click=start_server,
                                    expand=True,
                                ),
                                ft.FilledButton(
                                    "Join as Client",
                                    icon=ft.Icons.LOGIN,
                                    on_click=start_client,
                                    expand=True,
                                ),
                            ]
                        ),
                    ],
                    spacing=20,
                ),
                padding=30,
            )
        )
        page.update()

    page.on_disconnect = lambda e: state["node"] and state["node"].stop()
    show_setup_view()


if __name__ == "__main__":
    ft.run(main)
