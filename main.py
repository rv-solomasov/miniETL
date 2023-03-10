import socket
import threading
import json
from collections import deque


class DataQueue:
    def __init__(self, max_size):
        self.queue = deque()
        self.max_size = max_size
        self.curr_size = 0

    def push(self, data):
        if len(self.queue) < self.max_size:
            self.queue.append(data)
            self.curr_size += len(str(data).encode())
        else:
            raise Exception("Queue is full")

    def pop(self):
        if len(self.queue) > 0:
            data_removed = self.queue.popleft()
            self.curr_size -= len(str(data_removed).encode())
        else:
            raise Exception("Queue is empty")

    def is_empty(self):
        return len(self.queue) == 0

    def is_full(self):
        return len(self.queue) == self.max_size

    def clear(self):
        self.queue.clear()


class Pipeline:
    def __init__(
        self,
        max_data_volume=1024 * 1024,
        max_queue_size=10,
        host="localhost",
        port=8000,
    ):
        self.agents = {}
        self.max_data_volume = max_data_volume
        self.data_volume = 0
        self.host = host
        self.port = port
        self.status = 1
        self.max_queue_size = max_queue_size

    def add_agent(self, agent):
        self.agents[agent.id] = agent

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen()

        while self.status:
            conn, addr = self.socket.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn,))
            thread.start()
        thread.join()

    def handle_client(self, conn):
        data = b""
        while True:
            chunk = conn.recv(1024)
            if not chunk:
                break
            data += chunk
        data = json.loads(data.decode("utf-8"))
        agent_id = data.get("destination")
        agent = self.agents.get(agent_id)
        if agent is not None:
            agent.receive_data(data)
        conn.close()

    def shutdown(self):
        self.status = 0


class Agent:
    def __init__(self, name, id, pipeline):
        self.name = name
        self.id = id
        self.pipeline = pipeline
        self.sources = []
        self.destinations = []
        self.options = {}
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data_queue = DataQueue(self.pipeline.max_data_volume)
        self.connected = 0

    def connect(self):
        self.socket.connect((self.pipeline.host, self.pipeline.port))
        self.connected = 1

    def add_source(self, source: "Agent"):
        self.sources.append(source.id)
        source.destinations.append(self.id)

    def add_destination(self, destination: "Agent"):
        self.destinations.append(destination.id)
        destination.sources.append(self.id)

    def receive_data(self, data):
        if (
            self.data_queue.curr_size + len(json.dumps(data).encode())
            <= self.data_queue.max_size
        ) & (len(data) + len(self.data_queue.queue) <= self.pipeline.max_queue_size):
            self.data_queue.push(data["payload"])

    def send_data(self, destination: "Agent", payload):
        if not self.connected:
            self.connect()
            self.connected = 1
        data = {"destination": destination.id, "payload": payload}
        self.socket.sendall(json.dumps(data).encode())
        self.socket.close()
        if payload in self.data_queue.queue:
            self.data_queue.pop
        self.connected = 0


# Ideas for autonomous processing - no direct ~send/receive_data commands required
"""    def run(self):
        self.socket.listen()
        for source in self.sources:
            source.connect()

        while True:
            if not self.data_queue.is_empty():
                data = self.data_queue.pop()
                for destination in self.destinations:
                    destination.send_data(self, data["payload"])
            else:
                self.receive_data()
"""
