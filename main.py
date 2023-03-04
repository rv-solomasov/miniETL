import socket
import threading
import json


class Pipeline:
    def __init__(self):
        self.agents = []

    def add_agent(self, agent):
        self.agents.append(agent)

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(("localhost", 8000))
        self.socket.listen()

        while True:
            conn, addr = self.socket.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            thread.start()

    def handle_client(self, conn, addr):
        while True:
            data = conn.recv(1024)
            if not data:
                break
            data = json.loads(data)
            for agent in self.agents:
                if agent.id == data["destination"]:
                    agent.receive_data(data["payload"])
                    break

            conn.sendall(b"OK")

        conn.close()


class Agent:
    def __init__(self, name, id, pipeline, sources=[], destinations=[]):
        self.name = name
        self.id = id
        self.pipeline = pipeline
        self.sources = sources
        self.destinations = destinations
        self.options = {}
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, pipeline_socket):
        self.socket.connect(("localhost", pipeline_socket))

    def add_source(self, source: "Agent"):
        self.sources.append(source)

    def add_destination(self, destination: "Agent"):
        self.destinations.append(destination)

    def receive_data(self, payload):
        pass

    def send_data(self, destination: "Agent", payload):
        data = {"destination": destination.id, "payload": payload}
        self.socket.sendall(json.dumps(data).encode())
