import unittest
import threading
from main import Pipeline, Agent
from collections import deque
import time


class TestPipeline(unittest.TestCase):
    def test_pipeline(self):
        pipeline = Pipeline()
        agent1 = Agent(name="Agent 1", id=1, pipeline=pipeline)
        agent2 = Agent(name="Agent 2", id=2, pipeline=pipeline)

        agent1.add_destination(agent2)

        pipeline.add_agent(agent1)
        pipeline.add_agent(agent2)

        t1 = threading.Thread(target=pipeline.start)
        t1.start()

        agent1.connect()
        agent2.connect()

        data = {"message": "Hello World!"}
        agent2_data = deque(
            [{"destination": 2, "payload": {"message": "Hello World!"}}]
        )
        agent1.send_data(agent2, data)
        start = time.time()

        while agent2.data_queue.queue != agent2_data:
            continue
        else:
            stop = time.time()

        self.assertEqual(agent2_data, agent2.data_queue.queue)

        print(f"Time passed <= {stop-start}")

        pipeline.shutdown()


if __name__ == "__main__":
    unittest.main()
