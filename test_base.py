import unittest
import threading
from main import Pipeline, Agent, DataQueue


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
        agent1.send_data(agent2, data)

        t1.join()


if __name__ == "__main__":
    unittest.main()
