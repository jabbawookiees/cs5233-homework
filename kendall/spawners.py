from .main import EnterEventData, KendallStream, KendallEvent
import numpy.random


class KendallSpawner(KendallStream):
    def __init__(self, simulator):
        self.simulator = simulator

    def enter(self, event, event_data):
        self._pipe_event(event, event_data.time)
        next_event = KendallEvent(None)
        next_event_time = self.next_event_time(event, event_data.time)
        next_event_data = EnterEventData(next_event_time, self)
        self.simulator.add_event(next_event, next_event_data)

    def start(self):
        next_event = KendallEvent(None)
        next_event_time = self.next_event_time(None, 0)
        next_event_data = EnterEventData(next_event_time, self)
        self.simulator.add_event(next_event, next_event_data)

    def next_event_time(self, event, time):
        return time + 1

    def reset(self):
        pass


class ExponentialSpawner(KendallSpawner):
    def __init__(self, simulator, scale):
        self.simulator = simulator
        self.scale = scale

    def next_event_time(self, event, time):
        return time + numpy.random.exponential(self.scale)
