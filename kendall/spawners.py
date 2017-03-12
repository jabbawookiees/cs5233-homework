from .main import Entity, SISOStream
import numpy.random


class Spawner(SISOStream):
    """This implements the generic class for a spawner.
    A spawner has the properties:
    name -- The name of this queue

    Interesting things to override:
    next_event_time - Dictates the time between events
    create_entity - Dictates what entities are created
    """

    def start(self):
        first_event_time = self.next_event_time(0)
        self.simulator.tick_future(self, first_event_time)

    def ready(self):
        return False

    def tick(self, time):
        entity = self.create_entity(time)
        self.simulator.add_entity(entity)
        entity.on_exit(self, time)
        self.destination.enter(entity, time)
        next_spawn_time = self.next_event_time(time)
        self.simulator.tick_future(self, next_spawn_time)

    def notify_ready(self, time):
        pass

    def next_event_time(self, time):
        raise Exception("Please implement this for the spawner")

    def create_entity(self, time):
        return Entity()


class ConstantSpawner(Spawner):
    def __init__(self, *args, **kwargs):
        """
        Keyword arguments:
        spawn_time - Time between spawns
        """
        super(ConstantSpawner, self).__init__(*args, **kwargs)
        self.spawn_time = kwargs.get('spawn_time', 1)

    def next_event_time(self, time):
        return time + self.spawn_time


class ExponentialSpawner(Spawner):
    def __init__(self, *args, **kwargs):
        """
        Keyword arguments:
        spawn_time - Time between spawns
        """
        super(ExponentialSpawner, self).__init__(*args, **kwargs)
        self.spawn_time = kwargs.get('spawn_time', 1)

    def next_event_time(self, time):
        return time + numpy.random.exponential(self.spawn_time)
