import random
from collections import deque
from .main import SingleInputStream, SingleOutputStream, SISOStream


class Queue(SISOStream):
    """
    This is a simple FIFO queue.
    A queue has the properties:
    name -- The name of this queue
    queue -- The currently queued items
    max_queued -- The limit on the queue before it starts dropping events

    Override enqueue/dequeue to change the FIFO-ness on events.
    """

    def __init__(self, *args, **kwargs):
        super(Queue, self).__init__(*args, **kwargs)
        self.queue = deque()
        self.max_queued = kwargs.get('max_queued', 10**100)

    def start(self):
        while len(self.queue) > 0:
            self.queue.pop()

    def enter(self, entity, time):
        """
        This is what happens when an object enters the queue. If the piped destination is ready,
        then push the entity directly there. Otherwise queue up the entity (or drop).
        """
        if self.destination.ready():
            entity.on_enter(self, time)
            entity.on_exit(self, time)
            self.destination.enter(entity, time)
        elif not self.is_full():
            entity.on_enter(self, time)
            self.queue.append(entity)
            if self.is_full():
                self.source.notify_unready(self, time)
        else:
            entity.on_enter(self, time)
            entity.on_drop(self, time)

    def is_full(self):
        return len(self.queue) >= self.max_queued

    def ready(self):
        return not self.is_full() or self.destination.ready()

    def notify_ready(self, other, time):
        was_full = self.is_full()
        if len(self.queue) > 0:
            entity = self.queue.popleft()
            entity.on_exit(self, time)
            self.destination.enter(entity, time)

        if was_full:
            self.source.notify_ready(self, time)


class Worker(SISOStream):
    """
    This is a simple worker.
    A worker has the following properties:
    capacity - How many workers it can work on at the same time

    Interesting things to override:
    time_to_finish - Dictates the time to work on an entity
    """
    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self.entities = {}
        self.current = 0
        self.capacity = kwargs.get('capacity', 1)
        self.completed = deque()

    def time_to_finish(self, entity, time):
        return 1

    def push_completed(self, time):
        was_full = self.current == self.capacity
        if self.destination.ready() and len(self.completed) > 0:
            entity = self.completed.popleft()
            entity.on_exit(self, time)
            self.destination.enter(entity, time)
            self.current -= 1
            if was_full:
                self.source.notify_ready(self, time)

    def start(self):
        self.current = 0
        self.entities.clear()

    def enter(self, entity, time):
        if self.current < self.capacity:
            entity.on_enter(self, time)
            later = time + self.time_to_finish(entity, time)
            if self.entities.get(later) is None:
                self.entities[later] = deque()
            self.current += 1
            self.entities[later].append(entity)
            self.simulator.tick_future(self, later)
            if self.current >= self.capacity:
                self.source.notify_unready(self, time)
        else:
            entity.on_enter(self, time)
            entity.on_drop(self, time)

    def ready(self):
        return self.current <= self.capacity

    def tick(self, time):
        entity = self.entities[time].popleft()
        if len(self.entities[time]) == 0:
            del self.entities[time]
        self.completed.append(entity)
        self.push_completed(time)

    def notify_ready(self, other, time):
        self.push_completed(time)


class Splitter(SingleInputStream):
    """
    This implements a simple splitter.
    A splitter has the properties:
    destinations -- The destinations of this stream
    ready_destinations -- The ist of destinations which are ready
    """
    def __init__(self, *args, **kwargs):
        super(Splitter, self).__init__(*args, **kwargs)
        self.destinations = []
        self.ready_destinations = deque()

    def select_destination(self, entity, time):
        return self.ready_destinations[0]

    def start(self):
        while len(self.ready_destinations) > 0:
            self.ready_destinations.pop()
        for destination in self.destinations:
            self.ready_destinations.append(destination)

    def pipe(self, destination):
        "Connect this to where the entities go after queueing"
        self.destinations.append(destination)
        destination.reverse_pipe(self)

    def enter(self, entity, time):
        if len(self.ready_destinations) > 0:
            entity.on_enter(self, time)
            entity.on_exit(self, time)
            destination = self.select_destination(entity, time)
            destination.enter(entity, time)
        else:
            entity.on_enter(self, time)
            entity.on_drop(self, time)

    def ready(self):
        if len(self.ready_destinations) > 0:
            return True
        else:
            return False

    def notify_ready(self, other, time):
        self.ready_destinations.append(other)
        if len(self.ready_destinations) == 1:
            self.source.notify_ready(self, time)

    def notify_unready(self, other, time):
        self.ready_destinations.remove(other)
        if len(self.ready_destinations) == 0:
            self.source.notify_unready(self, time)


class RandomSplitter(Splitter):
    def select_destination(self, entity, time):
        return random.choice(self.ready_destinations)


class Merger(SingleOutputStream):
    """Stream merger. Takes multiple streams and pushes things as they come into another.
    A splitter has the properties:
    sources -- The list of all streams that lead to this one.
    """

    def __init__(self, *args, **kwargs):
        super(Merger, self).__init__(*args, **kwargs)
        self.sources = []

    def reverse_pipe(self, source):
        self.sources.append(source)

    def enter(self, entity, time):
        """
        This is called when an entity enters a stream. By default the entity passes through directly.
        """
        entity.on_enter(self, time)
        entity.on_exit(self, time)
        self.destination.enter(entity, time)

    def ready(self):
        return self.destination.ready()

    def notify_ready(self, other, time):
        for source in self.sources:
            source.notify_ready(self, time)

    def notify_unready(self, other, time):
        for source in self.sources:
            source.notify_unready(self, time)


class Dropper(SISOStream):
    def enter(self, entity, time):
        if self.destination is not None and self.destination.ready():
            entity.on_enter(self, time)
            entity.on_exit(self, time)
            self.destination.enter(entity, time)
        else:
            entity.on_enter(self, time)
            entity.on_drop(self, time)

    def ready(self):
        return True

    def notify_ready(self, other, time):
        pass

    def notify_unready(self, other, time):
        pass
