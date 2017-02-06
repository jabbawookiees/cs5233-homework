import random
from collections import deque
from .main import FinishEventData, KendallStream, KendallServer


class KendallQueue(KendallStream):
    """This implements a queue that simulates a Kendall queue.
    A queue has the properties:
    simulator -- The simulator environment which this queue belongs to
    queue -- The current queued items
    servers -- All the servers that this queue runes
    server_queue -- The currently free servers doing nothing
    max_queued -- The limit on the queue before it starts dropping events

    This is a FIFO queue.

    Override enqueue/dequeue to change the FIFO-ness on events.
    Override acquire_server/release_server to change the FIFO-ness on server usage.
    """
    server_class = KendallServer

    def __init__(self, simulator, server_count=1, max_queued=10**100):
        """
        The constructor for a KendallQueue

        Keyword Arguments:
        server_count -- The number of servers to be made (default: 1)
        max_queued -- The limit on the queue before it starts dropping events (default: 10**100)
        """
        super(KendallQueue, self).__init__()
        self.simulator = simulator
        self.queue = deque()
        self.servers = []
        self.server_queue = deque()
        self.max_queued = max_queued
        for x in xrange(server_count):
            server = self.server_class(self, x)
            self.servers.append(server)
        self.reset()

    def reset(self):
        while len(self.queue) > 0:
            self.queue.pop()
        while len(self.server_queue) > 0:
            self.server_queue.pop()
        for s in self.servers:
            s.reset()
            self.server_queue.append(s)

    def enter(self, event, event_data):
        server = self.acquire_server()
        if server is not None:
            event.on_arrive(self, event_data.time)
            event.on_enqueue(self, event_data.time)
            self._process(event, server, event_data.time)
        else:
            self._enqueue(event, event_data.time)

    def exit(self, event, event_data):
        self._finish(event, event_data.server, event_data.time)
        if len(self.queue) > 0:
            next_event = self.dequeue()
            server = self.acquire_server()
            self._process(next_event, server, event_data.time)

    def _enqueue(self, event, start_time):
        success = self.enqueue(event)
        event.on_arrive(self, start_time)
        if success:
            event.on_enqueue(self, start_time)
        else:
            event.on_drop(self, start_time)
            self._drop_event(event, start_time)
        return success

    def _process(self, event, server, start_time):
        server.assign(event, start_time)
        event.on_process(server, start_time)
        processing_time = event.processing_time(server)
        finish_event_data = FinishEventData(start_time + processing_time, self, server)
        self.simulator.add_event(event, finish_event_data)

    def _finish(self, event, server, start_time):
        event.on_finish(server, start_time)
        server.complete(event, start_time)
        self.release_server(server)
        self._pipe_event(event, start_time)

    def enqueue(self, event):
        if len(self.queue) < self.max_queued:
            self.queue.append(event)
            return True
        else:
            return False

    def dequeue(self):
        if len(self.queue) > 0:
            return self.queue.popleft()
        else:
            return None

    def acquire_server(self):
        if len(self.server_queue) > 0:
            return self.server_queue.popleft()
        else:
            return None

    def release_server(self, server):
        self.server_queue.append(server)


class RandomServerQueue(KendallQueue):
    def __init__(self, *args, **kwargs):
        super(RandomServerQueue, self).__init__(*args, **kwargs)

    def acquire_server(self):
        if len(self.server_queue) > 0:
            # Swap a random item with the left-most then do the thing
            rand = random.randrange(len(self.server_queue))
            sq = self.server_queue
            sq[0], sq[rand] = sq[rand], sq[0]
            return self.server_queue.popleft()
        else:
            return None
