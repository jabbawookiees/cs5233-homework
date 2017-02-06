import heapq


class EventData(object):
    def __init__(self):
        pass

    def __lt__(self, other):
        if self.time < other.time:
            return True
        elif self.time == other.time and self.queue < other.queue:
            return True
        else:
            return False


class EnterEventData(EventData):
    def __init__(self, time, queue):
        self.time = time
        self.queue = queue


class FinishEventData(EventData):
    def __init__(self, time, queue, server):
        self.time = time
        self.queue = queue
        self.server = server


class KendallServer(object):
    def __init__(self, queue, id):
        self.queue = queue
        self.id = id
        self.event = None

    def assign(self, event, time):
        self.event = event

    def complete(self, event, time):
        self.event = None

    def reset(self):
        self.event = None


class KendallEvent(object):
    def __init__(self, parent):
        self.parent = parent

    def processing_time(self, server):
        return 0

    def on_arrive(self, queue, global_time):
        "This is called when the arrival event happens whether it is enqueued or dropped"

    def on_drop(self, queue, global_time):
        "This is called when the arrival event is dropped because the queue is full"

    def on_enqueue(self, queue, global_time):
        "This is called when the arrival event is queued"

    def on_process(self, server, global_time):
        "This is called when the event is started by a server"

    def on_finish(self, server, global_time):
        "This is called when the arrival event is finished by a server"


class KendallStream(object):
    pipe_event_class = KendallEvent
    drop_event_class = KendallEvent

    def __init__(self):
        self.destination = None
        self.drop_destination = None

    def pipe(self, destination, drop_destination=None):
        self.destination = destination
        self.drop_destination = drop_destination

    def _pipe_event(self, event, time):
        if self.destination:
            new_event = self.pipe_prepare(event)
            event_data = EnterEventData(time, self.destination)
            self.simulator.add_event(new_event, event_data)

    def _drop_event(self, event, time):
        if self.drop_destination:
            new_event = self.drop_prepare(event)
            event_data = EnterEventData(time, self.drop_destination)
            self.simulator.add_event(new_event, event_data)

    def pipe_prepare(self, event):
        return self.pipe_event_class(event)

    def drop_prepare(self, event):
        return self.drop_event_class(event)


class KendallSimulator(object):
    def __init__(self, time_limit):
        self._simulator_queue = []
        self.queues = []
        self.time_limit = time_limit

    def register_queue(self, name, queue):
        queue.name = name
        queue.priority = len(self.queues)
        self.queues.append(queue)

    def add_event(self, event, event_data):
        heapq.heappush(self._simulator_queue, (event_data, event))

    def run(self):
        while len(self._simulator_queue) > 0:
            event_data, event = heapq.heappop(self._simulator_queue)
            if event_data.time > self.time_limit:
                break
            if isinstance(event_data, EnterEventData):
                queue = event_data.queue
                queue.enter(event, event_data)
            elif isinstance(event_data, FinishEventData):
                queue = event_data.queue
                queue.exit(event, event_data)
            else:
                raise Exception("Unsupported event type!")

    def reset(self):
        del self._simulator_queue[:]
        for queue in self.queues:
            queue.reset()
