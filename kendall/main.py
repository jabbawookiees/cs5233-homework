import heapq


class Simulator(object):
    """
    A Simulator is an object that runs simulations.
    It maintains a list of future events, among other things.
    It also contains a list of all entities ever in the simulation.
    """
    def __init__(self, *args, **kwargs):
        self.entity_list = []
        self.event_queue = []
        self.streams = {}
        self.current_time = 0
        self.time_limit = kwargs.get('time_limit', 0)

    def register_stream(self, stream):
        self.streams[stream.name] = stream

    def add_entity(self, entity):
        self.entity_list.append(entity)

    def tick_future(self, stream, time):
        event = SimulatorEvent(stream, stream.priority, time)
        heapq.heappush(self.event_queue, event)

    def run(self):
        for name, stream in self.streams.iteritems():
            stream.start()
        while len(self.event_queue) > 0:
            event = heapq.heappop(self.event_queue)
            if event.time > self.time_limit:
                break
            event.stream.tick(event.time)

    def reset(self):
        self.current_time = 0
        del self.entity_list[:]
        del self.event_queue[:]


class Entity(object):
    id = 1

    def __init__(self):
        self.id = Entity.id
        self.event_list = []
        self.event_table = {}
        Entity.id += 1

    def _on_any_event(self, stream, time, event):
        self.event_list.append((time, event, stream.name))
        self.event_table[stream.name, event] = time

    def on_enter(self, stream, time):
        self._on_any_event(stream, time, 'enter')

    def on_exit(self, stream, time):
        self._on_any_event(stream, time, 'exit')

    def on_drop(self, stream, time):
        self._on_any_event(stream, time, 'drop')


class SimulatorEvent(object):
    def __init__(self, stream, priority, time):
        self.stream = stream
        self.priority = priority
        self.time = time

    def __cmp__(self, other):
        if self.time < other.time:
            return -1
        elif self.time > other.time:
            return 1
        elif self.priority < other.priority:
            return -1
        elif self.priority > other.priority:
            return 1
        elif self.stream.name < other.stream.name:
            return 1
        elif self.stream.name > other.stream.name:
            return -1
        return 0


class Stream(object):
    """
    A stream is an abstract object which entities pass through.
    The currently implemented streams are:
    spawners - These don't accept input streams and just create entities.
    queues - These accept entities and queues them up.
    workers - These accept entities and waits for things to happen.
    splitters - These split entities from one stream among its destinations.
    mergers - These accept multiple streams as input and output one stream.

    Attributes:
    Streams have two attributes:
    name - The name of the stream used by the simulator. This should be unique.
    priority - The priority of this stream if multiple events with similar timings occur.
               In case of ties we use stream name.
    simulator - The simulator this stream belongs to.

    For the lazy, just set Stream.simulator = my_simulator
    """
    simulator = None

    def __init__(self, *args, **kwargs):
        """
        Keyword Arguments:
        name -- The name for the queue. Must be unique per simulator.
        """
        self.name = kwargs.get('name', None)
        self.priority = kwargs.get('priority', 1)
        self.simulator = kwargs.get('simulator', Stream.simulator)
        self.simulator.register_stream(self)

    def start(self):
        """
        This contains any initialization code when starting a new run.
        This should clear out and reset the state of all streams as well.
        Spawners will want to start its spawning loop here.
        """
        pass

    def enter(self, entity, time):
        """This is called when an entity enters this stream"""
        pass

    def tick(self, time):
        """
        This is called by the simulator when the a given time passes.
        This is triggered by using simulator.tick_future(self, time)
        """
        pass

    def notify_ready(self, other, time):
        """
        This is called by a destination when it's ready to receive entities.
        """
        pass

    def notify_unready(self, other, time):
        """
        This is called by a destination when it's no longer ready to receive entities.
        """
        pass


class SingleOutputStream(Stream):
    def __init__(self, *args, **kwargs):
        super(SingleOutputStream, self).__init__(*args, **kwargs)
        self.destination = None

    def pipe(self, destination):
        """Call source.pipe(destination) to flow entities from source to destination"""
        self.destination = destination
        self.destination.reverse_pipe(self)


class SingleInputStream(Stream):
    def __init__(self, *args, **kwargs):
        super(SingleInputStream, self).__init__(*args, **kwargs)
        self.source = None

    def reverse_pipe(self, source):
        """Called by other Streams when they pipe to this stream"""
        self.source = source

    def ready(self):
        return False


class SISOStream(SingleOutputStream, SingleInputStream):
    """
    A SISOStream is a Single Input and Single Output Stream. These would be queues and workers.
    """
    def __init__(self, *args, **kwargs):
        super(SISOStream, self).__init__(*args, **kwargs)

    def enter(self, entity, time):
        """
        This is called when an entity enters a stream. By default the entity passes through directly.
        """
        entity.on_enter(self, time)
        entity.on_exit(self, time)
        self.destination.enter(entity, time)

    def ready(self):
        if self.destination is not None:
            return self.destination.ready()
        else:
            return False

    def notify_ready(self, other, time):
        if self.source is not None:
            self.source.notify_ready(self, time)

    def notify_unready(self, other, time):
        if self.source is not None:
            self.source.notify_unready(self, time)
