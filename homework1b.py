import numpy.random
import matplotlib.pyplot as plt
from kendall import KendallSimulator, KendallServer, KendallEvent
from kendall.queues import RandomServerQueue
from kendall.spawners import ExponentialSpawner


def average(list):
    return sum(list) / float(len(list))


class TelephoneLine(KendallServer):
    def __init__(self, queue, id):
        self.queue = queue
        self.id = id
        self.reset()

    def assign(self, event, time):
        self.event = event
        self.start_time = time

    def complete(self, event, time):
        self.event = None
        self.total_time += time - self.start_time
        self.start_time = None

    def reset(self):
        self.event = None
        self.start_time = None
        self.total_time = 0.00


class CallEvent(KendallEvent):
    average_call_duration = 300.0
    count = 0
    dropped = 0
    last_spawn_time = 0.0
    spawn_delays = []
    call_durations = []

    def __init__(self, *args, **kwargs):
        super(CallEvent, self).__init__(*args, **kwargs)
        CallEvent.count += 1
        self.count = CallEvent.count
        self._processing_time = numpy.random.exponential(CallEvent.average_call_duration)

    def on_enqueue(self, queue, time):
        self.enter_time = time
        CallEvent.spawn_delays.append(time - CallEvent.last_spawn_time)
        CallEvent.call_durations.append(self._processing_time)
        CallEvent.last_spawn_time = time
        # print "Ship {} entered harbor at {}! Current queue: {}".format(self.count, time, len(queue.queue))

    def on_drop(self, queue, time):
        CallEvent.dropped += 1
        # print "Call {} was dropped at {}!".format(self.count, time)

    def on_process(self, server, time):
        # print "Call {} was started at {}!".format(self.count, time)
        pass

    def on_finish(self, queue, time):
        self.exit_time = time
        # print "Call {} was finished at {}!".format(self.count, time)

    def processing_time(self, server):
        return self._processing_time


class TelephoneSystem(RandomServerQueue):
    server_class = TelephoneLine


class CallSpawner(ExponentialSpawner):
    pipe_event_class = CallEvent


class AsymmetricTelephonySimulator(KendallSimulator):
    def __init__(self, time_limit, a_to_b_lines, b_to_a_lines):
        super(AsymmetricTelephonySimulator, self).__init__(time_limit)

        self.city_a_spawner = CallSpawner(self, 12.0)
        self.city_b_spawner = CallSpawner(self, 15.0)
        self.a_to_b_system = TelephoneSystem(self, server_count=a_to_b_lines, max_queued=0)
        self.b_to_a_system = TelephoneSystem(self, server_count=b_to_a_lines, max_queued=0)

        self.register_queue("City A Spawner", self.city_a_spawner)
        self.register_queue("City B Spawner", self.city_b_spawner)
        self.register_queue("A to B System", self.a_to_b_system)
        self.register_queue("B to A System", self.b_to_a_system)
        self.city_a_spawner.pipe(self.a_to_b_system)
        self.city_b_spawner.pipe(self.b_to_a_system)

        self.all_spawn_delays = []
        self.all_call_durations = []
        self.all_dropped_percentages = []
        self.all_minimums = []
        self.all_averages = []
        self.all_maximums = []

    def reset(self):
        super(AsymmetricTelephonySimulator, self).reset()
        self.city_a_spawner.start()
        self.city_b_spawner.start()
        CallEvent.count = 0
        CallEvent.dropped = 0
        CallEvent.last_spawn_time = 0.0
        CallEvent.spawn_delays = []
        CallEvent.call_durations = []

    def collect(self):
        minimum = min(CallEvent.call_durations)
        average = sum(CallEvent.call_durations) / len(CallEvent.call_durations)
        maximum = max(CallEvent.call_durations)

        self.all_spawn_delays.extend(CallEvent.spawn_delays)
        self.all_call_durations.extend(CallEvent.call_durations)
        self.all_minimums.append(minimum)
        self.all_maximums.append(maximum)
        self.all_averages.append(average)
        self.all_dropped_percentages.append(CallEvent.dropped / float(CallEvent.count))


class SymmetricTelephonySimulator(KendallSimulator):
    def __init__(self, time_limit, total_lines):
        super(SymmetricTelephonySimulator, self).__init__(time_limit)

        self.city_a_spawner = CallSpawner(self, 12.0)
        self.city_b_spawner = CallSpawner(self, 15.0)
        self.symmetric_system = TelephoneSystem(self, server_count=total_lines, max_queued=0)

        self.register_queue("City A Spawner", self.city_a_spawner)
        self.register_queue("City B Spawner", self.city_b_spawner)
        self.register_queue("Symmetric System", self.symmetric_system)
        self.city_a_spawner.pipe(self.symmetric_system)
        self.city_b_spawner.pipe(self.symmetric_system)

        self.all_spawn_delays = []
        self.all_call_durations = []
        self.all_dropped_percentages = []
        self.all_minimums = []
        self.all_averages = []
        self.all_maximums = []

    def reset(self):
        super(SymmetricTelephonySimulator, self).reset()
        self.city_a_spawner.start()
        self.city_b_spawner.start()
        CallEvent.count = 0
        CallEvent.dropped = 0
        CallEvent.last_spawn_time = 0.0
        CallEvent.spawn_delays = []
        CallEvent.call_durations = []

    def collect(self):
        minimum = min(CallEvent.call_durations)
        average = sum(CallEvent.call_durations) / len(CallEvent.call_durations)
        maximum = max(CallEvent.call_durations)

        self.all_spawn_delays.extend(CallEvent.spawn_delays)
        self.all_call_durations.extend(CallEvent.call_durations)
        self.all_minimums.append(minimum)
        self.all_maximums.append(maximum)
        self.all_averages.append(average)
        self.all_dropped_percentages.append(CallEvent.dropped / float(CallEvent.count))
