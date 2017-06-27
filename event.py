

class Event(object):
    def __init__(self, time):
        self.time = time

    def __cmp__(self, other):
        return cmp(self.time, other.time)
   #Type = enum('JobSubmit', 'JobComplete', 'StageSubmit', 'StageComplete', 'TaskSubmit', 'TaskComplete')


class EventJobSubmit(Event):
    def __init__(self, time, job):
        super(self.__class__, self).__init__(time)
        self.job = job


class EventReAlloc(Event):
    def __init__(self, time):
        super(self.__class__, self).__init__(time)


class EventJobComplete(Event):
    def __init__(self, time, job):
        super(self.__class__, self).__init__(time)
        self.job = job


class EventStageSubmit(Event):
    def __init__(self, time, stage):
        super(self.__class__, self).__init__(time)
        self.stage = stage


class EventStageComplete(Event):
    def __init__(self, time, stage):
        super(self.__class__, self).__init__(time)
        self.stage = stage


class EventTaskSubmit(Event):
    def __init__(self, time, task):
        super(self.__class__, self).__init__(time)
        self.task = task


class EventTaskComplete(Event):
    def __init__(self, time, task, machine_id):
        super(self.__class__, self).__init__(time)
        self.task = task
        self.running_machine_id = machine_id
