#
# Local Queue of sorted items to be scheduled
#
# 

from netsquid.simutil import sim_time
from netsquid.pydynaa import Entity, EventType, EventHandler
from qlinklayer.general import LinkLayerException
from easysquid.toolbox import logger


class LocalQueue(Entity):
    """
    Local queue of items ordered by sequence number, incl some additional features
    keeping 

    """

    def __init__(self, wsize=None, maxSeq=None, scheduleAfter=0, throw_events=False):

        # Largest possible sequence number before wraparound
        if maxSeq is None:
            self.maxSeq = 2 ** 32
        else:
            self.maxSeq = maxSeq

        # Maximum number of items to hold in the queue at any one time
        if wsize is None:
            self.wsize = self.maxSeq
        else:
            assert wsize <= self.maxSeq
            self.wsize = wsize

        # Next sequence number to assign
        self.nextSeq = 0

        # Time to first allow execution after addition
        self.scheduleAfter = scheduleAfter

        # Actual queue
        # TODO not a great data structure
        self.queue = {}

        # Current sequence number that can be consumed
        self.popSeq = 0

        self.throw_events = throw_events
        if self.throw_events:
            self._EVT_ITEM_ADDED = EventType("QUEUE ITEM ADDED", "Item added to the local queue")
            self._EVT_ITEM_REMOVED = EventType("QUEUE ITEM REMOVED", "Item removed to the local queue")

            # Data stored for data collection
            self._last_seq_added = None
            self._last_seq_removed = None

    def add(self, originID, request):
        """
        Add item to the Queue with a new sequence number, used by master node only.
        """

        # Check how many items are on the queue right now
        if len(self.queue) > self.wsize:
            raise LinkLayerException("Local queue full: {}".format(len))

        # There is space, create a new queue item
        seq = self.nextSeq

        logger.debug("Adding item with seq={} to local queue".format(seq))

        self.add_with_id(originID, seq, request)

        # Increment the next sequence number to assign
        self.nextSeq = (self.nextSeq + 1) % self.maxSeq

        return seq

    def add_with_id(self, originID, seq, request):
        """
        Stores the request within the queue at the specified sequence number along with the information specifying the
        origin of the request
        :param originID: int
            The ID of the node that placed the request in the queue
        :param seq: int
            The sequence number in the queue of the request item
        :param request: obj
            The request item that we are storing within the queue
        :return: None
        """
        # Compute the minimum time at which this request can be served
        now = sim_time()
        sa = now + self.scheduleAfter

        # TODO Needs fixing
        lq = _LocalQueueItem(request, seq, sa)
        self.queue[seq] = lq

        if self.throw_events:
            logger.debug("Scheduling item added event now.")
            self._schedule_now(self._EVT_ITEM_ADDED)
            self._last_seq_added = seq

    def remove_item(self, seq):
        """
        Removes the queue item corresponding to the provided sequence number from the queue
        :param seq: int
            Identifier of the queue item we wish to remove
        :return: obj `~qlinklayer.localQueue.LocalQueueItem`
            The queue item that we removed if any, else None
        """
        if seq in self.queue:
            q = self.queue.pop(seq)
            logger.debug("Removing item with seq={} from local queue".format(q.seq))

            if self.throw_events:
                logger.debug("Scheduling item removed event now.")
                self._schedule_now(self._EVT_ITEM_REMOVED)
                self._last_seq_removed = q.seq

            if seq == self.popSeq:
                self.popSeq = self._get_next_pop_seq()

            return q

        else:
            logger.warning("Sequence number {} not found in local queue".format(seq))
            return None

    def pop(self):
        """
        Get item off the top of the queue, if it is ready to be scheduled. 
        If no item is available, return None
        """

        if len(self.queue) == 0:
            # No items on queue
            return None

        # Get item off queue
        q = self.queue[self.popSeq]

        # Check if it's ready to be scheduled
        now = sim_time()

        if q.scheduleAt <= now:
            # Item ready

            # Remove from queue
            self.queue.pop(self.popSeq, None)

            logger.debug("Removing item with seq={} to local queue".format(q.seq))

            if self.throw_events:
                logger.debug("Scheduling item added event now.")
                self._schedule_now(self._EVT_ITEM_REMOVED)
                self._last_seq_removed = q.seq

            # Increment lower bound of sequence numbers to return next
            self.popSeq = self._get_next_pop_seq()

            # Return item
            return q

    def _get_next_pop_seq(self):
        # Return the next item if available otherwise increment
        inc_seq = (self.popSeq + 1) % self.maxSeq
        return inc_seq if not self.queue.keys() else min(self.queue.keys())

    def peek(self):
        """
        Get item off the top of the queue without removing it
        :return:
        """
        if len(self.queue) == 0:
            # No items on queue
            return None

        # Get item off queue
        return self.queue[self.popSeq]

    def get_min_schedule(self):
        """
        Get the smallest time at which we may schedule the next item
        """
        if len(self.queue) == 0:
            # No items on queue
            return None

        return self.queue[self.popSeq].scheduleAt

    def contains(self, seq):
        """
        Checks if the queue contains the provided sequence number
        :param seq: int
            The sequence number to check for
        :return: bool
        """
        if seq in self.queue:
            return True
        else:
            return False

    def ready(self, seq, minTime):
        """
        Mark the queue item with queue sequence number seq as ready, and mark it to be
        scheduled at least minTime from now.
        """

        try:
            self.queue[seq].ready = True
        except KeyError:
            # Not in queue, TODO error handling
            return

    def schedule_item(self, seq, scheduleAt):
        """
        Schedules the service time of an item in the queue
        :param seq: int
            Sequence number of the item
        :param scheduleAt: float
            Time to set the item for servicing
        """
        item = self.queue[seq]
        item.scheduleAt = scheduleAt + sim_time()
        item.schedule()

    def _reset_data(self):
        """
        Resets the variables storing the data for data collection
        :return:
        """
        self._last_seq_added = None
        self._last_seq_removed = None


class TimeoutLocalQueue(LocalQueue):
    def __init__(self, qid=None, wsize=None, maxSeq=None, scheduleAfter=0.0, throw_events=False):
        """
        Implements a local queue that supports timing out queue items asynchronously
        :param wsize: int
            Number of items that this queue holds at any given time
        :param maxSeq: int
            Maximum sequence number for items in the queue
        :param scheduleAfter: float
            Default schedule delay for added queue items
        """
        super(TimeoutLocalQueue, self).__init__(wsize=wsize, maxSeq=maxSeq, scheduleAfter=scheduleAfter,
                                                throw_events=throw_events)
        self._EVT_PROC_TIMEOUT = EventType("QUEUE ITEM REMOVED", "Triggers when an item has successfully been removed")
        self._EVT_SCHEDULE = EventType("LOCAL QUEUE SCHEDULE", "Triggers when a queue item is ready to be scheduled")
        self.timed_out_items = []
        self.ready_items = []
        self.qid = qid

    def add_with_id(self, originID, seq, request):
        """
        Adds an item to the queue.
        :param originID: int
            ID of the source that instructed the addition of this queue item
        :param seq: int
            Sequence location to add this item into the queue
        :param request: obj any
            The item to store in the queue
        """
        # Compute the minimum time at which this request can be served
        now = sim_time()
        sa = now + self.scheduleAfter

        # Check if the item specifies a max queue time
        lifetime = getattr(request, 'max_time', 0.0)

        if lifetime == 0:
            lifetime = None

        # Store the item and attach the timeout event
        lq = _TimeoutLocalQueueItem(request, seq, sa, lifetime=lifetime)
        self.queue[seq] = lq
        lq.prepare()

        if self.throw_events:
            logger.debug("Scheduling item added event now.")
            self._schedule_now(self._EVT_ITEM_ADDED)
            self._last_seq_added = seq

    def add_scheduling_event(self, qseq):
        """
        Configures a handler to catch the scheduling event of the queue item
        :param qseq: int
            Sequence number of the item we want to add a handler to
        """
        queue_item = self.queue[qseq]

        # Only attach a handler if the item supports the timeout event
        if isinstance(queue_item, _TimeoutLocalQueueItem):
            logger.debug("TimedLocalQueue has queue item {}".format(vars(queue_item)))

            # timeout_evt_handler = EventHandler(self._timeout_handler)
            # self._wait_once(timeout_evt_handler, entity=queue_item, event_type=queue_item._EVT_TIMEOUT)

            schedule_evt_handler = EventHandler(self._schedule_handler)
            self._wait_once(schedule_evt_handler, entity=queue_item, event_type=queue_item._EVT_SCHEDULE)

    # def _timeout_handler(self, evt):
    #     """
    #     Timeout handler for queue item timeout event
    #     :param evt: obj `~netsquid.pydynaa.Event`
    #         The event that triggered this handler
    #     """
    #     # Grab the item that timed out
    #     queue_item = evt.source
    #     logger.debug("Timeout Triggered")
    #
    #     # Check if the item is still stored locally
    #     if self.contains(queue_item.seq):
    #         logger.debug("Removing item from queue")
    #         self.remove_item(queue_item.seq)
    #
    #         # Store the item for retrieval by higher layers
    #         self.timed_out_items.append(queue_item)
    #         logger.debug("Scheduling processing timeout event now.")
    #         self._schedule_now(self._EVT_PROC_TIMEOUT)
    #
    #     else:
    #         logger.debug("Item already removed!")

    def _schedule_handler(self, evt):
        """
        Handler that is triggered when an item is ready to be scheduled, bubbles up to the distributed queue
        :param evt: obj `~netsquid,pydynaa.Event`
            The event that triggered the handler
        """
        logger.debug("Schedule handler triggered in local queue")
        logger.debug("Scheduling schedule event now.")
        queue_item = evt.source
        self.ready_items.append(queue_item)
        self._schedule_now(self._EVT_SCHEDULE)


class _LocalQueueItem:
    def __init__(self, request, seq, scheduleAt):
        self.request = request
        self.seq = seq
        self.scheduleAt = scheduleAt

        # Flag whether this queue item is ready to be executed
        self.ready = False

    def prepare(self):
        pass


class _TimeoutLocalQueueItem(_LocalQueueItem, Entity):
    def __init__(self, request, seq, scheduleAt, lifetime=0.0):
        """
        Local queue item that supports time outs
        :param request: obj any
            The request information to store with this queue item
        :param seq: int
            The sequence number of this item in the containing queue
        :param scheduleAt: float
            Delay time before queue item is officially scheduled
        :param lifetime: float
            The maximum amount of time this item can sit in the queue before timing out
        """
        super(_TimeoutLocalQueueItem, self).__init__(request=request, seq=seq, scheduleAt=scheduleAt)
        self.lifetime = lifetime
        self._EVT_TIMEOUT = EventType("QUEUE ITEM TIMEOUT", "Triggers when a queue item is stale")
        self._EVT_SCHEDULE = EventType("QUEUE ITEM SCHEDULE", "Triggers when a queue item is ready to be scheduled")

    def prepare(self):
        """
        Sets up the timeout event into the future
        """
        if self.lifetime:
            if self.request.create_time is not None:
                start = self.request.create_time
            else:
                start = sim_time()
            deadline = start + self.lifetime
            logger.debug("Scheduling timeout event at {}.".format(deadline))
            self._schedule_at(deadline, self._EVT_TIMEOUT)

    def schedule(self):
        """
        Schedules the item's schedule event for triggering pickup by the local queue
        """
        logger.debug("Scheduling timeout event at {}.".format(self.scheduleAt))
        self._schedule_at(self.scheduleAt, self._EVT_SCHEDULE)
