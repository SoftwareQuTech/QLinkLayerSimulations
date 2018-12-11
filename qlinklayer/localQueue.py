#
# Local Queue of sorted items to be scheduled
#
# 

from netsquid.simutil import sim_time
from netsquid.pydynaa import Entity, EventType, EventHandler
from qlinklayer.toolbox import LinkLayerException, check_schedule_cycle_bounds
from easysquid.toolbox import logger


class LocalQueue(Entity):

    def __init__(self, wsize=None, maxSeq=None, throw_events=False):
        """
        Local queue of items ordered by sequence number, incl some additional features
        Items have to manually be set to ready

        :param wsize: int
            Maximum number of items to hold in the queue at any one time
        :param maxSeq: int
            Largest possible sequence number before wraparound
        :param throw_events: bool
            Whether to throw events or not when adding and removing entries (for data collection)
        """

        # Largest possible sequence number before wraparound
        if maxSeq is None:
            self.maxSeq = 2 ** 8
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

        # Actual queue
        # TODO not a great data structure
        self.queue = {}

        # Current sequence number that can be consumed
        self.popSeq = None

        self.throw_events = throw_events
        if self.throw_events:
            self._EVT_ITEM_ADDED = EventType("QUEUE ITEM ADDED", "Item added to the local queue")
            self._EVT_ITEM_REMOVED = EventType("QUEUE ITEM REMOVED", "Item removed to the local queue")

            # Data stored for data collection
            self._seqs_added = []
            self._seqs_removed = []

    def is_full(self):
        """
        Checks whether the local queue is full
        :return: bool
            True/False
        """
        return self.num_items() >= self.maxSeq

    def is_empty(self):
        """
        Checks whether the local queue is empty
        :return: bool
            True/False
        """
        return self.num_items() == 0

    def num_items(self):
        return len(self.queue)

    def add(self, originID, request):
        """
        Add item to the Queue with a new sequence number, used by master node only.
        """

        # Check how many items are on the queue right now
        if self.is_full():
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

        lq = self.get_new_queue_item(request, seq)
        self.queue[seq] = lq

        if self.popSeq is None:
            self.popSeq = seq

        if self.throw_events:
            logger.debug("Scheduling item added event now.")
            self._schedule_now(self._EVT_ITEM_ADDED)
            self._seqs_added.append(seq)

    def get_new_queue_item(self, request, seq):
        """
        Returns a fresh queue item instance
        :param request: :obj:`qlinklayer.EGP.EGPRequest`
            The request
        :param seq: int
            The queue item sequence number
        :return: :obj:`qlinklayer.localQueue._EGPLocalQueueItem`
        """
        return _LocalQueueItem(request, seq)

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
                self._seqs_removed.append(q.seq)

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

        if q.ready:
            # Item ready

            # Remove from queue
            self.queue.pop(self.popSeq, None)

            logger.debug("Removing item with seq={} to local queue".format(q.seq))

            if self.throw_events:
                logger.debug("Scheduling item added event now.")
                self._schedule_now(self._EVT_ITEM_REMOVED)
                self._seqs_removed.append(q.seq)

            # Increment lower bound of sequence numbers to return next
            self.popSeq = self._get_next_pop_seq()

            # Return item
            return q

    def _get_next_pop_seq(self):
        # Return the next item if available otherwise increment
        if len(self.queue) == 0:
            return None
        return min(self.queue.keys())

    def peek(self, seq=None):
        """
        Get item off the top of the queue without removing it or the item with sequence number seq
        :return:
        """
        if len(self.queue) == 0:
            # No items on queue
            return None

        if seq is None:
            # Get item off queue
            return self.queue[self.popSeq]
        else:
            return self.queue.get(seq)

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

    def ready(self, seq):
        """
        Mark the queue item with queue sequence number seq as ready
        """
        try:
            queue_item = self.queue[seq]
        except KeyError:
            logger.warning("Sequence number {} not found in local queue".format(seq))
            return

        if queue_item.acked:
            queue_item.ready = True
            logger.debug("Item with seq {} is ready to be scheduled".format(seq))
        else:
            logger.warning("Sequence number {} is not acked and cannot be made ready yet".format(seq))
            return

    def ack(self, seq):
        """
        Mark the queue item with queue sequence number seq as acknowledged by remote node
        """
        try:
            self.queue[seq].acked = True
            logger.debug("Item with seq {} is acknowledged".format(seq))
        except KeyError:
            logger.warning("Sequence number {} not found in local queue".format(seq))
            # Not in queue
            return


class EGPLocalQueue(LocalQueue):

    def __init__(self, qid=None, wsize=None, maxSeq=None, timeout_callback=None, throw_events=False):
        """
        Local queue used by EGP. Supports timeout per MHP cycle
        :param qid: int
            The queue ID
        :param wsize: int
            Maximum number of items to hold in the queue at any one time
        :param maxSeq: int
            Largest possible sequence number before wraparound
        :param timeout_callback: func
            Function to be called upon timeout, taking an _LocalQueueItem as argument
        :param throw_events: bool
            Whether to throw events or not when adding and removing entries (for data collection)
        """
        super(EGPLocalQueue, self).__init__(wsize=wsize, maxSeq=maxSeq, throw_events=throw_events)

        self.qid = qid

        if timeout_callback is None:
            self.timeout_callback = lambda queue_item: queue_item
        else:
            self.timeout_callback = timeout_callback

    def set_timeout_callback(self, timeout_callback):
        """
        Sets the timeout callback function for timeout of queue items
        :param timeout_callback: func
            Function to be called upon timeout, taking an _LocalQueueItem as argument
        :return:
        """
        self.timeout_callback = timeout_callback

    def update_mhp_cycle_number(self, current_cycle, max_cycle):
        """
        Goes over the elements in the queue and checks if they are ready to be scheduled or have timed out.
        :return: None
        """
        logger.debug("Updating to MHP cycle {}".format(current_cycle))
        for q_item in list(self.queue.values()):
            q_item.update_mhp_cycle_number(current_cycle, max_cycle)

    def get_new_queue_item(self, request, seq):
        """
        Returns a fresh queue item instance
        :param request: :obj:`qlinklayer.EGP.EGPRequest`
            The request
        :param seq: int
            The queue item sequence number
        :return: :obj:`qlinklayer.localQueue._EGPLocalQueueItem`
        """
        return _EGPLocalQueueItem(request, seq, self.qid, self.timeout_callback)


class WFQLocalQueue(EGPLocalQueue):
    def get_new_queue_item(self, request, seq):
        """
        Returns a fresh queue item instance
        :param request: :obj:`qlinklayer.EGP.EGPRequest`
            The request
        :param seq: int
            The queue item sequence number
        :return: :obj:`qlinklayer.localQueue._EGPLocalQueueItem`
        """
        return _WFQLocalQueueItem(request, seq, self.qid, self.timeout_callback)


class TimeoutLocalQueue(LocalQueue):
    def __init__(self, qid=None, wsize=None, maxSeq=None, throw_events=False):
        """
        Implements a local queue that supports timing out queue items asynchronously
        :param wsize: int
            Number of items that this queue holds at any given time
        :param maxSeq: int
            Maximum sequence number for items in the queue
        :param scheduleAfter: float
            Default schedule delay for added queue items
        """
        super(TimeoutLocalQueue, self).__init__(wsize=wsize, maxSeq=maxSeq, throw_events=throw_events)
        self._EVT_PROC_TIMEOUT = EventType("QUEUE ITEM REMOVED", "Triggers when an item has successfully been removed")
        self._EVT_SCHEDULE = EventType("LOCAL QUEUE SCHEDULE", "Triggers when a queue item is ready to be scheduled")
        self.timed_out_items = []
        self.ready_items = []
        self.qid = qid

    def get_new_queue_item(self, request, seq):
        """
        Returns a fresh queue item instance
        :param request: :obj:`qlinklayer.EGP.EGPRequest`
            The request
        :param seq: int
            The queue item sequence number
        :return: :obj:`qlinklayer.localQueue._EGPLocalQueueItem`
        """
        # Check if the item specifies a max queue time
        lifetime = getattr(request, 'max_time', 0.0)

        if lifetime == 0:
            lifetime = None

        return _TimeoutLocalQueueItem(request, seq, lifetime=lifetime)

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

            schedule_evt_handler = EventHandler(self._schedule_handler)
            self._wait_once(schedule_evt_handler, entity=queue_item, event_type=queue_item._EVT_SCHEDULE)

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
    def __init__(self, request, seq):
        self.request = request  # The request
        self.seq = seq          # The queue sequence number
        self.ready = False      # Whether the request is ready to be scheduled
        self.acked = False      # Whether the queue items has been acknowledged from remote node


class _EGPLocalQueueItem(_LocalQueueItem):
    def __init__(self, request, seq, qid, timeout_callback):
        """
        Local queue item that supports time outs based on MHP cycles
        :param request: :obj:`qlinklayer.egp.EGPRequest`
            The EGP request
        :param seq: int
            The queue sequence number
        :param timeout_callback: func
            Function to be called upon timeout, taking an _LocalQueueItem as argument
        """
        super(_EGPLocalQueueItem, self).__init__(request, seq)

        self.qid = qid

        # Schedule and timeout MHP cycle
        self.schedule_cycle = self.request.sched_cycle
        self.timeout_cycle = self.request.timeout_cycle

        # Keep track of number of pairs left
        self.num_pairs_left = self.request.num_pairs

        # Function to be called when request times out
        self.timeout_callback = timeout_callback

    def update_mhp_cycle_number(self, current_cycle, max_cycle):
        """
        Updates the current MHP cycle number. Updates ready accordingly and triggers a timeout.
        :return: None
        :param current_cycle: int
            The current MHP cycle
        :param max_cycle: int
            The max MHP cycle
        """
        logger.debug("Updating to MHP cycle {}".format(current_cycle))
        if self.timeout_cycle is not None:
            if check_schedule_cycle_bounds(current_cycle, max_cycle, self.timeout_cycle):
                logger.debug("Item timed out, calling callback")
                if not self.acked:
                    logger.warning("Item timed out before being acknowledged.")
                self.timeout_callback(self)
        if self.acked:
            if not self.ready:
                if self.schedule_cycle is not None:
                    if check_schedule_cycle_bounds(current_cycle, max_cycle, self.schedule_cycle):
                        self.ready = True
                        logger.debug("Item is ready to be scheduled")
                else:
                    self.ready = True
                    logger.debug("Item is ready to be scheduled")


class _WFQLocalQueueItem(_EGPLocalQueueItem):
    def __init__(self, request, seq, qid, timeout_callback):
        super().__init__(request, seq, qid, timeout_callback)

        # Store virt finish and est nr of MHP cycles per pair (used to update the virt finish)
        self.virtual_finish = request.init_virtual_finish
        self.cycles_per_pair = request.est_cycles_per_pair

    def update_virtual_finish(self):
        """
        Adds est_cycles_per_pair to virtual_finish
        :return: None
        """
        if not self.request.atomic:
            if self.virtual_finish is not None:
                self.virtual_finish += self.cycles_per_pair


class _TimeoutLocalQueueItem(_LocalQueueItem, Entity):
    def __init__(self, request, seq, lifetime=0.0):
        """
        Local queue item that supports time outs
        :param request: obj any
            The request information to store with this queue item
        :param seq: int
            The sequence number of this item in the containing queue
        :param lifetime: float
            The maximum amount of time this item can sit in the queue before timing out
        """
        super(_TimeoutLocalQueueItem, self).__init__(request=request, seq=seq)
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
        logger.debug("Scheduling ready event at {}.".format(self.scheduleAt))
        self._schedule_at(self.scheduleAt, self._EVT_SCHEDULE)
