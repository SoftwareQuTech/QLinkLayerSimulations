#
# Scheduler
#
import abc
from math import ceil
from collections import namedtuple
from netsquid.pydynaa import Entity, EventType
from netsquid.simutil import sim_time
from easysquid.toolbox import logger
from qlinklayer.distQueue import EGPDistributedQueue
from qlinklayer.toolbox import LinkLayerException


SchedulerRequest = namedtuple("Scheduler_request", ["sched_cycle", "timeout_cycle", "min_fidelity", "purpose_id", "create_id", "num_pairs", "priority", "store", "atomic", "measure_directly", "master_request"], defaults=(0,)*7 + (True, False, False, True))

SchedulerGen = namedtuple("Scheduler_gen", ["flag", "aid", "comm_q", "storage_q", "param"], defaults=(False,) + (None,)*4)


class Scheduler(metaclass=abc.ABCMeta):
    """
    Stub for a scheduler to decide how we assign and consume requests.
    """
    @abc.abstractmethod
    def add_request(self, request):
        """
        Adds new request
        :param request: Any
        :return: Any
        """
        pass

    @abc.abstractmethod
    def next(self):
        """
        Returns next request (if any) to be processed.
        :return: Any
        """
        pass


class RequestScheduler(Scheduler, Entity):
    """
    Stub for a scheduler to decide how we assign and consume elements of the queue.
    """

    def __init__(self, distQueue, qmm):
        # Distributed Queue to schedule from
        self.distQueue = distQueue

        # Queue service timeout handler
        self._EVT_REQ_TIMEOUT = EventType("REQ TIMEOUT", "Triggers when request was not completed in time")

        # Quantum memory management
        self.qmm = qmm
        self.my_free_memory = self.qmm.get_free_mem_ad()
        self.other_mem = (0, 0)

        # Generation tracking
        self.curr_aid = None                        # The absolute queue ID of the current request being handled
        self.curr_gen = None                        # The current generation being handled
        self.timed_out_requests = []                # List of requests that have timed out
        self.prev_requests = []                     # Previous requests (for filtering delayed communications)
        self.max_prev_requests = 5                  # Number of previous requests to store

        # Suspended generation tracking
        self.num_suspended_cycles = 0

        # Timing information
        self.max_mhp_cycle_number = 2**64       # Max cycle number for scheduling
        self.mhp_cycle_number = 0               # Current cycle number
        self.mhp_cycle_period = 0.0             # Cycle period
        self.mhp_cycle_offset = 10              # Offset to accompany for communication variance
        self.mhp_full_cycle = 0.0               # Full MHP generation + communication RTT period
        self.local_trigger = 0.0                # Trigger for local MHP
        self.remote_trigger = 0.0               # Trigger for remote MHP

        if isinstance(distQueue, EGPDistributedQueue):
            distQueue.set_timeout_callback(self._handle_item_timeout)

    def configure_mhp_timings(self, cycle_period, full_cycle, local_trigger, remote_trigger, max_mhp_cycle_number=None,
                              mhp_cycle_number=None, mhp_cycle_offset=None):
        """
        Provides scheduler relevant timing information for external MHP to help properly schedule entanglement
        generation
        :param full_cycle: float
            The length of a full mhp cycle including the classical communication time
        :param cycle_period: float
            The duration of an MHP timeStep
        :param local_trigger: float
            The trigger offset for the local MHP
        :param remote_trigger: float
            The trigger offset for the remote MHP
        :param max_mhp_cycle_number: int
            MHP cycle number wrap around
        :param mhp_cycle_number: int
            Current MHP cycle number
        :param mhp_cycle_offset: int
            How many MHP cycles should be passed before a request is considered ready
        :return:
        """
        self.local_trigger = local_trigger
        self.remote_trigger = remote_trigger
        self.mhp_cycle_period = cycle_period
        self.mhp_full_cycle = full_cycle
        if max_mhp_cycle_number is not None:
            self.max_mhp_cycle_number = max_mhp_cycle_number
        if mhp_cycle_number is not None:
            self.mhp_cycle_number = mhp_cycle_number
        if mhp_cycle_offset is not None:
            self.mhp_cycle_offset = mhp_cycle_offset

    def inc_cycle(self):
        """
        Increments the locally tracked MHP cycle number.  Checks if any of the items in the backlog can be
        scheduled
        """
        # Calculate the new cycle number mod the max
        self.mhp_cycle_number = (self.mhp_cycle_number + 1) % self.max_mhp_cycle_number
        if self.mhp_cycle_number == 0:  # Skip MHP cycle number 0
            self.mhp_cycle_number = 1
        logger.debug("Incremented MHP cycle to {}".format(self.mhp_cycle_number))
        self.distQueue.update_mhp_cycle_number(self.mhp_cycle_number, self.max_mhp_cycle_number)

        # Decrement any suspended cycles
        if self.num_suspended_cycles > 0:
            self.num_suspended_cycles -= 1

    def get_schedule_cycle(self, request):
        """
        Estimates a reasonable MHP cycle number when the request can begin processing
        :param request: obj `~qlinklayer.egp.EGPRequest
            The request to get the schedule cycle for.  Currently unused but available for more sophisticated
            decision making
        :return: int
            The cycle number that this request should be scheduled at
        """
        # The bottleneck on how early we can start the request depends on how frequent the cycles occur and the time
        # it may take to propagate the request to the remote queue
        bottleneck = max(self.mhp_cycle_period, 2 * self.distQueue.comm_delay)

        # Provide some extra buffer room to accompany variance in communication time
        cycle_delay = self.mhp_cycle_offset

        # Compensate for the bottleneck
        if self.mhp_cycle_period:
            cycle_delay += ceil(bottleneck / self.mhp_cycle_period)
            cycle_delay += ceil(max(0.0, self.remote_trigger - self.local_trigger) / self.mhp_cycle_period)

        cycle_number = (self.mhp_cycle_number + cycle_delay) % self.max_mhp_cycle_number
        if cycle_number == 0:
            cycle_number = 1

        return cycle_number

    @staticmethod
    def _get_scheduler_request(egp_request, create_id, sched_cycle, timeout_cycle, master_request):
        """
        Creates a Scheduler request from a EGP request plus additional arguments.

        :param egp_request: :obj:`~qlinklayer.egp.EGPRequest`
        :param create_id: int
        :param sched_cycle: int
        :param timeout_cycle: int
        :param master_request: bool
        :return: :obj:`~qlinklayer.scheduler.SchedulerRequest`
        """
        scheduler_request = SchedulerRequest(sched_cycle=sched_cycle, timeout_cycle=timeout_cycle, min_fidelity=egp_request.min_fidelity, purpose_id=egp_request.purpose_id, create_id=create_id, num_pairs=egp_request.num_pairs, priority=egp_request.priority, store=egp_request.store, atomic=egp_request.atomic, measure_directly=egp_request.measure_directly, master_request=master_request)

        return scheduler_request

    def add_request(self, egp_request, create_id=0):
        """
        Adds a request to the distributed queue
        :param request: obj `~qlinklayer.egp.EGPRequest`
            The request to be added
        :param create_id: int
            The assigned create ID of this request
        """
        # Decide which cycle the request should begin processing
        schedule_cycle = self.get_schedule_cycle(egp_request)

        # Store the request into the queue
        try:
            qid = self.get_queue(egp_request)
        except LinkLayerException:
            logger.warning("Scheduler could not get a valid queue ID for request.")
            return False

        try:
            timeout_cycle = self.get_timeout_cycle(egp_request)
        except LinkLayerException:
            logger.warning("Specified timeout ({}) is longer then the mhp_cycle_period * max_mhp_cycle_number = {}".format(egp_request.max_time, self.mhp_cycle_period * self.max_mhp_cycle_number))
            return False

        scheduler_request = self._get_scheduler_request(egp_request, create_id, schedule_cycle, timeout_cycle, master_request=self.distQueue.master)

        try:
            self.distQueue.add(scheduler_request, qid)
        except LinkLayerException:
            logger.warning("Could not add request to queue.")
            return False

        return True

    def get_timeout_cycle(self, request):
        """
        Gets the MHP cycle where this request should timeout
        :param request: :obj:`qlinklayer.egp.EGPRequest`
        :return: int
        """
        max_time = request.max_time

        if max_time == 0:
            return 0

        # Compute how many MHP cycles this corresponds to
        if self.mhp_cycle_period == 0:
            raise ValueError("MHP cycle period cannot be zero when using timeouts")

        # Get current time
        now = sim_time()

        # Compute time since last MHP trigger
        t_left = (now - self.local_trigger) % self.mhp_cycle_period

        # Compute time until next MHP trigger
        t_right = self.mhp_cycle_period - t_left

        # Compute how many MHP from now that this request should time out
        if max_time < t_right:
            # This request will timeout before the next MHP cycle
            mhp_cycles = 1
        else:
            mhp_cycles = int((max_time - t_right) / self.mhp_cycle_period) + 2

        if mhp_cycles >= self.max_mhp_cycle_number:
            raise LinkLayerException("Specified timeout ({}) is longer then the mhp_cycle_period * max_mhp_cycle_number = {}".format(max_time, self.mhp_cycle_period * self.max_mhp_cycle_number))

        timeout_mhp_cycle = self.mhp_cycle_number + (mhp_cycles % self.max_mhp_cycle_number)
        if timeout_mhp_cycle == 0:
            timeout_mhp_cycle = 1

        return timeout_mhp_cycle

    def suspend_generation(self, t):
        """
        Instructs the scheduler to suspend generation for some specified time.
        :param t: float
            The amount of simulation time to suspend entanglement generation for
        """
        # Compute the number of suspended cycles
        if not self.mhp_cycle_period:
            num_suspended_cycles = 1
        else:
            num_suspended_cycles = ceil(t / self.mhp_cycle_period)

        # Update the number of suspended cycles if it exceeds the amount we are currently suspended for
        if num_suspended_cycles > self.num_suspended_cycles:
            logger.debug("Suspending generation for {} cycles".format(num_suspended_cycles))
            self.num_suspended_cycles = num_suspended_cycles

    def suspended(self):
        """
        Checks if the scheduler is currently suspending generation
        :return: bool
            True/False whether we are suspended or not
        """
        return self.num_suspended_cycles > 0

    def update_other_mem_size(self, mem):
        """
        Stores the other peer's free memory locally for use with scheduling
        :param mem: int
            The amount of free memory locations the other node has
        """
        self.other_mem = mem

    def other_has_resources(self):
        """
        Tells if our peer has any resources for the entanglement process
        :return: bool
            Whether/not peer has resources
        """
        return self.other_mem != (0, 0)

    def get_queue(self, request):
        """
        Determines which queue id to add the next request to.
        """
        # TODO - now we always use qid=0
        return 0

    def next_pop(self):
        """
        Determines which queue id to server next. 
        """
        # TODO - now we always use qid=0
        return 0

    def next(self):
        """
        Returns the next request (if any) to be processed.  Defaults to an information pass request.
        :return: tuple
            Tuple containing the request information for the MHP or None if no request
        """
        logger.debug("Getting next item from scheduler")
        # Get our available memory
        self.my_free_memory = self.qmm.get_free_mem_ad()

        next_gen = self.get_default_gen()

        if self.qmm.is_busy():
            logger.debug("QMM is currently busy")

        elif self.suspended():
            logger.debug("Generation is currently suspended")

        elif self.curr_gen:
            next_gen = self.curr_gen
            logger.debug("Scheduler has next gen {}".format(next_gen))

        else:
            next_gen = self.get_next_gen_template()
            logger.debug("Scheduler has next gen {}".format(next_gen))

        # If we are storing the qubit prevent additional attempts until we have a reply or have timed out
        if not self.is_handling_measure_directly() and next_gen.flag:
            suspend_time = self.mhp_full_cycle
            logger.debug("Next generation attempt after {}".format(suspend_time))
            self.suspend_generation(suspend_time)

        return next_gen

    def get_default_gen(self):
        """
        Returns the default gen template which is an info request
        :return: tuple
            Represents a gen template for an info request
        """
        return SchedulerGen(flag=False, aid=None, comm_q=None, storage_q=None, param=None)

    def is_generating(self):
        """
        Tells if the scheduler is currently handling a generation
        :return: bool
            True/False
        """
        # Check the current generation to see if the flag is True
        return self.curr_gen.flag if self.curr_gen else None

    def is_generating_aid(self, aid):
        """
        Returns True if the aid is currently being processed, otherwise False.
        :param aid: tuple(int, int)
            The absolute queue ID
        :return: bool
        """
        if self.curr_gen is None:
            return False
        if self.curr_aid == aid:
            return True
        else:
            return False

    def curr_storage_id(self):
        """
        Returns the storage id for the current generation if any
        :return: int
            Storage id in qmem
        """
        return self.curr_gen.storage_q if self.curr_gen else None

    def has_request(self, aid):
        """
        Checks if the provided aid corresponds to a request which still needs to be processed.
        :param aid: tuple(int, int)
            The absolute queue ID
        :return: bool
            True: If the aid exists in the dist queue
            False: Otherwise
        """
        try:
            queue_item = self.distQueue.local_peek(aid)
            if queue_item is None:
                return False
            else:
                return True
        except LinkLayerException:
            return False

    def get_request(self, aid):
        """
        Returns the request corresponding to this absolute queue ID, if exists, otherwise None.
        :param aid: tuple(int, int)
            The absolute queue ID
        :return: :obj:`~qlinklayer.egp.EGPRequest` or None
        """
        try:
            queue_item = self.distQueue.local_peek(aid)
            if queue_item is None:
                return None
            else:
                return queue_item.request
        except LinkLayerException:
            return None

    def get_next_gen_template(self):
        """
        Returns the next entanglement generation template to process.  Verifies that there are resources available
        before filling in the template and passing it back to be used.
        :return: tuple
            Represents the information to be used for the next entanglement generation attempts
        """

        if self.curr_gen:
            logger.debug("Currently processing generation")
            return self.get_default_gen()

        aid, request = self._get_next_request()
        if aid is None and request is None:
            return self.get_default_gen()

        # Check if we have the resources to fulfill this generation
        if self._has_resources_for_gen(request):
            logger.debug("Filling next available gen template")

            # Reserve resources in the quantum memory
            comm_q, storage_q = self.reserve_resources_for_gen(request)
            self.my_free_memory = self.qmm.get_free_mem_ad()

            # Convert to a tuple
            next_gen = SchedulerGen(flag=True, aid=aid, comm_q=comm_q, storage_q=storage_q, param=None)

            logger.debug("Created gen request {}".format(next_gen))
            self.curr_gen = next_gen
            self.curr_aid = aid
            return next_gen

        else:
            return self.get_default_gen()

    def mark_gen_completed(self, aid):
        """
        Marks a generation performed by the EGP as completed and cleans up any remaining state.
        :param aid: tuple of  int, int
            Contains the aid used for the generation
        :return:
        """
        logger.debug("Marking aid {} as completed".format(aid))
        if self.is_generating_aid(aid):
            # Get the used qubit info and free unused resources
            comm_q = self.curr_gen.comm_q
            storage_q = self.curr_gen.storage_q
            if comm_q != storage_q:
                self.qmm.free_qubit(comm_q)

            self.curr_gen = None
            # Update number of remaining pairs on request, remove if completed
            self.decrement_num_pairs(aid)
        else:
            logger.warning("Marking gen completed for inactive request")
            self.decrement_num_pairs(aid)

    def decrement_num_pairs(self, aid):
        """
        Decrements the remaining number of pairs of the request with the given absolute queue ID
        :param aid: tuple(int, int)
            The absolute queue ID
        :return: bool
        """
        try:
            queue_item = self.distQueue.local_peek(aid)
        except LinkLayerException as err:
            logger.warning("Could not find queue item with aid = {}, when trying to decrement number of remaining pairs.".format(aid))
            raise err

        if queue_item.num_pairs_left > 1:
            logger.debug("Decrementing number of remaining pairs")
            queue_item.num_pairs_left -= 1

        elif queue_item.num_pairs_left == 1:
            logger.debug("Generated final pair, removing request")
            self.clear_request(aid=aid)
        else:
            raise LinkLayerException("Current request with aid = {} has invalid number of remaining pairs.".format(aid))

    def previous_request(self, aid):
        """
        Checks if the provided AID was a previous request.  Used by higher layers for determining if communications were
        delayed and should be filtered out
        :param aid: tuple of (int, int)
            The absolute queue ID corresponding to the request to check
        :return: bool
            Whether the specified AID was a previously processed request
        """
        return aid in self.prev_requests

    def clear_request(self, aid):
        """
        Clears all stored request information: Outstanding generations, current generation, stored request
        :param aid: tuple of (int, int)
            The absolute queue id corresponding to the request
        :return: list of tuples
            The removed outstanding generations
        """
        # Add the request to the previous request list, drop any that are too old
        self.prev_requests.append(aid)
        if len(self.prev_requests) > self.max_prev_requests:
            self.prev_requests.pop(0)

        # Check if this is a request currently being processed
        if self.is_generating_aid(aid):
            logger.debug("Cleared current gen")

            # Vacate the reserved locations within the QMM
            self.qmm.vacate_qubit(self.curr_gen.comm_q)
            self.qmm.vacate_qubit(self.curr_gen.storage_q)
            self.curr_gen = None

        # If this item timed out need to remove from the queue
        qid, qseq = aid
        if self.distQueue.contains_item(qid, qseq):
            queue_item = self.distQueue.remove_item(qid, qseq)
            if queue_item is None:
                logger.error("Attempted to remove nonexistent item {} from local queue {}!".format(qseq, qid))
            else:
                if self.curr_aid == aid:
                    self.curr_aid = None

                # Remove queue item from pydynaa
                if isinstance(queue_item, Entity):
                    logger.debug("Removing local queue item from pydynaa")
                    queue_item.remove()

    def _has_resources_for_gen(self, request):
        """
        Checks if we have the resources to service a generation request.
        :return: bool
            True/False whether we have resources
        """
        other_free_comm, other_free_storage = self.other_mem
        my_free_comm, my_free_storage = self.my_free_memory

        # Verify whether we have the resources to satisfy this request
        logger.debug("Checking if we can satisfy next gen")
        if not other_free_comm:
            logger.debug("Peer memory has no available communication qubits!")
            return False

        elif not my_free_comm > 0:
            logger.debug("Local memory has no available communication qubits!")
            return False

        if request.store and not request.measure_directly:
            if not other_free_storage:
                logger.debug("Requested storage but peer memory has no available storage qubits!")
                return False

            elif not my_free_storage:
                logger.debug("Requested storage but local memory has no available storage qubits!")
                return False

        return True

    def is_handling_measure_directly(self):
        """
        Checks if the scheduler is managing a measure_directly request
        :return: bool
        """
        if self.curr_aid is None:
            return False

        # Check if current request is measure directly
        if self.distQueue.local_peek(self.curr_aid).request.measure_directly:
            return True
        else:
            return False

    def reserve_resources_for_gen(self, request):
        """
        Allocates the appropriate communication qubit/storage qubit given the specifications of the request
        :param request: obj `~easysquid.egp.EGPRequest`
            Specifies whether we want to store the entangled qubit or keep it in the communication id
        :return: int, int
            Qubit IDs to use for the communication process ad storage process
        """
        if request.store and not request.measure_directly:
            comm_q, storage_q = self.qmm.reserve_entanglement_pair()
        else:
            comm_q = self.qmm.reserve_communication_qubit()
            storage_q = comm_q
        return comm_q, storage_q

    def _get_next_request(self):
        """
        Gets the next request for processing
        :return: tuple of (tuple, request)
            The absolute queue ID and request (if any)
        """
        # Simply process the requests in FIFO order (for now...)
        queue_item = self.distQueue.queueList[0].peek()
        if queue_item is None:
            return None, None
        if queue_item.ready:
            aid = queue_item.qid, queue_item.seq
            return aid, queue_item.request
        else:
            return None, None

    def _handle_item_timeout(self, queue_item):
        """
        Timeout handler that is triggered when a queue item times out.  If the item has not been serviced yet
        then the stored request and it's information is removed.
        :param queue_item: obj `~qlinklayer.localQueue._LocalQueueItem`
            The local queue item
        """
        request = queue_item.request
        self.timed_out_requests.append(request)
        aid = queue_item.qid, queue_item.seq
        self.clear_request(aid)
        self._schedule_now(self._EVT_REQ_TIMEOUT)

    def _reset_outstanding_req_data(self):
        """
        Resets the variables storing the data for data collection
        :return:
        """
        self._last_aid_added = None
        self._last_aid_removed = None
