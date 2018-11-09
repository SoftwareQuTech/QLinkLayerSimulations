#
# Scheduler
#
from math import ceil
from netsquid import pydynaa
from easysquid.toolbox import logger


class RequestScheduler(pydynaa.Entity):
    """
    Stub for a scheduler to decide how we assign and consume elements of the queue.
    """

    def __init__(self, distQueue, qmm):
        # Distributed Queue to schedule from
        self.distQueue = distQueue

        # Queue service timeout handler
        self.service_timeout_handler = pydynaa.EventHandler(self._handle_item_timeout)
        self._EVT_REQ_TIMEOUT = pydynaa.EventType("REQ TIMEOUT", "Triggers when request was not completed in time")

        # Queue item schedule handler
        self.schedule_handler = pydynaa.EventHandler(self._schedule_request)
        self._wait(self.schedule_handler, entity=self.distQueue, event_type=self.distQueue._EVT_SCHEDULE)

        # Quantum memory management
        self.qmm = qmm
        self.my_free_memory = self.qmm.get_free_mem_ad()
        self.other_mem = (0, 0)

        # Generation tracking
        self.requests = {}                          # Tracks requests that can be scheduled
        self.schedule_backlog = {}                  # Backlog of requests waiting to be scheduled
        self.curr_request = None                    # The current request being handled
        self.curr_gen = None                        # The current generation being handled
        self.default_gen = self.get_default_gen()   # The default generation
        self.outstanding_gens = []                  # List of outstanding generations
        self.outstanding_items = {}                 # List of outstanding requests
        self.timed_out_requests = []                # List of requests that have timed out
        self.prev_requests = []                     # Previous requests (for filtering delayed communications)
        self.max_prev_requests = 5                  # Number of previous requests to store

        # Suspended generation tracking
        self.num_suspended_cycles = 0

        # Timing information
        self.max_mhp_cycle_number = 2**16       # Max cycle number for scheduling
        self.mhp_cycle_number = 0               # Current cycle number
        self.mhp_cycle_period = 0.0             # Cycle period
        self.mhp_cycle_offset = 10              # Offset to accompany for communication variance
        self.mhp_full_cycle = 0.0               # Full MHP generation + communication RTT period
        self.local_trigger = 0.0                # Trigger for local MHP
        self.remote_trigger = 0.0               # Trigger for remote MHP

    def configure_mhp_timings(self, cycle_period, full_cycle, local_trigger, remote_trigger):
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
        :return:
        """
        self.local_trigger = local_trigger
        self.remote_trigger = remote_trigger
        self.mhp_cycle_period = cycle_period
        self.mhp_full_cycle = full_cycle

    def inc_cycle(self):
        """
        Increments the locally tracked MHP cycle number.  Checks if any of the items in the backlog can be
        scheduled
        """
        # Calculate the new cycle number mod the max
        self.mhp_cycle_number = (self.mhp_cycle_number + 1) % self.max_mhp_cycle_number
        logger.debug("Incremented MHP cycle to {}".format(self.mhp_cycle_number))

        # Decrement any suspended cycles
        if self.num_suspended_cycles > 0:
            self.num_suspended_cycles -= 1

        # Comb through the backlog for requests to schedule
        backlog = list(self.schedule_backlog.items())
        for key, request in backlog:
            if self.check_schedule_cycle_bounds(request.sched_cycle):
                aid = self.outstanding_items[key]
                self.requests[aid] = request
                self.schedule_backlog.pop(key)

    def check_schedule_cycle_bounds(self, sched_cycle):
        """
        Checks whether the provided schedule cycle slot falls within our accepting window
        :param sched_cycle: int
            The mhp cycle number to check
        """
        right_boundary = self.mhp_cycle_number
        left_boundary = (right_boundary - self.max_mhp_cycle_number // 2) % self.max_mhp_cycle_number

        # Check if the provided time falls within our modular window
        if left_boundary < right_boundary:
            return left_boundary <= sched_cycle <= right_boundary
        else:
            return sched_cycle <= right_boundary or sched_cycle >= left_boundary

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

        return (self.mhp_cycle_number + cycle_delay) % self.max_mhp_cycle_number

    def add_request(self, request):
        """
        Adds a request to the distributed queue
        :param request: obj `~qlinklayer.egp.EGPRequest`
            The request to be added
        """
        # Decide which cycle the request should begin processing
        schedule_cycle = self.get_schedule_cycle(request)

        # Store the request into the queue
        qid = self.get_queue(request)
        request.add_sched_cycle(schedule_cycle)
        self.distQueue.add(request, qid)

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
            next_gen = self.default_gen

        elif self.suspended():
            logger.debug("Generation is currently suspended")
            next_gen = self.default_gen

        elif self.curr_gen:
            next_gen = self.curr_gen
            logger.debug("Scheduler has next gen {}".format(next_gen))

        # If there are outstanding generations and we have memory, overwrite with a request
        elif self.outstanding_gens:
            next_gen = self.get_next_gen_template()
            logger.debug("Scheduler has next gen {}".format(next_gen))

        # If there are outstanding requests then create the gen templates and try to get one
        elif self.outstanding_items:
            logger.debug("No available generations, processing outstanding items")
            self._process_outstanding_items()
            if self.outstanding_gens:
                next_gen = self.get_next_gen_template()
                logger.debug("Scheduler has next gen {}".format(next_gen))

        else:
            logger.debug("Scheduler has no items to process")

        # If we are storing the qubit prevent additional attempts until we have a reply or have timed out
        if not self.handling_measure_directly() and next_gen[0]:
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
        return False, None, None, None, None

    def generating(self):
        """
        Tells if the scheduler is currently handling a generation
        :return: bool
            True/False
        """
        # Check the current generation to see if the flag is True
        return self.curr_gen[0] if self.curr_gen else None

    def curr_aid(self):
        """
        Returns the aid for the current generation request if any else none
        :return: tuple of (int, int)
            Specifies the (QID, QSEQ) corresponding to the current request if any
        """
        return self.curr_gen[1] if self.curr_gen else None

    def curr_storage_id(self):
        """
        Returns the storage id for the current generation if any
        :return: int
            Storage id in qmem
        """
        return self.curr_gen[3] if self.curr_gen else None

    def get_next_gen_template(self):
        """
        Returns the next entanglement generation template to process.  Verifies that there are resources available
        before filling in the template and passing it back to be used.
        :return: tuple
            Represents the information to be used for the next entanglement generation attempts
        """
        logger.debug("Filling next available gen template")

        # Obtain the next template
        gen_template = self.outstanding_gens[0]

        # Obtain the request to check for generation options
        aid = gen_template[1]
        request = self.get_request(aid)

        # Check if we have the resources to fulfill this generation
        if self._has_resources_for_gen(request):
            # Reserve resources in the quantum memory
            comm_q, storage_q = self.reserve_resources_for_gen(request)
            self.my_free_memory = self.qmm.get_free_mem_ad()

            # Fill in the template
            gen_template[2] = comm_q
            gen_template[3] = storage_q

            # Convert to a tuple
            next_gen = tuple(gen_template)

            logger.debug("Created gen request {}".format(next_gen))
            self.outstanding_gens.pop(0)
            self.curr_gen = next_gen

        else:
            next_gen = self.get_default_gen()

        return next_gen

    def mark_gen_completed(self, aid):
        """
        Marks a generation performed by the EGP as completed and cleans up any remaining state.
        :param aid: tuple of  int, int
            Contains the aid used for the generation
        :return:
        """
        logger.debug("Marking aid {} as completed".format(aid))
        if self.curr_gen and self.curr_gen[1] == aid:
            # Get the used qubit info and free unused resources
            comm_q, storage_q = self.curr_gen[2:4]
            if comm_q != storage_q:
                self.qmm.free_qubit(comm_q)

            self.curr_gen = None
            # Update number of remaining pairs on request, remove if completed
            if self.curr_request.num_pairs > 1:
                logger.debug("Decrementing number of remaining pairs")
                self.curr_request.num_pairs -= 1

            elif self.curr_request.num_pairs == 1:
                logger.debug("Generated final pair, removing request")
                self.clear_request(aid=aid)

            else:
                raise Exception("Current request has invalid number of remaining pairs")

        else:
            logger.warning("Marking gen completed for inactive request")
            req = self.get_request(aid)
            req.num_pairs -= 1

    def get_request(self, aid):
        """
        Retrieves the stored request if the scheduler still contains it
        :param aid: tuple of (int, int)
            The absolute queue id corresponding to the request
        :return: obj `~qlinklayer.egp.EGPRequest`
            The request corresponding to this absolute queue id
        """
        return self.requests.get(aid)

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
        # Remove any outstanding generations
        removed_gens = self._prune_request_generations(aid=aid)

        # Add the request to the previous request list, drop any that are too old
        self.prev_requests.append(aid)
        if len(self.prev_requests) > self.max_prev_requests:
            self.prev_requests.pop(0)

        # Check if this is a request currently being processed
        if self.curr_aid() == aid:
            logger.debug("Cleared current gen")
            removed_gens.append(self.curr_gen)

            # Vacate the reserved locations within the QMM
            self.qmm.vacate_qubit(self.curr_gen[2])
            self.qmm.vacate_qubit(self.curr_gen[3])
            self.curr_gen = None

        logger.debug("Removed remaining generations for {}: {}".format(aid, removed_gens))
        request = self.requests.pop(aid, None)

        # Remove request info if available
        if request:
            key = (request.create_id, request.otherID)
            self.outstanding_items.pop(key, None)

            if self.curr_request == request:
                self.curr_request = None

        # If this item timed out need to remove from the queue
        qid, qseq = aid
        if self.distQueue.contains_item(qid, qseq):
            queue_item = self.distQueue.remove_item(qid, qseq)
            if queue_item is None:
                logger.error("Attempted to remove nonexistent item {} from local queue {}!".format(qseq, qid))
            else:
                # Remove queue item from pydynaa
                queue_item.remove()

        return removed_gens

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

    def handling_measure_directly(self):
        """
        Checks if the scheduler is managing a measure_directly request
        :return: bool
        """
        return self.curr_request and self.curr_request.measure_directly

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

    def _schedule_request(self, evt):
        """
        Event handler for scheduling queue items from the distributed queue that are ready to be serviced.  Crafts
        generation templates to be filled in the future and stores request information for tracking.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        # Get the queue that has an item ready
        queue = evt.source
        qid, queue_item = queue.ready_items.pop(0)
        qseq = queue_item.seq

        # Store the request under the absolute queue id
        aid = (qid, qseq)
        request = queue_item.request

        # Store the absolute queue id under a unique request key
        logger.debug("Scheduling request {}".format(aid))

        key = (request.create_id, request.otherID)
        self.outstanding_items[key] = aid
        self.schedule_backlog[key] = request

        if queue_item.lifetime:
            self._wait_once(self.service_timeout_handler, entity=queue_item, event_type=queue_item._EVT_TIMEOUT)

    def _get_next_request(self):
        """
        Gets the next request for processing
        :return: tuple of (tuple, request)
            The absolute queue ID and request (if any)
        """
        # Simply process the requests in FIFO order (for now...)
        sorted_request_aids = sorted(self.requests.keys(), key=lambda aid: aid[1])
        if sorted_request_aids:
            aid = sorted_request_aids[0]
            return aid, self.requests[aid]
        return None, None

    def _process_outstanding_items(self):
        """
        Makes decisions on which request to process next.  Currently sorts the requests by queue sequence number
        and chooses the lowest to process first.
        :return:
        """
        # Check if we are already processing a generation request or if we have any requests to service
        if not self.requests:
            logger.debug("No available requests to process")
            return

        if self.curr_gen:
            logger.debug("Currently processing generation")
            return

        next_aid, next_request = self._get_next_request()
        if next_aid is None and next_request is None:
            return

        logger.debug("Creating gen templates for request {}".format(vars(next_request)))

        # Create templates for all generations part of this request
        for i in range(next_request.num_pairs):
            gen_template = [True, next_aid, None, None, None]
            self.outstanding_gens.append(gen_template)

        self.curr_request = next_request

    def _handle_item_timeout(self, evt):
        """
        Timeout handler that is triggered when a queue item times out.  If the item has not been serviced yet
        then the stored request and it's information is removed.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        queue_item = evt.source
        request = queue_item.request
        logger.debug("Removing local queue item from pydynaa")
        key = (request.create_id, request.otherID)
        if key in self.outstanding_items:
            logger.error("Failed to service request in time, clearing")
            aid = self.outstanding_items[key]
            if aid in self.requests.keys():
                request = self.requests[aid]
            self.timed_out_requests.append(request)
            self.clear_request(aid=aid)
            logger.debug("Scheduling request timeout event now.")
            self._schedule_now(self._EVT_REQ_TIMEOUT)
        self.outstanding_items.pop(key, None)
        self.schedule_backlog.pop(key, None)

    def _prune_request_generations(self, aid):
        """
        Filters the oustanding generations list of any generation requests corresponding to the provided absolute queue
        id.  To be used when clearing a request
        :param aid: tuple of (int, int)
            Absolute queue ID of the request we want to filter generations for
        """
        logger.debug("Pruning remaining generations for aid {}".format(aid))
        removed = list(filter(lambda gen: gen[1] == aid, self.outstanding_gens))
        self.outstanding_gens = list(filter(lambda gen: gen[1] != aid, self.outstanding_gens))
        logger.debug("Pruned generations {}".format(removed))
        return removed

    def _reset_outstanding_req_data(self):
        """
        Resets the variables storing the data for data collection
        :return:
        """
        self._last_aid_added = None
        self._last_aid_removed = None
