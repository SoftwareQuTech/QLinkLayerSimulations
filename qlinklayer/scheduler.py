#
# Scheduler
#
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
        self.requests = {}
        self.curr_request = None
        self.curr_gen = None
        self.default_gen = self.get_default_gen()
        self.outstanding_gens = []
        self.outstanding_items = {}
        self.timed_out_requests = []
        self._suspend = False
        self.resumeID = -1

    def suspend_generation(self, t):
        """
        Instructs the scheduler to suspend generation for some specified time.
        :param t: float
            The amount of simulation time to suspend entanglement generation for
        """
        self._suspend = True

        # Set up an event handler to resume entanglement generation
        from functools import partial
        self.resumeID += 1
        self.resume_handler = pydynaa.EventHandler(partial(self._resume_generation_handler, resumeID=self.resumeID))
        EVT_RESUME = pydynaa.EventType("RESUME", "Triggers when we believe peer finished correction")
        self._wait_once(self.resume_handler, entity=self, event_type=EVT_RESUME)
        logger.debug("Scheduling resume event after {}.".format(t))
        self._schedule_after(t, EVT_RESUME)

    def resume_generation(self):
        self._suspend = False

    def _resume_generation_handler(self, evt, resumeID):
        """
        Callback handler to flip the suspend flag and allow the scheduler to resume entanglement generation
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        if resumeID == self.resumeID:
            logger.debug("Resuming generation")
            self._suspend = False

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

        if self.qmm.is_busy() or self._suspend:
            next_gen = self.default_gen

        elif self.curr_gen:
            next_gen = self.curr_gen

        # If there are outstanding generations and we have memory, overwrite with a request
        elif self.outstanding_gens:
            next_gen = self.get_next_gen_template()

        # If there are outstanding requests then create the gen templates and try to get one
        elif self.outstanding_items:
            self._process_outstanding_items()
            next_gen = self.get_next_gen_template()

        return next_gen

    def get_default_gen(self):
        """
        Returns the default gen template which is an info request
        :return: tuple
            Represents a gen template for an info request
        """
        self.my_free_memory = self.qmm.get_free_mem_ad()
        return False, None, None, None, None, self.my_free_memory

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
            # Compute our new free memory after reservation
            free_memory = self.qmm.get_free_mem_ad()
            gen_template[-1] = free_memory

            # Reserve resources in the quantum memory
            comm_q, storage_q = self.reserve_resources_for_gen(request)

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

    def mark_gen_completed(self, gen_id):
        """
        Marks a generation performed by the EGP as completed.
        :param gen_id: tuple of (tuple, int, int)
            Contains the aid, comm_q, and storage_q used for the generation
        :return:
        """
        logger.debug("Marking gen id {} as completed".format(gen_id))
        if self.curr_gen and self.curr_gen[1:4] == gen_id:
            self.curr_gen = None

    def get_request(self, aid):
        """
        Retrieves the stored request if the scheduler still contains it
        :param aid: tuple of (int, int)
            The absolute queue id corresponding to the request
        :return: obj `~qlinklayer.egp.EGPRequest`
            The request corresponding to this absolute queue id
        """
        return self.requests.get(aid)

    def clear_request(self, aid):
        """
        Clears all stored request information: Outstanding generations, current generation, stored request
        :param aid: tuple of (int, int)
            The absolute queue id corresponding to the request
        :return: list of tuples
            The removed outstanding generations
        """
        removed_gens = self._prune_request_generations(aid=aid)

        if self.curr_gen and self.curr_gen[1] == aid:
            logger.debug("Cleared current gen")
            removed_gens.append(self.curr_gen)
            self.qmm.vacate_qubit(self.curr_gen[2])
            self.qmm.vacate_qubit(self.curr_gen[3])
            self.curr_gen = None

        logger.debug("Removed remaining generations for {}: {}".format(aid, removed_gens))
        request = self.requests.pop(aid, None)
        if request:
            key = (request.create_id, request.otherID)
            self.outstanding_items.pop(key, None)

            if self.curr_request == request:
                self.curr_request = None

        qid, qseq = aid
        queue_item = self.distQueue.remove_item(qid, qseq)
        if queue_item is None:
            logger.error("Attempted to remove nonexistent item {} from local queue {}!".format(qseq, qid))
        else:
            # Remove queue item from pydynaa
            queue_item.remove()

        # Check if we have any requests to follow up with and begin processing them
        self._process_outstanding_items()

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
        return self.curr_request and not self.curr_request.measure_directly

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
        self.requests[aid] = request

        # Store the absolute queue id under a unique request key
        logger.debug("Scheduling request {}".format(aid))
        key = (request.create_id, request.otherID)
        self.outstanding_items[key] = aid

        if queue_item.lifetime:
            self._wait_once(self.service_timeout_handler, entity=queue_item, event_type=queue_item._EVT_TIMEOUT)

        self._process_outstanding_items()

    def _process_outstanding_items(self):
        """
        Makes decisions on which request to process next.  Currently sorts the requests by queue sequence number
        and chooses the lowest to process first.
        :return:
        """
        # Check if we are already processing a generation request or if we have any requests to service
        if not self.requests and not self.curr_gen:
            return

        # Simply process the requests in FIFO order (for now...)
        sorted_request_aids = sorted(self.requests.keys(), key=lambda aid: aid[1])
        next_aid = sorted_request_aids[0]
        next_request = self.requests[next_aid]

        logger.debug("Creating gen templates for request {}".format(vars(next_request)))

        # Create templates for all generations part of this request
        for i in range(next_request.num_pairs):
            gen_template = [True, next_aid, None, None, None, None]
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
            request = self.requests[aid]
            self.timed_out_requests.append(request)
            self.clear_request(aid=aid)
            logger.debug("Scheduling request timeout event now.")
            self._schedule_now(self._EVT_REQ_TIMEOUT)

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
