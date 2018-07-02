#
# Scheduler
#
from netsquid import pydynaa
from easysquid.toolbox import create_logger

logger = create_logger("logger")


class RequestScheduler(pydynaa.Entity):
    """
    Stub for a scheduler to decide how we assign and consume elements of the queue.
    """

    def __init__(self, distQueue, qmm):

        # Distributed Queue to schedule from
        self.distQueue = distQueue

        # Queue service timeout handler
        self.service_timeout_handler = pydynaa.EventHandler(self._handle_item_timeout)
        self._wait(self.service_timeout_handler, entity=self.distQueue, event_type=self.distQueue._EVT_QUEUE_TIMEOUT)
        self._EVT_REQ_TIMEOUT = pydynaa.EventType("REQ TIMEOUT", "Triggers when request was not completed in time")

        # Queue item schedule handler
        self.schedule_handler = pydynaa.EventHandler(self._schedule_request)
        self._wait(self.schedule_handler, entity=self.distQueue, event_type=self.distQueue._EVT_SCHEDULE)

        # Quantum memory management
        self.qmm = qmm
        self.my_free_memory = self.qmm.get_free_mem_ad()
        self.other_mem = 0

        # Generation tracking
        self.requests = {}
        self.curr_gen = None
        self.default_gen = (False, None, None, None, None, self.my_free_memory)
        self.outstanding_gens = []
        self.outstanding_items = {}
        self.timed_out_requests = []

    def update_other_mem_size(self, mem):
        """
        Stores the other peer's free memory locally for use with scheduling
        :param mem: int
            The amount of free memory locations the other node has
        """
        self.other_mem = mem

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

        # Default to an information request
        next_gen = self.default_gen

        if self.curr_gen:
            next_gen = self.curr_gen

        # If there are outstanding generations and we have memory, overwrite with a request
        elif self.outstanding_gens and not self.qmm.is_busy() and self._has_resources_for_gen():
            logger.debug("Filling next available gen template")

            # Obtain the next template
            next_gen = self.outstanding_gens.pop(0)

            # Reserve resources in the quantum memory
            comm_q, [storage_q] = self.qmm.reserve_entanglement_pair(1)

            # Compute our new free memory after reservation
            new_free_memory = self.qmm.get_free_mem_ad()

            # Fill in the template
            next_gen[2] = comm_q
            next_gen[3] = storage_q
            next_gen[-1] = new_free_memory
            next_gen = tuple(next_gen)

            logger.debug("Created gen request {}".format(next_gen))
            self.curr_gen = next_gen

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
            self.curr_gen = None

        logger.debug("Removed remaining generations for {}: {}".format(aid, removed_gens))
        request = self.requests.pop(aid, None)
        if request:
            key = (request.create_id, request.otherID)
            self.outstanding_items.pop(key, None)

        return removed_gens

    def _has_resources_for_gen(self):
        """
        Checks if we have the resources to service a generation request.
        :return: bool
            True/False whether we have resources
        """
        # Verify whether we have the resources to satisfy this request
        logger.debug("Checking if we can satisfy next gen")
        if not self.other_mem > 0:
            logger.debug("Peer memory size {} cannot satisfy gen!".format(self.other_mem))
            return False

        elif not self.my_free_memory > 0:
            logger.debug("Local memory size {} lcannot satisfy gen!".format(self.my_free_memory))
            return False

        return True

    def _schedule_request(self, evt):
        """
        Event handler for scheduling queue items from the distributed queue that are ready to be serviced.  Crafts
        generation templates to be filled in the future and stores request information for tracking.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        # Get the queue that has an item ready
        queue = evt.source

        # Get the qid to pop from in the distQueue
        qid = self.next_pop()

        # Get the item and request
        queue_item = queue.local_pop(qid)
        qseq = queue_item.seq

        # Store the request under the absolute queue id
        aid = (qid, qseq)
        request = queue_item.request
        self.requests[aid] = request

        # Store the absolute queue id under a unique request key
        logger.debug("Scheduling request {}".format(aid))
        key = (request.create_id, request.otherID)
        self.outstanding_items[key] = aid
        self._wait_once(self.service_timeout_handler, entity=queue_item, event_type=queue_item._EVT_TIMEOUT)

        logger.debug("Creating gen templates for request {}".format(vars(request)))

        # Create templates for all generations part of this request
        for i in range(request.num_pairs):
            gen_template = [queue_item.ready, aid, None, None, None, None]
            self.outstanding_gens.append(gen_template)

    def _handle_item_timeout(self, evt):
        """
        Timeout handler that is triggered when a queue item times out.  If the item has not been serviced yet
        then the stored request and it's information is removed.
        :param evt: obj `~netsquid.pydynaa.Event`
            The event that triggered this handler
        """
        queue_item = evt.source
        request = queue_item.request
        key = (request.create_id, request.otherID)
        if key in self.outstanding_items:
            logger.error("Failed to service request in time, clearing")
            aid = self.outstanding_items[key]
            request = self.requests[aid]
            self.timed_out_requests.append(request)
            self.clear_request(aid=aid)
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
