#
# Scheduler
#
from netsquid import pydynaa
from easysquid.toolbox import create_logger

logger = create_logger("logger")


class RequestScheduler:
    """
    Stub for a scheduler to decide how we assign and consume elements of the queue.
    """

    def __init__(self, distQueue, qmm):

        # Distributed Queue to schedule from
        self.distQueue = distQueue
        self.qmm = qmm
        self.my_free_memory = self.qmm.get_free_mem_ad()
        self.other_mem = 0

    def next(self):
        """
        Returns the next request (if any) to be processed
        :return: tuple or None
            Tuple containing the request information for the MHP or None if no request
        """
        # Get the queue id containing the next request and obtain the next request
        if not self.request_ready():
            logger.debug("Next request is not ready")
            return None

        qid = self.next_pop()
        queue_item = self.distQueue.local_peek(qid)
        request = queue_item.request

        if not self.has_resources_for_request(request):
            logger.debug("Not enough resources for request")
            return None

        logger.debug("Scheduler has next request: {}".format(vars(queue_item)))

        generations = self.construct_generation_list(qid, queue_item)

        self.distQueue.local_pop(qid)

        return generations

    def construct_generation_list(self, qid, queue_item):
        request = queue_item.request

        # Reserve qubits to be used for the request
        comm_q, storage_ids = self.qmm.reserve_entanglement_pair(request.num_pairs)
        free_memory_size = self.qmm.get_free_mem_ad()

        generation_list = []
        for storage_q in storage_ids:
            generation_request = (queue_item.ready, (qid, queue_item.seq), comm_q, storage_q, None, free_memory_size)
            generation_list.append(generation_request)

        return generation_list

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

    def request_ready(self):
        """
        Default scheduling policy is to declare an item ready, if it's
        minimum time is in the past.
        """
        logger.debug("Scheduler checking if request is ready")
        now = pydynaa.DynAASim().current_time

        # Check if there are any requests in this queue
        qid = self.next_pop()
        queue_item = self.distQueue.local_peek(qid)
        if queue_item is None:
            logger.debug("No requests in distributed queue")
            return False

        # Determine min time of item on that queue
        minTime = self.distQueue.get_min_schedule(qid)
        if minTime is None:
            # No items at all
            return False

        # Trivial policy: schedule when both A and B can realistically know
        if minTime <= now:
            return queue_item.ready
        else:
            return False

    def has_resources_for_request(self, request):
        # Verify whether we have the resources to satisfy this request
        logger.debug("Checking if we can satisfy request: {}".format(vars(request)))
        n = request.num_pairs
        if self.other_mem < n:
            logger.debug("Peer memory size {} less than requested pairs {}".format(self.other_mem, n))
            return False

        elif self.my_free_memory < n:
            logger.debug("Local memory size {} less than requested pairs {}".format(self.my_free_memory, n))
            return False

        return True

    def timeout_stale_requests(self):
        # Check if there are any items in the queue
        qid = self.next_pop()
        next_queue_item = self.distQueue.local_peek(qid)

        # Continue until we run out of items or find something to service
        stale_requests = []
        now = pydynaa.DynAASim().current_time
        while next_queue_item:
            # Grab the EGPRequest off of the queue item
            next_request = next_queue_item.request

            # Check if we missed or hit a deadline
            if next_request.create_time + next_request.max_time <= now:
                # Pass up all stale requests for error handling
                stale_requests.append(self.distQueue.local_pop(qid).request)

            # We found an item we can still service
            else:
                return stale_requests

            qid = self.next_pop()
            next_queue_item = self.distQueue.local_peek(qid)

        return stale_requests

    def update_other_mem_size(self, mem):
        """
        Stores the other peer's free memory locally for use with scheduling
        :param mem: int
            The amount of free memory locations the other node has
        """
        self.other_mem = mem
