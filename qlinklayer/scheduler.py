#
# Scheduler
#

class RequestScheduler:
    """
    Stub for a scheduler to decide how we assign and consume elements of the queue.
    """

    def __init__(self, distQueue = None):

        # Distributed Queue to schedule from
        self.distQueue = distQueue

    def next_add(self, request):
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
        
        now = pydynaa.DynAASim().current_time

        # Determine Queue to use
        qid = self.next_pop()

        # Determine min time of item on that queue
        minTime = self.distQueue.get_min_schedule(qid)
        if minTime is None:
            # No items at all 
            return False

        # Trivial policy: schedule when both A and B can realistically know
        if minTime <= now:
            return True
        else:
            return False 


        


