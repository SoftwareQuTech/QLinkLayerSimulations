#
# General functionality
#

class LinkLayerException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def check_schedule_cycle_bounds(current_cycle, max_cycle, check_cycle):
    """
    Checks whether the provided schedule cycle slot falls within our accepting window
    :param current_cycle: int
        The current MHP cycle
    :param max_cycle: int
        The max MHP cycle
    :param check_cycle: int
        The mhp cycle number to check
    """
    right_boundary = current_cycle
    left_boundary = (right_boundary - max_cycle // 2) % max_cycle

    # Check if the provided time falls within our modular window
    if left_boundary < right_boundary:
        return left_boundary <= check_cycle <= right_boundary
    else:
        return check_cycle <= right_boundary or check_cycle >= left_boundary

