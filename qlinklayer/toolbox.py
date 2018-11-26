#
# General functionality
#
from SimulaQron.cqc.backend.cqcHeader import CQCHeader, CQCEPRRequestHeader, CQC_HDR_LENGTH, CQC_CMD_HDR_LENGTH, \
    CQCCmdHeader, CQC_TP_COMMAND, CQC_CMD_EPR, CQC_EPR_REQ_LENGTH
from collections import namedtuple


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


CQC_EPR_request_tuple = namedtuple("cqc_epr_request",
                                   ["purpose_id", "other_id", "num_pairs", "min_fidelity", "max_time", "priority",
                                    "store", "measure_directly", "atomic"])


def unpack_raw_cqc_request(cqc_request_raw):
    """
    Unpacks a full CQC request for generating entanglement.

    :param cqc_request_raw: bytes
        The cqc request consisting of CQCHeader, CQCCmdHeader, CQCEPRRequestHeader
    :return: namedtuple
        Returns a namedtuple with the following fields:
        * purpose_id
        * other_id
        * num_pairs
        * min_fidelity
        * max_time
        * priority
        * store
        * measure_directly
        * atomic
    """
    try:
        cqc_header = CQCHeader(cqc_request_raw[:CQC_HDR_LENGTH])
        if not cqc_header.tp == CQC_TP_COMMAND:
            raise LinkLayerException("raw CQC request is not of type command")

        cqc_request_raw = cqc_request_raw[CQC_HDR_LENGTH:]
        cqc_cmd_header = CQCCmdHeader(cqc_request_raw[:CQC_CMD_HDR_LENGTH])
        if not cqc_cmd_header.instr == CQC_CMD_EPR:
            raise LinkLayerException("raw CQC request is not a command for EPR")

        cqc_request_raw = cqc_request_raw[CQC_CMD_HDR_LENGTH:]
        cqc_epr_req_header = CQCEPRRequestHeader(cqc_request_raw[:CQC_EPR_REQ_LENGTH])
    except IndexError:
        raise LinkLayerException("Could not unpack raw CQC request")
    cqc_request_tuple = CQC_EPR_request_tuple(purpose_id=cqc_header.app_id, other_id=cqc_epr_req_header.remote_ip,
                                              num_pairs=cqc_epr_req_header.num_pairs,
                                              min_fidelity=cqc_epr_req_header.min_fidelity,
                                              max_time=cqc_epr_req_header.max_time,
                                              priority=cqc_epr_req_header.priority, store=cqc_epr_req_header.store,
                                              measure_directly=cqc_epr_req_header.measure_directly,
                                              atomic=cqc_epr_req_header.atomic)

    return cqc_request_tuple
