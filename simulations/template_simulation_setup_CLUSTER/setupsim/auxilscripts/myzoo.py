#!/usr/bin/python


import numpy as np


def generate_zoolist(number_of_lions, number_of_tigers, number_of_remaining_wildebeasts):
    """
    Generate a list of animals (string) from the input parameters in the 
    order: lions - tigers - wildebeasts.

    Parameters
    ----------
    number_of_lions : int
    number_of_tigers : int
    number_of_remaining_wildebeasts : float

    Returns
    -------
    list of str
        List of animals.
    """
    zoolist = []
    for __ in range(number_of_lions):
        zoolist.append(np.string_("lion"))
    for __ in range(number_of_tigers):
        zoolist.append(np.string_("tiger"))
    for __ in range(int(np.floor(number_of_remaining_wildebeasts))):
        zoolist.append(np.string_("scared wildebeast"))
    return zoolist
