#
# Quantum Memory Management Unit
#
from easysquid.toolbox import logger, EasySquidException
from easysquid.easygate import ZGate
from netsquid.qubits import qubitapi as qapi


class QuantumMemoryManagement:
    """
    Quantum memory management unit - STUB

    Converts logical qubit id's to physical qubit ids and vice versa.
    """

    def __init__(self, node):
        self.node = node
        # self.reserved_qubits = [self.qubit_in_use(i) for i in range(self.node.qmem.num_positions)]
        self.reserved_qubits = [False] * self.node.qmem.num_positions

    def is_busy(self):
        return self.node.qmem.busy

    def reserve_qubit(self, qid):
        """
        Reserves the qubit ID locally for a process
        :param qid: int
            The qubit ID to reserve
        """
        if self.reserved_qubits[qid]:
            raise EasySquidException("Qubit location {} already reserved!".format(qid))
        else:
            self.reserved_qubits[qid] = True

    def vacate_qubit(self, qid):
        """
        Removes the reservation for a qubit id
        :param qid: int
            The qubit ID we want to free for other processes
        """
        self.reserved_qubits[qid] = False

    def get_move_delays(self, comm_id):
        """
        Gets the delay times for a move operation in the memory device from comm_id to other qubits.
        If a move is not possible the move time is 'float('inf')'.
        #TODO: For now we ask the QPD but this should be somewhere else since it depends on how the move is done.
        :return: dict of float
            The key are where we're moving to and the entries are move time from qubit ID 'comm_id' to qubit ID 'key'.
        """
        return self.node.qmem.get_move_times(comm_id=comm_id)

    def get_correction_delay(self, comm_id):
        """
        Gets the delay time for applying a Z gate onto the electron.
        #TODO: Refactor this to not perform the operation when it is easier to get the gate time
        :return: float
            The amount of time it takes to apply the z gate to the electron
        """
        return self.node.qmem.get_operation_time(ZGate(), comm_id)

    def qubit_reserved(self, id):
        """
        Tells us if a qubit is reserved
        :param id: int
            Qubit address within the quantum memory device
        :return: bool
            Whether the address is in use or not
        """
        # return self.node.qmem.in_use(id)
        return self.reserved_qubits[id]

    def reserve_communication_qubit(self):
        """
        Reserves the communication qubit locally
        :return: int
            The qubit address corresponding to the reserved communication qubit.  -1 if failed
        """
        free_comms = self.get_free_communication_ids()
        if free_comms:
            comm_q = free_comms[0]
            self.reserve_qubit(comm_q)
            return comm_q
        else:
            return -1

    def reserve_storage_qubit(self):
        """
        Reserves a set of storage qubits locally
        :param n: int
            The number of storage qubits we want to reserve
        :return: list or int
            A list of the reserved storage qubit ids or -1 if failure
        """
        free_storage = self.get_free_storage_ids()
        if free_storage:
            storage_q = free_storage[0]
            self.reserve_qubit(storage_q)
            return storage_q
        else:
            return -1

    def free_qubit(self, id):
        """
        Frees the locally reserved qubit space
        :param id: int
            Address of the qubit to free
        """
        self.vacate_qubit(qid=id)
        q = self.node.qmem.pop(id)[0]
        if q is not None:
            qapi.discard(q)
        else:
            logger.warning("Trying to free a non-existing qubit")

    def free_qubits(self, id_list):
        """
        Releases the provided qubits from the memory device and marks their locations as not reserved in the
        memory manager
        :param id_list: list of int
            List of qubit ids in memory to free
        """
        logger.debug("Freeing qubits {}".format(id_list))
        for q in id_list:
            self.free_qubit(q)

    def reserve_entanglement_pair(self):
        """
        Reserves a pair of communication qubit and storage qubits for use with MHP.
        :param n: int
            The number of entangled qubits desired to be produced
        :return: tuple (int, list)
            A tuple of the communication qubit and storage qubits reserved
        """
        # Obtain a communication qubit
        comm_q = self.reserve_communication_qubit()
        if comm_q == -1:
            return -1, -1

        # Obtain n storage qubits
        storage_q = self.reserve_storage_qubit()
        if storage_q == -1:
            self.free_qubit(id=comm_q)
            return -1, -1

        return comm_q, storage_q

    def get_free_mem_ad(self):
        """
        Returns the amount of free memory (storage qubit locations) that we have in the
        quantum memory device
        :return:
        """
        free_comms = self.get_free_communication_ids()
        free_storage = self.get_free_storage_ids()
        free_mem = (len(free_comms), len(free_storage))

        logger.debug("Quantum Memory Device has free memory {}".format(free_mem))
        return free_mem

    def get_free_communication_ids(self):
        """
        Obtains all free communication qubit IDs for provisioning resources for incoming generation requests
        :return: list of ints
            List of the available communication qubits that can be used
        """
        comm_qs = self.node.qmem.get_communication_qubit_ids()
        free_comms = []
        for qid in comm_qs:
            # if not self.reserved_qubits[qid] and not self.qubit_in_use(id=qid):
            if not self.qubit_reserved(qid):
                free_comms.append(qid)

        return free_comms

    def get_free_storage_ids(self):
        """
        Obtains all free storage qubit IDs for provision resources for incoming generation requests
        :return: list of ints
            List of the available storage qubits that can be used
        """
        storage_qs = self.node.qmem.get_storage_qubit_ids()
        free_storage = []
        for qid in storage_qs:
            # if not self.reserved_qubits[qid] and not self.qubit_in_use(id=qid):
            if not self.qubit_reserved(qid):
                free_storage.append(qid)

        return free_storage

    def logical_to_physical(self, qubit_id):
        """
        Converts the provided logical qubit id into the corresponding physical id
        :param qubit_id: int
            Logical ID of the qubit we want the physical ID for
        :return: int
            Physical ID of the qubit
        """
        return qubit_id

    def physical_to_logical(self, qubit_id):
        """
        Converts the provided physical qubit id into the corresponding logical id
        :param qubit_id: int
            Physical ID of the qubit we want the logical ID for
        :return: int
            Logical ID of the qubit
        """
        return qubit_id
