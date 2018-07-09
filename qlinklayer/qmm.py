#
# Quantum Memory Management Unit
#
from easysquid.toolbox import create_logger


logger = create_logger("logger")


class QuantumMemoryManagement:
    """
    Quantum memory management unit - STUB

    Converts logical qubit id's to physical qubit ids and vice versa.
    """

    def __init__(self, node):
        self.node = node
        self.reserved_qubits = [self.qubit_in_use(i) for i in range(self.node.qmem.max_num)]

    def is_busy(self):
        return self.node.qmem.busy

    def get_move_delay(self):
        """
        Gets the delay time for a move operation in the memory device.
        #TODO: Refacotr this to not perform the operation when it is easier to get the total time
        :return: float
            The amount of time it takes to move the electron state to the carbon
        """
        # If we are storing the entangled pair into the electron then the move time is 0
        if self.node.qmem.max_num > 1:
            return self.node.qmem.get_move_time(0, 1)

        else:
            return 0.0

    def get_correction_delay(self):
        """
        Gets the delay time for applying a Z gate onto the electron.
        #TODO: Refactor this to not perform the operation when it is easier to get the gate time
        :return: float
            The amount of time it takes to apply the z gate to the electron
        """
        return self.node.qmem.get_z_time(0)

    def qubit_in_use(self, id):
        """
        Tells us if a qubit is in use within the quantum memory device
        :param id: int
            Qubit address within the quantum memory device
        :return: bool
            Whether the address is in use or not
        """
        return self.node.qmem.in_use(id)

    def reserve_communication_qubit(self):
        """
        Reserves the communication qubit locally
        :return: int
            The qubit address corresponding to the reserved communication qubit.  -1 if failed
        """
        # Currently assume position 0 is always the communication qubit
        comm_q = 0
        if self.qubit_in_use(comm_q):
            return -1
        else:
            self.reserved_qubits[comm_q] = True
            return comm_q

    def reserve_storage_qubit(self, n):
        """
        Reserves a set of storage qubits locally
        :param n: int
            The number of storage qubits we want to reserve
        :return: list or int
            A list of the reserved storage qubit ids or -1 if failure
        """
        storage_qs = []
        logger.debug("Attempting to reserve {} qubits from: {}".format(n, self.reserved_qubits))
        for address in range(1, self.node.qmem.max_num):
            # Check if qmem is using this address, also check if it has already been reserved
            if not self.qubit_in_use(address) and not self.reserved_qubits[address]:
                storage_qs.append(address)

            if len(storage_qs) == n:
                return storage_qs

        return -1

    def free_qubit(self, id):
        """
        Frees the locally reserved qubit space
        :param id: int
            Address of the qubit to free
        """
        self.reserved_qubits[id] = False
        self.node.qmem.release_qubit(id)

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

    def reserve_entanglement_pair(self, n=1):
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
            return -1, [-1]

        # Obtain n storage qubits
        storage_q = self.reserve_storage_qubit(n)
        if storage_q == -1:
            return -1, [-1]

        # Mark the obtained qubits as reserved
        self.reserved_qubits[comm_q] = True
        for q in storage_q:
            self.reserved_qubits[q] = True

        return comm_q, storage_q

    def get_free_mem_ad(self):
        """
        Returns the amount of free memory (storage qubit locations) that we have in the
        quantum memory device
        :return:
        """
        free_mem = 0
        # Ignore communication qubit id 0
        for address in range(1, self.node.qmem.max_num):
            if not self.qubit_in_use(address) and not self.reserved_qubits[address]:
                free_mem += 1

        logger.debug("Quantum Memory Device has free memory {}".format(free_mem))
        return free_mem

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
