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
            return -1, -1

        # Obtain n storage qubits
        storage_q = self.reserve_storage_qubit(n)
        if storage_q == -1:
            return -1, -1

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
        return qubit_id

    def physical_to_logical(self, qubit_id):
        return qubit_id
