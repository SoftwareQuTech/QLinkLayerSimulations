# Simulation for Entanglement Generation Protocol (Link Layer)
This simulation runs the node-centric EGP using an MHP based on NV hardware.
The entanglement generation CREATE+measure, that is the electron spin is measured directly after a photon is emitted, without waiting for the reply from the midpoint.
Because of this, qubit-error will be extracted in the analysis instead of fidelity.
One goal of this simulaton is to investigate the local queues at the two nodes and compare the results with queuing theory.
Therefore the lengths of the local queues at the two nodes is extracted during the simulation.

Possible parameters for that can be set before running the simulation are:

 - 
