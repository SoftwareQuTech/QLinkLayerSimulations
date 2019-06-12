QLinkLayer simulation implementation
====================================

This repo contains the implementation of the link layer protocol used for simulations in the paper:
A Link Layer Protocol for Quantum Networks (https://arxiv.org/abs/1903.09778)

The data collected for the above paper using the code in this repo can be found at:
https://dataverse.nl/dataset.xhtml?persistentId=hdl:10411/RA1RRK&version=DRAFT

-----------------------
Running the simulations
-----------------------
The simulation backend used for these simulations is our purpose-built discrete-event simulator [NetSQUID](https://netsquid.org/).
NetSQUID is currently under development and is not yet freely available.
For this reason you cannot run the simulation without getting access to NetSQUID.
However you can still take a look at the actual implementation of the protocol.

-----
Notes
-----
* The fidelity estimation unit used in these simulations was very simplistic and therefore not correct. It did not do any estimation based on measurement outcomes but only using a simple model for what the generated states are based on bright state populations, detection efficiencies and dark counts, not taking gate and measurement noise into account.

* The simulation we ran used a rather pessimistic coherence time for the electron (T1=2.68 ms, T2=1 ms) and nuclear spin (T1=inf, T2=3.5 ms), whereas coherence times of seconds have been experimentally verified using dynamical decoupling.

* Only one of the carbons (nr 1) was used, so whenever an type K OK was returned to the higher layer it was consumed and measured to be used again. Thus multiple entangled pairs were never stored at the same time.

* During the earlier of these simulations we noticed a bug with the distributed queue in some rare cases, which amounted to some warnings in the logs. This is however now fixed in the latest version of the repo.

* To more efficiently estimate QBER, the measurement-bases used for type M requests were iterated over (X, Y, Z) between the nodes such that the two nodes always measured in the same basis. The nodes choose the measurement-basis based on the current MHP-cycle modulo 3. However, this is not what a real implementation of a link layer protocol would do, see specification of the fields RBC, ROTX1, ROTY and ROTX2 in the CREATE header specified in https://datatracker.ietf.org/doc/draft-dahlberg-ll-quantum/

-----
Files
-----

The entanglement generation protocol (EGP) constist of the following files:

* `qlinklayer/distQueue.py`: Implementation of the distributed queue used by the EGP.
* `qlinklayer/egp.py`: The main file for the protocol. API to higher layers is given as the method `qlinklayer.egp.NodeCentricEGP.create`.
* `qlinklayer/feu.py`: A naive (and incorrect) implementation of the fidelity estimation unit used by the EGP to provide estimation of fidelities of the generated states.
* `qlinklayer/localQueue.py`: Describing each local queue at the nodes which the distributed queue makes sure are synchronized.
* `qlinklayer/qmm.py`: The quantum memory manager used by the EGP to allocate qubits used for entanglement generation.
* `qlinklayer/scheduler.py`: A scheduler used to scheduler incoming requests from higher layers for entanglement generation.

Other file:

* `qlinklayer/datacollection.py`: Defines how data should be collected during simulations (e.g. collect states, errors etc.)
* `qlinklayer/mhp.py`: A simulated version of the middle-heralded-protocol (MHP) which is the protocol in the physical layer which the EGP communicates with.
* `qlinklayer/scenario.py` and `qlinklayer/specific_scenarious.py`: Acts as higher layers during the simulations and issues entanglement generation requests (CREATE) to the EGP.

The folder `qlinklayer/simulations/` contains all files that were used to setup the simulations for the paper.
