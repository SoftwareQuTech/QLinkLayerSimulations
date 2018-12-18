import sys
import os
import math
from argparse import ArgumentParser


def main(total_cores, cores_per_node, paramsetfile):
    # Get keys and runindices from paramsetfile
    tasks = []
    with open(paramsetfile, 'r') as f:
        for line in f.readlines():
            key, runindex = map(lambda s: s.strip(), line.split(" "))
            tasks.append((key, runindex))

    # Compute number of tasks per node
    num_tasks = len(tasks)
    num_nodes = math.ceil(total_cores / cores_per_node)
    tasks_per_node = math.ceil(num_tasks / num_nodes)

    # Print the number of nodes to be used by shell script calling
    print(num_nodes)

    # Create the new paramsetfiles for each node
    path_to_folder = os.path.dirname(paramsetfile)
    for node_index in range(num_nodes):
        filename = os.path.join(path_to_folder, "paramset_{}.csv".format(node_index + 1))
        with open(filename, 'w') as f:
            for task_index in range(tasks_per_node * node_index, tasks_per_node * (node_index + 1)):
                if task_index >= len(tasks):
                    return
                key, runindex = tasks[task_index]
                f.write("{} {}\n".format(key, runindex))


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--total_cores', required=True, type=int, help='Total number of cores to be used')
    parser.add_argument('--cores_per_node', required=True, type=int, help='Number of cores per node')
    parser.add_argument('--paramsetfile', required=True, type=str, help='The absolute path to the current combined paramsetfile')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    if not os.path.isfile(args.paramsetfile):
        raise ValueError("Argument {} is not a file".format(paramsetfile))
    main(total_cores=args.total_cores, cores_per_node=args.cores_per_node, paramsetfile=args.paramsetfile)
