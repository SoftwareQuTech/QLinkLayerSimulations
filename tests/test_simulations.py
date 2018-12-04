import unittest
import os
import shutil
import json
import glob

from simulations import _get_configs_from_easysquid
from simulations.create_measure_simulation.readonly import create_simdetails_and_paramcombinations
from simulations import create_measure_simulation
from simulations.create_measure_simulation.setupsim import perform_single_simulation_run
from simulations import analysis_sql_data


class TestSimulations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        sim_dir = os.path.dirname(create_measure_simulation.__file__)
        os.environ["SIMULATION_DIR"] = sim_dir
        cls.sim_name = "Test_NoNoise"
        cls.results_folder = os.path.join(os.path.dirname(__file__), "test_simulation_tmp")
        cls.alpha = 0.1
        cls.create_probA = 1
        cls.create_probB = 0
        cls.max_mhp_cycle = 10000

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.results_folder):
            shutil.rmtree(cls.results_folder)

    def test_simulations_1(self):
        # Test grabbing files from easysquid
        _get_configs_from_easysquid.main()

    def test_simulations_2(self):
        paramcombinations = {
            self.sim_name: {
                "create_probA": self.create_probA,
                "create_probB": self.create_probB,
                "min_pairs": 1,
                "max_pairs": 1,
                "tmax_pair": 0,
                "request_cycle": 0,
                "num_requests": 500,
                "max_sim_time": 0,
                "max_wall_time": 345600,
                "max_mhp_cycle": self.max_mhp_cycle,
                "enable_pdb": False,
                "alphaA": self.alpha,
                "alphaB": self.alpha,
                "measure_directly": True,
                "t0": 0,
                "wall_time_per_timestep": 1,
                "save_additional_data": True,
                "collect_queue_data": True,
                "config": "setupsim/config/no_noise/no_noise.json"
            }
        }

        # Test creating simdetails and paramcombinations
        create_simdetails_and_paramcombinations.setup_sim_parameters(params=paramcombinations,
                                                                     description_string="Test simulation",
                                                                     number_of_runs=1, outputdirname="test_simulations",
                                                                     make_paramcombinations=False, ask_for_input=False)

        if os.path.exists(self.results_folder):
            shutil.rmtree(self.results_folder)
        os.mkdir(self.results_folder)
        paramfile = os.path.join(os.environ["SIMULATION_DIR"], "setupsim/paramcombinations.json")
        shutil.copy(paramfile, self.results_folder)

    def test_simulations_3(self):
        timestamp = "TEST_SIMULATION"
        runindex = 0
        paramfile = os.path.join(os.environ["SIMULATION_DIR"], "setupsim/paramcombinations.json")
        actualkey = self.sim_name
        params_for_simulation = [timestamp, self.results_folder, runindex, paramfile, actualkey]
        perform_single_simulation_run.main(params_for_simulation)

    def test_simulations_4(self):
        analysis_sql_data.main(results_path=self.results_folder, no_plot=True, save_figs=False, save_output=True)

        add_data_file_path = glob.glob("{}/*additional_data.json".format(self.results_folder))[0]
        with open(add_data_file_path, 'r') as f:
            additional_data = json.load(f)

        # Get the additional data
        mhp_t_cycle = additional_data["mhp_t_cycle"]
        request_t_cycle = additional_data["request_t_cycle"]
        alphaA = additional_data["alphaA"]
        alphaB = additional_data["alphaB"]
        create_probA = additional_data["create_request_probA"]
        create_probB = additional_data["create_request_probB"]
        total_matrix_time = additional_data['total_real_time']
        p_succ = additional_data["p_succ"]

        self.assertEqual(mhp_t_cycle, request_t_cycle)
        self.assertEqual(alphaA, self.alpha)
        self.assertEqual(alphaB, self.alpha)
        self.assertEqual(create_probA, self.create_probA)
        self.assertEqual(create_probB, self.create_probB)
        self.assertEqual(total_matrix_time, mhp_t_cycle * self.max_mhp_cycle)

        self.assertAlmostEqual(p_succ, 2 * self.alpha * (1 - self.alpha) + self.alpha ** 2, places=1)

    def test_simulations_5(self):
        if os.path.exists(self.results_folder):
            shutil.rmtree(self.results_folder)
        os.mkdir(self.results_folder)
        paramfile = os.path.join(os.path.dirname(__file__), "resources/paramcombinations.json")
        shutil.copy(paramfile, self.results_folder)

        # Load full_paramcombinations.json
        with open(paramfile) as f:
            paramcombinations = json.load(f)

        timestamp = "TEST_SIMULATION"
        runindex = 0
        for actualkey in paramcombinations.keys():
            params_for_simulation = [timestamp, self.results_folder, runindex, paramfile, actualkey]
            perform_single_simulation_run.main(params_for_simulation)

    def test_simulations_6(self):
        nr_of_add_data_files = len(
            glob.glob("{}/test_simulation_tmp/*additional_data.json".format(os.path.dirname(__file__))))
        self.assertEqual(nr_of_add_data_files, 20)

        nr_of_data_files = len(glob.glob("{}/test_simulation_tmp/*.db".format(os.path.dirname(__file__))))
        self.assertEqual(nr_of_data_files, 20)

        analysis_sql_data.main(results_path=self.results_folder, no_plot=True, save_figs=False, save_output=True)

        nr_of_data_files = len(
            glob.glob("{}/test_simulation_tmp/*analysis_output.txt".format(os.path.dirname(__file__))))
        self.assertEqual(nr_of_data_files, 20)


if __name__ == '__main__':
    unittest.main()
