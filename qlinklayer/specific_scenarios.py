from qlinklayer.scenario import EGPSimulationScenario, MeasureAfterSuccessScenario, MeasureBeforeSuccessScenario


class MixedScenario(EGPSimulationScenario):
    def __init__(self):
        pass

    def _ok_callback(self, result):
        pass

    @staticmethod
    def unpack_cqc_ok(results):
        pass

    def _err_callback(self, result):
        pass