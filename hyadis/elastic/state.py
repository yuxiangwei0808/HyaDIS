from typing import Any, OrderedDict


class State:

    def __init__(self):
        self.named_states = dict()

    @staticmethod
    def is_loadable(module: Any):
        return hasattr(module, "state_dict") and hasattr(module, "load_state_dict")

    def __setitem__(self, name: str, module: Any):
        self.named_states[name] = module

    def __getitem__(self, name: str):
        return self.named_states[name]

    def state_dict(self):
        return {name: module.state_dict() for name, module in self.named_states.items()}

    def load_state_dict(self, state_dict: OrderedDict):
        for name, state in state_dict.items():
            self.named_states[name].load_state_dict(state)
