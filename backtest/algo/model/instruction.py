from algo.model.sub_order import SubOrder
from typing import Optional
from enum import Enum


class InstructionOperate(Enum):
    trade = 1
    cancel = 2


class Instruction(object):
    def __init__(self, sub_order: Optional[SubOrder], instruction_operate: Optional[InstructionOperate]):
        self.sub_order = sub_order
        self.operation: Optional[InstructionOperate] = instruction_operate

    def __str__(self):
        return f"operation:{self.operation},sub_order:{self.sub_order}"
