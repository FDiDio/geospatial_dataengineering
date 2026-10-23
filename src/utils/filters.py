import operator
from collections.abc import Sequence
from functools import reduce
from typing import Any

import pyarrow.dataset as ds

# A filter condition independent of the storage: (column, operator, value)
Condition = tuple[str, str, Any]

OPERATORS = {">=": operator.ge, ">": operator.gt, "<=": operator.le, "<": operator.lt, "==": operator.eq}


def to_arrow_expression(conditions: Sequence[Condition]) -> ds.Expression | None:
    """
    Combine the conditions into a PyArrow dataset filter expression.

    Returns:
        pyarrow.dataset.Expression: Conjunction of the conditions, or None if there are none.
    """
    expressions = [OPERATORS[op](ds.field(column), value) for column, op, value in conditions]
    return reduce(operator.and_, expressions) if expressions else None
