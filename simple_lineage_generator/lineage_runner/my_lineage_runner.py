import re
from typing import Dict, List, Tuple

from sqllineage.runner import LineageRunner


class myLineageRunner(LineageRunner):
    def __init__(
        self,
        sql: str,
        dialect: str = ...,
        encoding: str | None = None,
        verbose: bool = False,
        draw_options: Dict[str, str] | None = None,
    ):
        super().__init__(sql, dialect, encoding, verbose, draw_options)

    def get_column_lineage_pairs(self) -> List[Tuple]:
        column_lineage: list = []

        for path in self.get_column_lineage(exclude_subquery=True):
            lineage_str = "<-".join(str(col) for col in reversed(path))
            # If column is *, skip it
            if ".*" in lineage_str:
                continue

            full_lineage: list = lineage_str.split("<-")
            pairs: list = list(zip(full_lineage, full_lineage[1:]))
            # All entries should be pairs, len == 2
            assert all(len(p) == 2 for p in pairs)
            # Every column has it's own list of pair of lineage
            column_lineage.append(pairs)

        return column_lineage

    def my_table_lineage(self) -> str:
        return str(self)
