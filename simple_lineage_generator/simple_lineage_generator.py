import os
import glob
import logging
import argparse

from typing import List, Set
from dotenv import load_dotenv

from collections import namedtuple
from rich.logging import RichHandler
from rich.console import Console
from rich.syntax import Syntax
from neo4j import GraphDatabase, Driver, Session
from lineage_runner.my_lineage_runner import myLineageRunner
from utils.clear_query import clean_query


TableTuple = namedtuple("TableTuple", "parent child")
ColumnTuple = namedtuple(
    "ColumnTuple",
    "table_parent table_child column_parent column_parent_short_name column_child column_child_short_name",
)


def list_query_files(directory: str, extensions: list = [".hql", ".sql"]) -> List[str]:
    """Lists all query files with extensions defined by 'extensions' under
    the diectory 'directory'

    :param directory: absolute path for the directory to be listed
    :type directory: str
    :param extensions: query extensions, defaults to [".hql", ".sql" ]
    :type extensions: list, optional
    :return: list of absolute path files
    :rtype: List[str]
    """

    logging.info(f"Listing files with extension {extensions} from '{directory}'")
    return [
        f
        for ext in extensions
        for f in glob.glob(directory + "/**/*" + ext, recursive=True)
    ]


def gen_table_relations(table_tuples: Set[TableTuple], session: Session) -> None:
    for entry in table_tuples:
        # For every table in tuple, also generate it's relation to schema
        for table in entry:
            schema = (
                table.split(".")[0] if len(table.split(".")) == 2 else "__NO_SCHEMA__"
            )
            node_schema_stm = (
                f"MERGE (s:Schema {{name:'{schema}', color: 'black' }}) RETURN s"
            )
            session.execute_write(_n4j_run_statement, node_schema_stm)

            node_table = (
                "MERGE (p:Table {{name:'{}', color: 'blue', schema: '{}'}}) RETURN p"
            )
            node_table_stm = node_table.format(table, schema)
            session.execute_write(_n4j_run_statement, node_table_stm)

            rel_schema_table_stm = f"""
                MATCH (
                    s:Schema {{name: '{schema}'}}
                ),(t:Table {{name: '{table}'}}
                ) MERGE (t)-[r:IS_IN]->(s)
            """
            session.execute_write(_n4j_run_statement, rel_schema_table_stm)

        # Now can Add downstrem realtion between tables
        rel_table_stm = f"""
            MATCH (p:Table {{name: '{entry.parent}'}}), (c:Table {{name: '{entry.child}'}}) 
            CREATE (c)-[r:SOURCES_FROM]->(p) 
            RETURN r
        """
        session.execute_write(_n4j_run_statement, rel_table_stm)


def gen_column_relations(column_tuples: Set[ColumnTuple], session: Session) -> None:
    for i in column_tuples:
        statements: list = []
        # "table_parent table_child column_parent column_parent_short_name column_child column_child_short_name"
        node_column = """
            MERGE (c:Column 
                {{ name:'{}', 
                short_name: '{}', 
                color: 'gray', 
                table: '{}' }}
            ) RETURN c
        """

        rel_table_column = """
            MATCH (
                t:Table 
                    {{name: '{}'}}
                ),(c:Column 
                    {{name: '{}'}}
                ) MERGE (c)-[r:HAS_COLUMN]->(t) 
            RETURN r
        """

        rel_columns = """
            MATCH (
                p: Column 
                    {{name: '{}'}}
                ), ( c:Column 
                    {{name: '{}'}}
                ) MERGE (c)-[r:SOURCES_FROM]->(p)               
            RETURN r
        """

        # column_parent node
        statements.append(
            node_column.format(
                i.column_parent, i.column_parent_short_name, i.table_parent
            )
        )
        # column_child node
        statements.append(
            node_column.format(i.column_child, i.column_child_short_name, i.table_child)
        )
        # parent-column-table rel
        statements.append(rel_table_column.format(i.table_parent, i.column_parent))
        # child-column-table rel
        statements.append(rel_table_column.format(i.table_child, i.column_child))
        # columns rel
        statements.append(rel_columns.format(i.column_parent, i.column_child))

        for i in statements:
            session.execute_write(_n4j_run_statement, i)


def _n4j_run_statement(tx, statement: str):
    clean_statement = clean_query(statement)
    logging.debug(f"Neo4j: executing statement '{clean_statement}'")
    tx.run(clean_statement)



def graph_table_and_columns(column_lineage: List[tuple], n4j_driver: Driver) -> None:
    """From a table+column lineage, supports graphing it through nodes and relationships
    by establishing entries of parent-child pair relationships in table-table and column-level.
    Since all columns have the complete table nae in it, we can derive the source table from it. 

    :param column_lineage: result of myLineageRunner.get_column_lineage_pairs
    :type column_lineage: List[tuple]
    :param n4j_driver: neo4j driver through 'bolt' port
    :type n4j_driver: Driver
    """
    table_tuples = []
    column_tuples = []
    # Each entry is a complete column lineage in pairs
    for entry in column_lineage:
        for column_pair in entry:
            column_parent, column_child = column_pair
            table_parent, table_child = tuple(
                map(lambda x: x.rsplit(".", 1)[0], column_pair)
            )
            column_parent_short_name, column_child_short_name = tuple(
                map(lambda x: x.rsplit(".", 1)[-1], column_pair)
            )

            table_tuples.append(TableTuple(table_parent, table_child))
            column_tuples.append(
                ColumnTuple(
                    table_parent,
                    table_child,
                    column_parent,
                    column_parent_short_name,
                    column_child,
                    column_child_short_name,
                )
            )

    # After all entries for a table, graph table and columns
    with n4j_driver.session(database=os.environ["NEO4J_DB"]) as session:
        gen_table_relations(set(table_tuples), session)
        gen_column_relations(set(column_tuples), session)


def create_lineage(directory: str, n4j_driver: Driver) -> None:
    files = list_query_files(directory)

    for f in files:
        logging.info(f"==== Fetching lineage from query file: '{f}'")
        with open(f, "r") as content:
            try:
                query: str = content.read()
            except Exception as e:
                msg = f"Error reading file: '{f}'. {type(e).__name__}: {e}"
                logging.error(msg)
                logger.error(msg)
                
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                CONSOLE.print(Syntax(query, "sql"))

            runner = myLineageRunner(sql=query, dialect="non-validating")
            column_lineage = runner.get_column_lineage_pairs()

            if not (column_lineage):
                msg = f"Impossible to parse SQL in file '{f}'. Skipping ..."
                logging.error(msg)
                logger.error(msg)
                continue

            graph_table_and_columns(
                column_lineage=column_lineage, n4j_driver=n4j_driver
            )


def main():
    parser = argparse.ArgumentParser(
        description="Gets a repo absolute path for repo as input and scan all \
                                   sql and hql files to genetate data lineage"
    )
    parser.add_argument(
        "--repo-abs-path",
        "-r",
        type=str,
        help="Repo's absolute path",
        default=DIRECTORY,
    )
    parser.add_argument("--debug", "-d", action='store_true', help='Use debug mode')
    args = parser.parse_args()

    log_level = "DEBUG" if args.debug else "INFO"
    logging.basicConfig(
        level=log_level, format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
    )
    
    n4j_driver: Driver = GraphDatabase.driver(os.environ["NEO4J_URI"], auth=None)
    create_lineage(args.repo_abs_path, n4j_driver)


if __name__ == "__main__":
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    
    log_error, log_info = "log/error.log", "log/info.log"
    try:
        os.remove(log_error)
        os.remove(log_info)
    except FileNotFoundError:
        pass

    info_handler = logging.FileHandler(log_info)
    info_handler.setLevel(logging.INFO)

    error_handler = logging.FileHandler(log_error)
    error_handler.setLevel(logging.ERROR)

    logger.addHandler(info_handler)
    logger.addHandler(error_handler)

    load_dotenv()
    
    DIRECTORY = os.environ["SIMPLE_LINEAGE_ROOT_FOLDER"]

    CONSOLE = Console()
    
    main()
