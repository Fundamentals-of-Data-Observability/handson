
# Injecting code in DBT to generate data observations naturally
# ***DBT*** START

# https://docs.python.org/2/library/site.html#module-site

# https://stackoverflow.com/questions/53626264/rewriting-a-python-module-only-if-it-gets-imported
import sys
from ast import *
from importlib.abc import MetaPathFinder, Loader
from importlib.machinery import ModuleSpec
from pathlib import Path

PRINT_DEBUG = False

class DBTKensuAgentPathFinder(MetaPathFinder):

  def find_spec(self, fullname, path, target=None):
    if path is None:
      return None
    if fullname not in ["dbt.task.run",
                        "dbt.adapters.sql.connections",
                        "dbt.context.providers",
                        "dbt.adapters.bigquery.connections"]:
      return None
    p = path
    if isinstance(path, list):
      p = path[0]
    elif isinstance(path, str):
      p = path
    elif hasattr(path, "_path"):
      # _NamespacePath : https://github.com/python/cpython/blob/13d44891426e0faf165f974f2e46907ab5b645a9/Lib/importlib/_bootstrap_external.py#L1329
      p = path._path[0]
    # so we provide the current loader => will use exec_module
    return ModuleSpec(fullname, DBTKensuAgentLoader(fullname, p))


class DBTKensuAgentLoader(Loader):
  PSEUDO_FILENAME = '<kensu>'
  def __init__(self, fullname, path) -> None:
    super().__init__()
    self.fullname = fullname
    self.module_name = fullname.split(".")[-1]
    self.path = path
    source = Path(self.path, self.module_name + ".py").read_text()
    tree = parse(source, self.PSEUDO_FILENAME)
    new_tree = DBTKensuTransformer().visit(tree)
    fix_missing_locations(new_tree)
    self.code = compile(new_tree, self.PSEUDO_FILENAME, 'exec')

  def exec_module(self, module):
    module.__file__ = self.PSEUDO_FILENAME # + module.__file__
    #exec(self.code, module.__dict__)
    exec(self.code, module.__dict__)


class DBTKensuTransformer(NodeTransformer):
  CURRENT_ModelRunner = False
  CURRENT_SQLConnectionManager = False
  CURRENT_ProviderContext = False
  CURRENT_BigQueryConnectionManager = False
  CURRENT_CLASS_NAME = ''

  def visit_Module(self, node):
    new_node = self.generic_visit(node)
    return new_node

  def visit_ClassDef(self, node):
    cls_name = node.name
    DBTKensuTransformer.CURRENT_CLASS_NAME = cls_name

    DBTKensuTransformer.CURRENT_ModelRunner = cls_name == "ModelRunner"
    DBTKensuTransformer.CURRENT_SQLConnectionManager = cls_name == "SQLConnectionManager"
    DBTKensuTransformer.CURRENT_ProviderContext = cls_name == "ProviderContext"
    DBTKensuTransformer.CURRENT_BigQueryConnectionManager = \
      bool(cls_name == "BigQueryConnectionManager")

    new_node = self.generic_visit(node)
    return new_node


  def visit_FunctionDef(self, node):
    new_node = self.generic_visit(node)
    if DBTKensuTransformer.CURRENT_ModelRunner:
      if node.name == "execute":
        # just init here; the actual datasources & lineages will be reported by dbt plugins themselves (bigquery, postgres)
        # add this code:
        # from dbt.task.kensu_reporting import dbt_init_kensu, kensu_report_rules
        # kensu_collector = dbt_init_kensu(context, model)
        # Index = 1 because after `context`` is created
        new_node.body.insert(1,
          ImportFrom(
            module='kensu_reporting',
            names=[
                alias(name='dbt_init_kensu', asname=None),
                alias(name='kensu_report_rules', asname=None),
            ],
            level=0,
          )
        )
        new_node.body.insert(2,
          Assign(
              targets=[Name(id='kensu_collector', ctx=Store())],
              value=Call(
                  func=Name(id='dbt_init_kensu', ctx=Load()),
                  args=[
                      Name(id='context', ctx=Load()),
                      Name(id='model', ctx=Load()),
                  ],
                  keywords=[],
              ),
              type_comment=None,
          )
        )
        # add the lines to create the rules, just before `return some_expr`:
        # if not kensu_collector.report_to_file:
        #  kensu_report_rules(self, context, model, kensu_collector, result)
        new_node.body.insert(
          -1,
          If(
            test=UnaryOp(
              op=Not(),
              operand=Attribute(
                value=Name(id='kensu_collector', ctx=Load()),
                attr='report_to_file',
                ctx=Load(),
              ),
            ),
            body=[
              Expr(
                value=Call(
                  func=Name(id='kensu_report_rules', ctx=Load()),
                  args=[
                    Name(id='self', ctx=Load()),
                    Name(id='context', ctx=Load()),
                    Name(id='model', ctx=Load()),
                    Name(id='kensu_collector', ctx=Load()),
                    Name(id='result', ctx=Load()),
                  ],
                  keywords=[],
                ),
              ),
            ],
            orelse=[],
          )
        )
        debug_new_node(new_node)
    elif DBTKensuTransformer.CURRENT_SQLConnectionManager:
      if node.name == "add_query":
        # the last statement is `with self.exception_handler(sql):`
        with_last_statement = new_node.body[-1]
        # insert kensu before the return in the `with` statement
        with_last_statement.body.insert(-1,
          ImportFrom(
            module='kensu_reporting',
            names=[
                alias(name='maybe_report_postgres', asname=None),
            ],
            level=0,
          )
        )
        with_last_statement.body.insert(-1,
          Expr(
            value=Call(
              func=Name(id='maybe_report_postgres', ctx=Load()),
              args=[
                Name(id='self', ctx=Load()),
                Name(id='cursor', ctx=Load()),
                Name(id='sql', ctx=Load()),
                Name(id='bindings', ctx=Load()),
              ],
              keywords=[],
            ),
          ),
        )
        debug_new_node(new_node)
    elif DBTKensuTransformer.CURRENT_ProviderContext:
      if node.name == "load_agate_table":
        # intercept return table
        new_node.body.insert(-1,
          ImportFrom(
            module='kensu_reporting',
            names=[alias(name='intercept_seed_table', asname=None)],
            level=0,
          )
        )
        new_node.body.insert(-1,
          Expr(
            value=Call(
              func=Name(id='intercept_seed_table', ctx=Load()),
              args=[
                Name(id='table', ctx=Load())
              ],
              keywords=[],
            ),
          )
        )
        debug_new_node(new_node)
    elif DBTKensuTransformer.CURRENT_BigQueryConnectionManager:
      # FIXME: is copy_and_results and _query_and_results not changed/used yet, but maybe useful?
      if node.name == "get_bigquery_client":
        # debug_node(node, cls_name="BigQueryConnectionManager")
        # insert `import kensu.google.cloud.bigquery`
        new_node.body.insert(-1, Import(
          names=[alias(name='kensu.google.cloud.bigquery', asname=None)]
        ))
        return_stmt = new_node.body[-1]
        # FIXME: ideally we should check if original return statement is as expected
        # change from: return       google.cloud.bigquery.Client(...)
        # change to  : return kensu.google.cloud.bigquery.Client(...)
        return_stmt.value.func = Attribute(
          value=Attribute(
            value=Attribute(
              value=Attribute(
                value=Name(id='kensu', ctx=Load()),
                attr='google',
                ctx=Load(),
              ),
              attr='cloud',
              ctx=Load(),
            ),
            attr='bigquery',
            ctx=Load(),
          ),
          attr='Client',
          ctx=Load(),
        )
        debug_new_node(new_node)
    return new_node

def debug_node(node, cls_name, code_type='ORIGINAL'):
  if PRINT_DEBUG:
    print(f"PRINTING {code_type} {cls_name}.{node.name} code:")
    import astpretty
    astpretty.pprint(node)
    print(f"END OF {code_type}: {cls_name}.{node.name} code.")

def debug_new_node(new_node):
  cls_name = DBTKensuTransformer.CURRENT_CLASS_NAME
  print(f"Code for {cls_name}.{new_node.name} was modified with Kensu trackers (sitecustomize.py)")
  debug_node(node=new_node, cls_name=cls_name, code_type='MODIFIED')

# ensuring the Path Finder is loaded first to intercept all others
sys.meta_path.insert(0, DBTKensuAgentPathFinder())

# ***DBT*** END