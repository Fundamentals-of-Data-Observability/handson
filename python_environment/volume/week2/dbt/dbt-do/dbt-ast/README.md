
# Resources
https://docs.python.org/3/library/ast.html
https://greentreesnakes.readthedocs.io/en/latest/tofrom.html


https://github.com/dbt-labs/jaffle_shop


# Setup virtual env with required dependencies (dbt not patched yet):

```bash
# py3.10 don't seem to work currently (RecursionError: maximum recursion depth exceeded in logging)
# should work with python3.7 too, but some dependency versions might need to be changed (like pandas etc)
PY=python3.8
$PY -m venv dbt_env
source dbt_env/bin/activate
# quite important to use pip freeze requirements to prevent package version conflicts (or solve them yourself, e.g. force MarkupSafe)
$PY -m pip install --upgrade pip setuptools wheel
$PY -m pip install -r requirements.txt --force
```


# Setup Agent (monkey-patch dbt code in runtime via AST)
```bash
#PY_USER_SITE=`python3 -m site --user-site`
# FIXME/WARNING: this seem to return python main installation, and not a virtualenv
# (venv) ➜  dbt-ast git:(main) ✗ ./dbt-ast/venv/bin/python -m site --user-site                                        
# /home/someuser/.local/lib/python3.10/site-packages
PY_USER_SITE="dbt_env/lib/python3.8/site-packages/"
cp sitecustomize.py kensu_reporting.py kensu_postgres.py $PY_USER_SITE
```

# Setup sample dbt project: Jaffle_shop

```bash
git clone git@github.com:dbt-labs/jaffle_shop.git
cd jaffle_shop
```

```bash
docker pull postgres
docker run --name dbt-jaffle-shop -p 5432:5432 -e POSTGRES_PASSWORD=admin -e POSTGRES_DB=jaffle_shop -d postgres
```

```yml
cat <<EOT >> ~/.profiles.yml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: admin
      port: 5432
      dbname: jaffle_shop
      schema: dbt_alice
      threads: 4
EOT
```

```
dbt debug
```

 
REPLACE `TODO` below with access tokens in [conf.ini](./conf.ini) and copy it to directory where `dbt` will be called from:

```bash
cp conf.ini ./jaffle_shop/
```

# RUN

```bash
# git clone https://github.com/dbt-labs/jaffle_shop.git
cd jaffle_shop
export KSU_CONF_FILE="conf.ini"
dbt seed
dbt run
```

# Developer tool
Dumping AST code from python code into injection code :D

```bash
pip3 install astpretty
```

Then run for example
```python
import ast
import astpretty
example_code = '''

from dbt.task.kensu_reporting import maybe_report_postgres
maybe_report_postgres(conn_mngr=self, cursor=cursor, sql=sql)

'''

def pp(code = example_code):
  astpretty.pprint(ast.parse(code, mode="exec"), show_offsets=False, indent='  ')

pp()
```
