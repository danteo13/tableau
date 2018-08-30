import pypyodbc as pyodbc
import xml.etree.cElementTree as ET
import os
import getpass

# initilalise global variables
dsn = None
user = None
pw = None

ds_id = 0
selected_files = []

# list of tables in database - this is used in delete statements
db_tables = [
    "tableau_user.tds_datasources",
    "tableau_user.tds_columns",
    "tableau_user.tds_relations",
    "tableau_user.tds_folders",
    "tableau_user.tds_drill_paths"
]

##################################################################
## log in functions
##################################################################

def login_db():
    """
    Log in to databse to store the documentation data.
    You'll need to create a DSN entry via ODBC administrator using the EXASOL driver.
    The script uses 'EXASOL' as the default DSN entry, but you can use a different
    name if you wish to do so.
    """
    # set default values for connection string
    global dsn
    global user
    global pw
    global conn

    default_dsn = 'EXASOL'

    dsn = input("Specify DSN (DEFAULT = %s) : " %(default_dsn))
    user = input("Specify user name (leave blank to use stored username): ")

    # if skipped, use defaults
    if not dsn:
        dsn = default_dsn

    conn_string = "DSN=%s;UID=%s;PWD=%s" %(dsn, user,
        getpass.getpass ("Enter your password (leave blank to use stored password): "))
    conn = pyodbc.connect(conn_string)
    return conn.cursor()


def host_logged_in():
    """
    displays a message showing if connected to database (and which)
    """
    if not dsn:
        return "Database : Not logged in"
    else:
        return "Database : Logged in to " + dsn


##################################################################
## create database schema functions
##################################################################

def create_dbschema(cur):
    """
    Drop all tables and create them from scratch.
    DDL definitions are contained in the file tableau_ddl.sql
    Changes to the schema should be updated in the file tableau_ddl.sql
    """
    with open("tableau_ddl.sql", "r") as ddl:
        sql = ddl.read()
        for statement in sql.split(";"):
            print(statement)
            cur.execute(statement)

    cur.execute("commit")


##################################################################
## read file functions
##################################################################

def get_filenames():
    files = [f for f in os.listdir() if f.endswith(".twb") and f not in selected_files]
    return files


def add_files(filename):
    selected_files.append(filename)


def remove_files(filename):
    selected_files.remove(filename)


def parse_xml(twb):
    tree = ET.ElementTree(file = twb)
    root = tree.getroot()
    return root


##################################################################
## load database functions
##################################################################

def data_ds(xml, cur, filename):
    """
    extract and load data assoicated with <datasource> tags
    """
    for ds in xml.iter('datasource'):
        ds_name = ds.get("name")
        ds_caption = filename.split(".")[0]
        ds_repo_id = ""
        for repo in ds.iter("repository-location"):
            ds_repo_id = repo.get("id")

        # check of datasource exists
        ds_exists(ds_caption, cur)

        # generate sql statement
        sql_stmt = """
            INSERT INTO tableau_user.tds_datasources (
            datasource_id,
            datasource_name,
            datasource_caption,
            datasource_repo_id
            )
            VALUES('{}','{}','{}','{}')""".format(
            ds_id,
            ds_name,
            ds_caption,
            ds_repo_id
            )

        cur.execute(sql_stmt)

    cur.execute("commit")


def data_columns(xml, cur):
    """
    extract and load data assoicated with <column> tags
    """
    # 1st iteration is build name / caption dictionary
    col_dict = {}
    for col in xml.iter('column'):
        col_name = col.get("name")
        col_caption = col.get("caption")
        col_dict[col_name] = col_caption

    # create <map> dictionary
    map_dict = {}
    for map in xml.iter("map"):
        map_key = map.get("key")
        map_value = map.get("value")
        map_dict[map_key] = map_value

    # get attributes from column tag
    for col in xml.iter('column'):
        column_caption = col.get("caption")
        column_name = col.get("name")
        column_datatype = col.get("datatype")
        column_role = col.get("role")
        column_type = col.get("type")
        column_aggregation = col.get("aggregation")
        column_hidden = col.get("hidden")
        if not column_hidden:
            column_hidden = False

        # run get_comments formula
        formatted_text = get_comments(col)

        # get formula and replace column name with caption for better readability
        calculation_formula = None
        calculation_class = None
        for calc in col.iter("calculation"):
            calculation_class = calc.get("class")
            calculation_formula = calc.get("formula")
            if calculation_formula:
                calculation_formula = replace_formula(calculation_formula, col_dict)

        # find matching record in map_dict
        mapped_value = map_dict.get(column_name , None)
        db_table = None
        db_column = None
        if mapped_value:
            mapped_value = mapped_value.split(".")
            db_table = mapped_value[0]
            db_column = mapped_value[1]

        sql_stmt = """
            INSERT INTO tableau_user.tds_columns (
            datasource_id,
            column_name,
            column_caption,
            column_datatype,
            column_role,
            column_type,
            column_aggregation,
            column_hidden,
            calculation_class,
            calculation_formula,
            db_table,
            db_column,
            formatted_text
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)"""

        cur.execute(sql_stmt, (
            ds_id,
            column_name,
            column_caption,
            column_datatype,
            column_role,
            column_type,
            column_aggregation,
            column_hidden,
            calculation_class,
            calculation_formula,
            db_table,
            db_column,
            formatted_text
            )
        )


    cur.execute("commit")


def data_relations(xml, cur):
    """
    extract and load data assoicated with <relation> tags
    """
    for rel in xml.iter("relation"):
        relation_name = rel.get("name")
        if relation_name:
            relation_type = rel.get("type")
            relation_text = None
            if relation_type == "table":
                relation_text = rel.get("table")
            elif relation_type == "text":
                relation_text = rel.text

            sql_stmt = """
                INSERT INTO tableau_user.tds_relations (
                datasource_id,
                relation_name,
                relation_type,
                relation_text
                )
                VALUES(?,?,?,?)"""

            cur.execute(sql_stmt, (
                ds_id,
                relation_name,
                relation_type,
                relation_text
                )
            )

    cur.execute("commit")


def data_folders(xml, cur):
    """
    extract and load data assoicated with <folder> tags
    """
    for folder in xml.iter("folder"):
        folder_name = folder.get("name")
        folder_role = folder.get("role")

        for item in folder.iter("folder-item"):
            folder_item_name = item.get("name")
            folder_item_type = item.get("type")

            sql_stmt = """
                INSERT INTO tableau_user.tds_folders (
                datasource_id,
                folder_name,
                folder_role,
                folder_item_name,
                folder_item_type
                )
                VALUES(?,?,?,?,?)"""

            cur.execute(sql_stmt, (
                ds_id,
                folder_name,
                folder_role,
                folder_item_name,
                folder_item_type
                )
            )

    cur.execute("commit")


def data_drill_paths(xml, cur):
    """
    extract and load data assoicated with <drill-path> tags
    """
    for dp in xml.iter("drill-path"):
        drill_path_name = dp.get("name")
        drill_path_order = 0

        for field in dp.iter("field"):
            column_name = field.text
            drill_path_order += 1

            sql_stmt = """
                INSERT INTO tableau_user.tds_drill_paths (
                datasource_id,
                drill_path_name,
                column_name,
                drill_path_order
                )
                VALUES(?,?,?,?)"""

            cur.execute(sql_stmt, (
                ds_id,
                drill_path_name,
                column_name,
                drill_path_order
                )
            )

    cur.execute("commit")


def logout_db():
    conn.close()


##################################################
## Helper Functions
##################################################
def ds_exists(caption, cur):
    """
    checks if datasource already exists
    if yes, then delete all associated data from tables
    otherwise generate a new datasource_id
    """
    global ds_id
    cur.execute("""
        select distinct datasource_id
        from tableau_user.tds_datasources
        where datasource_caption = '{}'
        """.format(caption)
        )

    ds_result = cur.fetchall()

    if ds_result:
        ds_id = str(ds_result[0][0])
        for tbl in db_tables:
            cur.execute("""
                delete from {} where datasource_id = {}
                """.format(tbl, ds_id)
            )
    else:
        cur.execute("""
            select coalesce(max(datasource_id), 0)
            from tableau_user.tds_datasources
            """
        )

        ds_result = cur.fetchall()
        ds_id = str(ds_result[0][0] + 1)

    cur.execute("commit")


def replace_formula(string, dict):
    """
    look up if a dictionary key exists in the formula
    if so, replace with the dictionary value (and add square brackets)
    """
    for key in dict:
        if key:
            if key in string:
                value = dict[key]
                if value:
                    string = string.replace(key, '[' + value +']')

    return string


def get_comments(tag):
    string = None
    for run in tag.iter('run'):
        if string:
            string += run.text
            string = string.replace("Æ","")
        else:
            string = run.text

    return string
