import tds_doc as td

# initialise menus
menu = [
    "LOG IN TO DATABASE",
    "ADD FILE",
    "REMOVE FILE",
    "PARSE SELECTED FILES AND LOAD TO DATABASE",
    "CREATE SCHEMA FROM SCRATCH",
    "QUIT",
    # "--",
    # "IMPORT DATA",
    # "EXPORT DATA"
]

# build main menu
def getMenuChoice(aMenu):
    """
    Takes a list of strings as input, displays as a numbered menu
    and loops until user selects a valid numbered
    """
    if not aMenu:
        raise ValueError("No Menu Content")

    while True:
        print("\n")
        print("-" * 75)
        print("     TABLEAU DOCUMENTATION TOOL")
        print("-" * 75)

        for index, item in enumerate(aMenu, start = 1):
            print(index, "\t", item)

        print("\n" + td.host_logged_in())
        print("Files ready to load:")
        for i in td.selected_files:
            print("-", i)

        try:
            choice = int(input("\nChoose a menu option:"))
            if 1 <= choice <= len(aMenu):
                return choice
            else:
                print("Choose a number between 1 and", len(aMenu))

        except ValueError:
            print("Choose the number of a menu option.")


# add files to selected files list
def addFiles():
    flist = td.get_filenames()
    if not flist:
        raise ValueError("No More Files Available")

    print("\n")
    for k, v in enumerate(flist, start = 1):
        print(k, v, sep = '\t')

    fileIndex = int(input("\nChoose a file to add:"))
    selected_file = flist[fileIndex - 1]
    td.add_files(selected_file)


# build files menu after selecting files option
def removeFiles():
    flist = td.selected_files
    if not flist:
        raise ValueError("Nothing to remove!")

    print("\n")
    for k, v in enumerate(flist, start = 1):
        print(k, v, sep = '\t')

    fileIndex = int(input("\nChoose a file to remove:"))
    selected_file = flist[fileIndex - 1]
    td.remove_files(selected_file)


def loadToDatabase(xml, cur, f):
    td.data_ds(xml, cur, f)
    td.data_columns(xml, cur)
    td.data_relations(xml, cur)
    td.data_folders(xml, cur)
    td.data_drill_paths(xml, cur)


def dummy(n):
    return ("\n*** Sorry! Option %s is not yet available! ***", n)

def quit():
    """
    Quit
    """
    print("Goodbye...")
    raise SystemExit


def executeChoice(choice):
    """
    Execute the choice the user selects in the menu
    """
    global parsedXML
    global cursor
    if choice == 1:
        cursor = td.login_db()
        print("Login Successful!")
    if choice == 2:
        addFiles()
    if choice == 3:
        removeFiles()
    if choice == 4:
        for f in td.selected_files:
            parsedXML = td.parse_xml(f)
            loadToDatabase(parsedXML, cursor, f)
            print(f, "loaded successfully to database!")
        td.selected_files = []
    if choice == 5:
        print("""
            *** WARNING! ***
            This will wipe out all of your data in the database!
            """)
        confirm = input("Enter 'Y' to continue. Press any other key to abort.")
        if confirm == "Y":
            td.create_dbschema(cursor)
        else:
            print("Action aborted!")
    if choice == 6:
        td.logout_db()
        quit()
    # if choice == 8:
    #     td.import_data()
    # if choice == 9:
    #     td.export_data()


def main():
    while True:
        choice = getMenuChoice(menu)
        executeChoice(choice)


if __name__ == "__main__": main()
