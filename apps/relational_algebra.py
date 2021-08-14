from ravel.app import AppConsole
import psycopg2
import tabulate

class RelaAlgConsole(AppConsole):
    def do_echo(self, line):
        print("test", line)

    def default(self, line):
        "Execute a PostgreSQL statement"
        try:
        
            print(line)

            # self.db.cursor.execute(line)
        except psycopg2.ProgrammingError as e:
            print(e)
            return

        # try:
        #     data = self.db.cursor.fetchall()
        #     if data is not None:
        #         names = [row[0] for row in self.db.cursor.description]
        #         print(tabulate.tabulate(data, headers=names))
        # except psycopg2.ProgrammingError:
        #     # no results, eg from an insert/delete
        #     pass
        # except TypeError as e:
        #     print(e)


shortcut = "ra"
description = "execute a relational algebra"
console = RelaAlgConsole