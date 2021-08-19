from ravel.app import AppConsole
import psycopg2
import tabulate
import re
from z3 import *

class RelaAlgConsole(AppConsole):
    def do_echo(self, line):
        print("test", line)

    def default(self, line):
        "Execute a PostgreSQL statement"
        try:
        
            data, condition, z3 = self._get_sql(line)

            for d in data:
                self.db.cursor.execute(d)
            for c in condition:
                if c != '':
                    self.db.cursor.execute(c)
            for z in z3:
                self.db.cursor.execute(z)

        except psycopg2.ProgrammingError as e:
            print(e)
            return

        try:
            print('************************************************************************')
            print(line)
            self.db.cursor.execute("select * from output;")
            data = self.db.cursor.fetchall()
            if data is not None:
                names = [row[0] for row in self.db.cursor.description]
                print(tabulate.tabulate(data, headers=names))
            print('************************************************************************')
        except psycopg2.ProgrammingError:
            # no results, eg from an insert/delete
            pass
        except TypeError as e:
            print(e)

    def do_data(self, line):
        query = line
        
        

    def _get_sql(self, query):

        data = []
        condition = []
        z3 = []

        #print("INPUT: " + query + "\n")
        #t_result = 't_result'
        if 'WHERE' in query:
            qlist = query.split('WHERE')
            query = qlist[0].lower() + ' where ' + qlist[1]
        elif 'where' in query:
            qlist = query.split('where')
            query = qlist[0].lower() + ' where ' + qlist[1]
        else: query = query.lower()    
        #select = re.split("select|from",query)
        p1 = re.compile(r'select(.*?)from', re.S) 
        select =  re.findall(p1, query)[0].strip()

        if 'from' in query:
            if 'where' in query:
                p2 = re.compile(r'from(.*?)where', re.S) 
            else: p2 = re.compile(r'from(.*?);', re.S) 
        table_name =  re.findall(p2, query)[0].strip()

        if 'where' in query:
            p3 = re.compile(r'where(.*?);', re.S) 
            where =  re.findall(p3, query)[0].strip()
            # where = where.upper()
        else:
            where = None

        query_list = re.split("select|from|join|where",query)

        if 'join' in query and 'join' in table_name:
            #print('JOIN CASE')
            table1 = table_name.split('join')[0].strip()
            table2 = table_name.split('join')[1].strip()

            table1_info = table1.lower()
            table2_info = table2.lower()
        
            table1_name =  table1_info.split('(')[0].strip()
            table2_name =  table2_info.split('(')[0].strip()
            #t_result = f"{table1_name}_join_{table2_name}"
            t_result = "output"
            #result = f"Optional: DROP TABLE IF EXISTS {t_result}; \n\n"

            if where == None:
                data, condition = self._cjoin(table1, table2,'')
            else:
                data, condition =  self._cjoin(table1,table2,where)
                #result += f"SELECT * FROM cjoin('{table1}', '{table2}');"
                # result += f"CREATE TABLE {t_result}_temp AS SELECT * from {t_result} where {where};"
                # result += f"DROP TABLE IF EXISTS {t_result};"
                # result += f"ALTER TABLE {t_result}_temp RENAME TO {t_result};"
        else:
            #t_result = f"{table_name}_o"
            t_result = "output"
            
            # print("Step1: Create data content\n")
            
            sql = "DROP TABLE IF EXISTS {}; \n".format(t_result)
            # print(sql)
            data.append(sql)

            # execute postgres SQL
            # self.db.cursor.execute(sql)
    
            q1 = "CREATE UNLOGGED TABLE {} AS {} \n".format(t_result, query)
            # print(q1)
            data.append(q1)

            # execute postgres SQL
            # self.db.cursor.execute(q1)

            if where == None:
                #result = query
                return data, condition, z3

            # print("\nStep2: Update Conditions\n")

        if where != None:

            if "and"  in where: 
                where_list = where.split("and")
                #plpy.info(where_list[0].strip())
            else: 
                where_list = []
                where_list.append(where.strip())

            ##### c may contain '(OR)'

            for disj in where_list:
                has_or = False
                
                if 'or' in disj:
                    if 'and' not in query:
                        disj = disj.strip()
                        c_orlist = disj.split('or')
                    else:
                        disj = disj.strip()[1:-1].strip()
                        c_orlist = disj.split('or')
                    has_or = True
                
                else:   
                    c = disj.strip()

                if has_or:
                    or_exp = ''
                    l_list = []
                    r_list = []
                    for c in c_orlist:
                        p1 = re.compile(r'[(](.*?)[,]', re.S) 
                        left =  re.findall(p1, c)[0].strip()

                        left_is_attr = False

                        if "'" in left:
                            left = left # cons

                        else:  # attr
                            left = left.replace(table_name, t_result)
                            left_is_attr = True
                            if '.' in left:
                                left = left.split('.')[1]
                        # 2nd arg
                        right_is_attr = False
                        p2 = re.compile(r'[,](.*?)[)]', re.S) 
                        right =  re.findall(p2, c)[0].strip()

                        if table_name in right:
                            right = right.replace(table_name, t_result)
                            right_is_attr = True
                            if '.' in right:
                                right = right.split('.')[1]

                        elif "'" in right:
                            right = right # cons

                        if 'not_equal' in c:
                            or_exp += "{} || ' != ' || {} || ',' ||".format(left, right)
                        elif 'equal' in c:
                            or_exp += "{} || ' == ' || {  } || ',' ||".format(left, right)
                        elif 'greater' in c:
                            or_exp += "{} || ' > ' || {  } || ',' ||".format(left, right)
                        elif 'less' in c:
                            or_exp += "{} || ' < ' || {  } || ',' ||".format(left, right)
                        elif 'geq' in c:
                            or_exp += "{} || ' >= ' || {  } || ',' ||".format(left, right)
                        elif 'leq' in c:
                            or_exp += "{} || ' <= ' || { } || ',' ||".format(left, right)

                        left = left.replace("'","")
                        right = right.replace("'","")
                        l_list.append(left)
                        r_list.append(right)
                    or_exp = or_exp[:-9]
                    #or_exp += ")"
                    for idx in range(len(l_list)):
                        
                        #result += f"UPDATE {t_result} SET cond = array_append(cond, {or_exp})  WHERE is_var({t_result}.{l_list[idx]}) Or is_var({t_result}.{r_list[idx]}); \n"

                        if "'" in r_list[idx] or "'" in l_list[idx]:
                            q = "UPDATE {} SET condition = array_append(condition, 'Or(' || {} || ')' ) ; \n".format(t_result, or_exp)

                        else: 
                            q = ''
                        
                        if q not in condition:
                            condition.append(q) 

                            # if q != '':
                            #     print(q)
                                # execute postgres SQL
                                # self.db.cursor.execute(q)

                            
                
                elif not has_or:

                    # 1st arg
                    p1 = re.compile(r'[(](.*?)[,]', re.S) 
                    left =  re.findall(p1, c)[0].strip()

                    left_is_attr = False

                    if "'" in left:
                        left = left # cons

                    else:  # attr
                        left = left.replace(table_name, t_result)
                        left_is_attr = True
                        if '.' in left:
                            left = left.split('.')[1]
                    # 2nd arg
                    right_is_attr = False
                    p2 = re.compile(r'[,](.*?)[)]', re.S) 
                    right =  re.findall(p2, c)[0].strip()

                    if table_name in right:
                        right = right.replace(table_name, t_result)
                        right_is_attr = True
                        if '.' in right:
                            right = right.split('.')[1]

                    elif "'" in right:
                        right = right # cons

                    #result += "Insert SELECT conditions:\n"

                    if 'not_equal' in c:

                        if right_is_attr or left_is_attr:
                            q = "UPDATE {} SET condition = array_append(condition, {} ||' != '|| {}); \n".format(t_result, left, right)
                        else:
                            q = ''
                        condition.append(q)

                        # if q != '':
                        #     print(q)
                            # execute postgres SQL
                            # self.db.cursor.execute(q)

                    elif 'equal' in c:

                        if right_is_attr or left_is_attr:
                            q = "UPDATE {} SET condition = array_append(condition, {} ||' == '|| {}) ;\n".format(t_result, left, right)
                        else:
                            q = ''
                        condition.append(q)

                        # if q != '':
                        #     print(q)
                            # execute postgres SQL
                            # self.db.cursor.execute(q)

                    elif 'greater' in c:

                        if right_is_attr or left_is_attr:
                            q = "UPDATE {} SET condition = array_append(condition, {} ||' > '|| {}) ;\n".format(t_result, left, right)
                        else:
                            q = ''
                        condition.append(q)

                        # if q != '':
                        #     print(q)
                            # execute postgres SQL
                            # self.db.cursor.execute(q)

                    elif 'less' in c:

                        if right_is_attr or left_is_attr:
                            q = "UPDATE {} SET condition = array_append(condition, {} ||' < '|| {});\n".format(t_result, left, right)

                        else:
                            q = ''
                        condition.append(q)

                        # if q != '':
                        #     print(q)
                            # execute postgres SQL
                            # self.db.cursor.execute(q)

                    elif 'geq' in c:

                        if right_is_attr or left_is_attr:
                            q = "UPDATE {} SET condition = array_append(condition, {} ||' >= '|| {}) ;\n".format(t_result, left, right)
                        else:
                            q = ''
                        condition.append(q)

                        # if q != '':
                        #     print(q)
                            # execute postgres SQL
                            # self.db.cursor.execute(q)

                    elif 'leq' in c:

                        if right_is_attr or left_is_attr:
                            q = "UPDATE {} SET condition = array_append(condition, {} ||' <= '|| {});\n".format(t_result, left, right)
                        else:
                            q = ''
                        condition.append(q)

                        # if q != '':
                        #     print(q)
                            # execute postgres SQL
                            # self.db.cursor.execute(q)

        #result += f"SELECT * FROM {t_result};\n"
        # print("\nStep3: Normalization\n")

        q_contra = "DELETE FROM {} WHERE is_contradiction({}.condition);\n".format(t_result, t_result)

        z3.append(q_contra)

        # print(q_contra)
        # execute postgres SQL
        # self.db.cursor.execute(q_contra)

        q_tauto = "UPDATE {} SET condition = '{{}}' WHERE is_tauto({}.condition);\n".format(t_result, t_result)

        z3.append(q_tauto)
        # print(q_tauto)
        # execute postgres SQL
        # self.db.cursor.execute(q_tauto)


        q_rm = "UPDATE {} SET condition = remove_redundant(condition) where has_redundant(condition);\n".format(t_result)
        z3.append(q_rm)

        # print(q_rm)
        # execute postgres SQL
        # self.db.cursor.execute(q_rm)

        # q_projection = f"SELECT {select} from {t_result};\n"
        # result += q_projection



        return data, condition, z3

    def _cjoin(self, table1_info,table2_info,where):
        data = []
        condition = []
        #t_result = 't_result'

        table1_info = table1_info.lower()

        table2_info = table2_info.lower()

        table1 =  table1_info.split('(')[0].strip()
        table2 =  table2_info.split('(')[0].strip()

        t_result = "output"
        #t_result = f"{table1}_join_{table2}"
        p2 = re.compile(r'[(](.*?)[)]', re.S)
        t1_attr =  re.findall(p2, table1_info)[0].strip().split(',')
        t1_attr = [v.strip() for v in t1_attr ]
        t2_attr =  re.findall(p2, table2_info)[0].strip().split(',')
        t2_attr = [v.strip() for v in t2_attr ]

        common_attr=[val for val in t1_attr if val in t2_attr and val != 'condition']
        union_attr = list(set(t1_attr).union(set(t2_attr)))

        # print("Step1: Create Data Content\n")
        sql = "DROP TABLE IF EXISTS {};\n".format(t_result)
        # print(sql)
        data.append(sql) 

        # execute postgres SQL
        # self.db.cursor.execute(sql)

        slt_attr = ""

        for a in t1_attr:
            if a not in common_attr and a != "condition":
                slt_attr += " {}.{}, ".format(table1, a)
        
        for a in t2_attr:
            if a not in common_attr and a != "condition":
                slt_attr += "{}.{},".format(table2, a)

        for a in common_attr:
            slt_attr += "{}.{}, {}.{} AS {}_{},".format(table1, a, table2, a, table2, a)

        if "condition" in t1_attr and "condition" in t2_attr:
            slt_attr += "array_cat({}.condition, {}.condition) AS condition,".format(table1, table2)
            #slt_attr += f" {table1}.cond AS cond, {table2}.cond AS {table2}_cond,"
        elif "condition" in t1_attr:
            slt_attr += " {}.condition AS condition,".format(table1)
        elif "condition" in t2_attr:
            slt_attr += " {}.condition AS {}condition,".format(table2, table2)

        slt_attr = slt_attr[:-1]

        join_cond = ""

        for a in common_attr:
            join_cond += " equal({}.{}, {}.{}) AND".format(table1, a, table2, a)
        join_cond = join_cond[:-3]

        
        if where != '':
            where_cond = ''
            if "{}.".format(table1) or "{}.".format(table2) in where:
                where.replace("{}.".format(table1),'').replace("{}.".format(table2),'')
            where_1 = where[:]
            where_2 = where[:]
            for c in common_attr:
                if c in where_1:
                    where_1 = where_1.replace(c, "{}.{}".format(table1, c))
                    where_2 = where_2.replace(c, "{}.{}".format(table2, c))
            #where_cond = f"({where_1}) and ({where_2})"   
            where_cond = "{}".format(where_1)     
            sql = "CREATE UNLOGGED TABLE {} AS SELECT {} FROM {} INNER JOIN {} on {} WHERE {}; \n".format(t_result, slt_attr, table1, table2, join_cond, where_cond)
            data.append(sql)
            
            # print(sql)
            # execute postgres SQL
            # self.db.cursor.execute(sql)

        else:
            sql = "CREATE UNLOGGED TABLE {} AS SELECT {} FROM {} INNER JOIN {} on {}; \n".format(t_result, slt_attr, table1, table2, join_cond)
            data.append(sql)

            # print(sql)
            # execute postgres SQL
            # self.db.cursor.execute(sql)
        
        #result += f"SELECT * FROM {t_result};\n"

        # print("\nStep2: Update Conditions\n")

        #result += f"UPDATE {t_result} SET cond =  array_cat(cond, {table2}_cond);\n"
        # print("\n2.1: Insert Join Conditions\n")
        for attr in common_attr:
            #result += f"UPDATE {t_result} SET cond = array_append(cond, {attr} || ' == ' || {table2}_{attr})  WHERE  (is_var({t_result}.{attr}) OR is_var({t_result}.{table2}_{attr}) );"
            sql = "UPDATE {} SET condition = array_append(condition, {} || ' == ' || {}_{});\n".format(t_result, attr, table2, attr)
            condition.append(sql)

            # print(sql)
            # execute postgres SQL
            # self.db.cursor.execute(sql)
        join_attr = ""

        # print("2.2: Projection and drop duplicated attributes\n")
        for attr in common_attr:
            sql = "UPDATE {} SET {} = {}_{} WHERE not is_var({});\n".format(t_result, attr, table2, attr, attr)
            condition.append(sql)
            
            # print(sql)
            # execute postgres SQL
            # self.db.cursor.execute(sql)

        q_dropcol = ''
        for attr in common_attr: 
            q_dropcol += "DROP COLUMN {}_{},".format(table2, attr)
        
        q_dropcol = q_dropcol[:-1]
         
        sql = "ALTER TABLE {} {}; \n".format(t_result, q_dropcol)
        condition.append(sql)

        # print(sql)
        # execute postgres SQL
        # self.db.cursor.execute(sql)

        #ALTER TABLE t_result DROP COLUMN dest, DROP COLUMN path;

        return data, condition

shortcut = "s"
description = "execute a relational algebra"
console = RelaAlgConsole