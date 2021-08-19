from ravel.app import AppConsole
import psycopg2
import tabulate

rib_file = './topo/RouteView/rib.txt'
upd_file = './topo/RouteView/update.txt'

class BGPConsole(AppConsole):
    def do_echo(self, line):
        print("test", line)
    
    '''
    Load realistic data from RouteView RIB and UPDATE
    1. bgp_policy and candidate routes are generated from RIB
    2. routes_delta are generated from UPDATE
    default file: 2021.06.10 00:00
    '''
    def do_load(self,line):
        "Load data which inferred from BGP RouteView RIB and UPDATE file"
        # args = line.split()
        # if len(args) != 2:
        #     print("Invalid syntax") 
        #     return
        
        # rib_file = args[0]
        # upd_file = args[1]
        ptable, rtable, update_table = self._gen_ptable()
        self.db.cursor.executemany("INSERT INTO bgp_policy VALUES (%s, %s, %s, %s);", ptable)
        self.db.cursor.executemany("INSERT INTO routes(dest, path, min_len) VALUES (%s, %s, %s);", rtable)
        self.db.cursor.executemany("INSERT INTO routes_delta(dest, operation, path, len_path) VALUES (%s, %s, %s, %s);", update_table)
    
    def do_loaddemo(self, line):
        "Load sample data which inferred from BGP RouteView RIB and UPDATE file"
        ptable = [
            ['1.6.68.0/22', 'x579', 2, '{"l(x579) <= 2"}'],
            ['1.6.4.0/22', 'x531', 2, '{"l(x531) <= 2"}'],
            ['1.6.188.0/24', 'y5', -1, '{"y5 == 3303 6453 9583"}'],
            ['1.22.208.0/24', 'z2', -1, '{"z2 == nopath"}']
        ]

        rtable = [['1.6.4.0/22', '3303 6453 9583', 2], ['1.6.4.0/22', '23673 9583', 2], ['1.6.4.0/22', '20912 3257 1299 9583', 2], ['1.6.4.0/22', '8492 31133 174 9583', 2], ['1.6.4.0/22', '3561 209 3356 1299 9583', 2], ['1.6.4.0/22', '7018 1299 9583', 2], ['1.6.4.0/22', '31019 43531 9583', 2], ['1.6.4.0/22', '34224 6453 9583', 2], ['1.6.4.0/22', '37100 6461 7473 9583', 2], ['1.6.4.0/22', '3549 3356 1299 9583', 2], ['1.6.4.0/22', '3130 2914 1299 9583', 2], ['1.6.4.0/22', '2152 7473 9583', 2], ['1.6.4.0/22', '293 3320 9583', 2], ['1.6.4.0/22', '6939 9583', 2], ['1.6.4.0/22', '2497 3491 9583', 2], ['1.6.4.0/22', '53767 14315 6453 6453 9583', 2], ['1.6.4.0/22', '3257 1299 9583', 2], ['1.6.4.0/22', '701 3320 9583', 2], ['1.6.4.0/22', '5413 6461 7473 9583', 2], ['1.6.4.0/22', '22652 6461 7473 9583', 2], ['1.6.4.0/22', '3741 174 9583', 2], ['1.6.4.0/22', '7660 4635 3491 3491 9583', 2], ['1.6.4.0/22', '2914 3491 9583', 2], ['1.6.4.0/22', '24441 3491 3491 9583', 2], ['1.6.4.0/22', '1221 4637 3320 9583', 2], ['1.6.4.0/22', '11686 174 9583', 2], ['1.6.4.0/22', '20130 6939 9583', 2], ['1.6.4.0/22', '57866 6461 7473 9583', 2], ['1.6.4.0/22', '49788 1299 9583', 2], ['1.6.4.0/22', '18106 9583', 2], ['1.6.4.0/22', '1403 1299 9583', 2], ['1.6.4.0/22', '3549 3356 174 9583', 2], ['1.6.4.0/22', '1299 9583', 2], ['1.6.4.0/22', '3130 1239 6453 9583', 2], ['1.6.68.0/22', '3303 6453 9583', 2], ['1.6.68.0/22', '23673 3491 174 9583', 2], ['1.6.68.0/22', '20912 3257 1299 9583', 2], ['1.6.68.0/22', '8492 31133 174 9583', 2], ['1.6.68.0/22', '3561 209 3356 1299 9583', 2], ['1.6.68.0/22', '7018 1299 9583', 2], ['1.6.68.0/22', '31019 43531 9583', 2], ['1.6.68.0/22', '34224 6453 9583', 2], ['1.6.68.0/22', '37100 6453 9583', 2], ['1.6.68.0/22', '3130 2914 1299 9583', 2], ['1.6.68.0/22', '22652 6453 9583', 2], ['1.6.68.0/22', '293 3320 9583', 2], ['1.6.68.0/22', '6939 9583', 2], ['1.6.68.0/22', '3549 3356 1299 9583', 2], ['1.6.68.0/22', '2914 6453 9583', 2], ['1.6.68.0/22', '53767 14315 6453 6453 9583', 2], ['1.6.68.0/22', '3257 1299 9583', 2], ['1.6.68.0/22', '701 3320 9583', 2], ['1.6.68.0/22', '49788 1299 9583', 2], ['1.6.68.0/22', '5413 1299 9583', 2], ['1.6.68.0/22', '7660 2516 3320 9583', 2], ['1.6.68.0/22', '2497 6453 9583', 2], ['1.6.68.0/22', '3741 174 9583', 2], ['1.6.68.0/22', '57866 1299 9583', 2], ['1.6.68.0/22', '2152 3356 6453 9583', 2], ['1.6.68.0/22', '24441 3491 3491 6453 9583', 2], ['1.6.68.0/22', '1221 4637 3320 9583', 2], ['1.6.68.0/22', '11686 174 9583', 2], ['1.6.68.0/22', '20130 6939 9583', 2], ['1.6.68.0/22', '18106 6939 9583', 2], ['1.6.68.0/22', '1403 1299 9583', 2], ['1.6.68.0/22', '3549 3356 174 9583', 2], ['1.6.68.0/22', '1299 9583', 2], ['1.6.68.0/22', '3130 1239 6453 9583', 2], ['1.6.188.0/24', '3303 6453 9583', -1], ['1.6.188.0/24', '23673 3491 174 9583', -1], ['1.6.188.0/24', '20912 3257 1299 9583', -1], ['1.6.188.0/24', '8492 31133 174 9583', -1], ['1.6.188.0/24', '3561 209 3356 1299 9583', -1], ['1.6.188.0/24', '7018 1299 9583', -1], ['1.6.188.0/24', '31019 43531 9583', -1], ['1.6.188.0/24', '34224 6453 9583', -1], ['1.6.188.0/24', '37100 6453 9583', -1], ['1.6.188.0/24', '49788 1299 9583', -1], ['1.6.188.0/24', '3130 2914 1299 9583', -1], ['1.6.188.0/24', '22652 6453 9583', -1], ['1.6.188.0/24', '293 3320 9583', -1], ['1.6.188.0/24', '6939 9583', -1], ['1.6.188.0/24', '3549 3356 1299 9583', -1], ['1.6.188.0/24', '2914 6453 9583', -1], ['1.6.188.0/24', '53767 14315 6453 6453 9583', -1], ['1.6.188.0/24', '3257 1299 9583', -1], ['1.6.188.0/24', '701 3320 9583', -1], ['1.6.188.0/24', '5413 1299 9583', -1], ['1.6.188.0/24', '7660 2516 3320 9583', -1], ['1.6.188.0/24', '2497 6453 9583', -1], ['1.6.188.0/24', '3741 174 9583', -1], ['1.6.188.0/24', '57866 1299 9583', -1], ['1.6.188.0/24', '2152 3356 6453 9583', -1], ['1.6.188.0/24', '24441 3491 3491 6453 9583', -1], ['1.6.188.0/24', '1221 4637 3320 9583', -1], ['1.6.188.0/24', '11686 174 9583', -1], ['1.6.188.0/24', '20130 6939 9583', -1], ['1.6.188.0/24', '18106 6939 9583', -1], ['1.6.188.0/24', '1403 1299 9583', -1], ['1.6.188.0/24', '3549 3356 174 9583', -1], ['1.6.188.0/24', '1299 9583', -1], ['1.6.188.0/24', '3130 2914 1299 9583', -1], ['1.22.208.0/24', '3303 9498 45528', -1]]

        update_table = [
            ['1.6.68.0/22', 'A', '3130 2914 1299 9583', 4],
            ['1.6.68.0/22', 'A', '9583', 1],
            ['1.6.4.0/22', 'A', '3130 2914 1299 9583', 4],
            ['1.6.188.0/24', 'A','2914 6453 9583',  3 ],
            ['1.22.208.0/24', 'A', '3130 2914 1299 9498 45528', 5]
        ]

        self.db.cursor.executemany("INSERT INTO bgp_policy VALUES (%s, %s, %s, %s);", ptable)
        self.db.cursor.executemany("INSERT INTO routes(dest, path, min_len) VALUES (%s, %s, %s);", rtable)
        self.db.cursor.executemany("INSERT INTO routes_delta(dest, operation, path, len_path) VALUES (%s, %s, %s, %s);", update_table)

    def do_join(self, line):
        """Do join operation. 
            Format: join <table_1> <table_2>"""
        args = line.split()
        if len(args) != 2:
            print("Invalid syntax") 
            return
        
        policy = args[0]
        routes = args[1]

        self.db.cursor.execute("select * from {};".format(policy))
        policy_cols = [row[0] for row in self.db.cursor.description]

        self.db.cursor.execute("select * from {};".format(routes))
        routes_cols = [row[0] for row in self.db.cursor.description]

        common_attr= set(policy_cols).intersection(set(routes_cols)) - set(['condition'])
        union_attr = set(policy_cols).union(set(routes_cols)) - set(['condition'])
        diff_attr = union_attr - common_attr

        print("common: ", common_attr)
        print("diff: ", diff_attr)

        sql_attr = ""
        sql_equal = ""
        for c in common_attr:
            sql_attr += "{}.{}, {}.{} AS {}_{},".format(policy, c, routes, c, routes, c)

            if 'len' in c:
                sql_equal += "{}.{} = {}.{} and ".format(policy, c, routes, c)
            else:
                sql_equal += "equal({}.{}, {}.{}) and ".format(policy, c, routes, c)

        sql_equal = sql_equal[: -4]

        for d in diff_attr:
            sql_attr += "{},".format(d)

        sql_attr += "{}.condition".format(policy)

        name = "{}_join_{}".format(policy, routes)
        try:
            print("select * from {}(dest, path, min_len, condition) join {}(dest, path, min_len)\n".format(policy, routes))
            print("Step1: Create Data Content")
            print("DROP TABLE IF EXISTS {};".format(name))
            self.db.cursor.execute("DROP TABLE IF EXISTS {};".format(name))

            sql = "CREATE UNLOGGED TABLE {} AS SELECT ".format(name) + \
                    sql_attr + \
                    " FROM {}, {} where ".format(policy, routes) + \
                    sql_equal + "; "
            print(sql)
            self.db.cursor.execute(sql)

            print("\nStep2: Update Conditions\n \
                    2.1: Insert Join Conditions")

            for c in common_attr:
                sql = "UPDATE {} SET condition = array_append(condition, {} || ' == ' || {}_{});".format(name, c, routes, c)
                print(sql)
                self.db.cursor.execute(sql)

            sql = "update {} set condition = array_append(condition, 'l(' || path || ') == ' || l({}_path));".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            print("2.2: Projection and drop duplicated attributes")

            for c in common_attr:
                if 'len' in c:
                    sql = "UPDATE {} SET {} = {}_{} WHERE {} > {}_{};".format(name, c, routes, c, c, routes, c)
                else:
                    sql = "UPDATE {} SET {} = {}_{} WHERE not is_var({});".format(name, c, routes, c, c)
                print(sql)
                self.db.cursor.execute(sql)

            drop = ""
            for c in common_attr:
                drop = drop + "DROP COLUMN " + routes +  "_" + c + ","
            drop = drop[:-1]
            sql = "ALTER TABLE {} {};".format(name, drop)
            print(sql)
            self.db.cursor.execute(sql)

            print("\nStep3: Normalization\n")
            sql = "DELETE FROM {} WHERE is_contradiction(condition);".format(name)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET condition = '{{}}' WHERE is_tauto(condition);".format(name)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET condition = remove_redundant(condition) where has_redundant(condition);".format(name)
            print(sql)
            self.db.cursor.execute(sql) 

            # print("\nStep 4: extending values")
            # sql = "DROP TABLE IF EXISTS current_best_routes;"
            # print(sql)
            # self.db.cursor.execute(sql)

            # sql = "create table current_best_routes as select dest, set_path_val(path, condition) as path, min_len from {};".format(name)
            # print(sql)
            # self.db.cursor.execute(sql)

        except psycopg2.ProgrammingError as e:
            print(e)
            return

        try:
            print('\n************************************************************************')
            self.db.cursor.execute("select * from {};".format(name))
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

    def do_extend_values(self, line):
        """Extend values in condtion column to variables and rename the table name
           Usage: extend_values [table] [new_name] ..."""
        args = line.split()
        if len(args) != 2:
            print("Invalid syntax") 
            return

        old_name = args[0]
        new_name = args[1]

        try:
            sql = "DROP TABLE IF EXISTS {};".format(new_name)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "create table {} as select dest, set_path_val(path, condition) as path, min_len from {};".format(new_name, old_name)
            print(sql)
            self.db.cursor.execute(sql)
        except psycopg2.ProgrammingError as e:
            print(e)
            return

        try:
            print('\n************************************************************************')
            self.db.cursor.execute("select * from {};".format(new_name))
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

    '''
    Update Policy
    arg1: policy table
    arg2: routes_delta
    '''
    def do_update_policy(self, line):
        """Update current bgp policy that affecting by bgp announcement
            Usage: update_policy [policy] [delta]"""
        args = line.split()
        if len(args) != 2:
            print("Invalid syntax") 
            return
        
        policy = args[0]
        delta = args[1]

        try:
            sql = "UPDATE {} \
                    SET min_len = {}.len_path, \
                    condition = ARRAY['l(' || {}.path || ') <= ' || {}.len_path] \
                    FROM {} \
                    WHERE {}.min_len > {}.len_path \
                    AND {}.dest = {}.dest;".format(policy, delta, policy, delta, delta, policy, delta, policy, delta)
            
            print(sql)
            self.db.cursor.execute(sql)
        except psycopg2.ProgrammingError as e:
            print(e)
            return



    '''
    Union
    arg1: current_best_routes
    arg2: routes_delta
    '''
    def do_union(self, line):
        """Union operation. 
            Usage: union [table1] [table2]"""
        args = line.split()
        if len(args) != 2:
            print("Invalid syntax") 
            return
        
        current = args[0]
        delta = args[1]

        name = "{}_union_{}".format(current, delta)

        try:
            sql = "DROP TABLE IF EXISTS {};".format(name)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "create table {} as select dest, path, min_len as len_path \
                    from {} \
                    where dest in (\
                        select dest from {} \
                    ) \
                    union select dest, path, len_path \
                    from {};".format(name, current, delta, delta)
            print(sql)
            self.db.cursor.execute(sql)
        except psycopg2.ProgrammingError as e:
            print(e)
            return
        
        try:
            print('\n************************************************************************')
            self.db.cursor.execute("select * from {};".format(name))
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
    
    def do_rename(self, line):
        """Rename table name or column name. 
            Usage: rename [table] [new_table]
                   rename [table] [col] [new_col]"""
        args = line.split()
        type = args[0]
        if type == 'table' and len(args) != 3:
            print("Invalid syntax. Format: rename table table_name new_table_name") 
            return
        elif type == 'col' and len(args) != 4:
            print("Invalid syntax. Format: rename col table_name col_name new_col_name") 
            return

        if type == 'table':
            old_name = args[1]
            new_name = args[2]

            sql = "ALTER TABLE {} RENAME TO {}".format(old_name, new_name)
            print(sql)
            self.db.cursor.execute(sql)

        elif type == 'col':
            table_name = args[1]
            old_name = args[2]
            new_name = args[3]

            sql = "ALTER TABLE {} RENAME COLUMN {} to {}".format(table_name, old_name, new_name)

        
        
    '''
    Tool function for gen_ptable()
    '''
    def _addtwodimdict(self, thedict, key_a, key_b, val): 
        if key_a in thedict:
            thedict[key_a].update({key_b: val})
        else:
            thedict.update({key_a:{key_b: val}})  

    '''
    Generate policy, condidates routes and routes_delta
    1. policy: a. shortest path policy
               b. static routes policy
               c. filter policy
    2. candidate routes
    3. delta routes: a. annoucement
                     b. withdrawal
    '''
    def _gen_ptable(self, filename = rib_file, update_file = upd_file, size1 = 333,size2 = 333,size3 = 334,symbol1 = 'x', symbol2 = 'y', symbol3 = 'z'):
        # policy, routing, rib = gen_all(rib_file, 'x', 'W', size1)
        # for i in range(0,5):
        #     print(policy[i])
        ctable = [] # policy table
        rtable = [] # route condidate table
        update_table =[] # UPDATE table
        upd_set = set() # check update duplication

        ips = set()
        p1 = []
        p2 = []
        p3 = []

        count1 = 0
        count2 = 0
        count3 = 0

        fo = open(filename, 'r')

        ips1 = set()
        ips2 = set()
        ips3 = set()
        spath_dict = {}

        temp_table = []

        p1_ctable = []
        p2_ctable = []
        p3_ctable = []
        p1_rtable = []
        p2_rtable = []
        p3_rtable = []

        var_count1 = 1
        var_count2 = 1
        var_count3 = 1
        p3_cond = []
        already_dict = {}
        for line in fo:

            record = line.split('|')

            ip = record[5]
            path = record[6].replace('{', '').replace('}', '')
            s_path = len(path.split(' '))
            
            # P1
            if ip in ips1: 
                if s_path < spath_dict[ip]:
                    spath_dict[ip] = s_path
                    self._addtwodimdict(already_dict, ip, s_path, False)
                if[ip, path] not in temp_table:
                    temp_table.append([ip, path])
                    self._addtwodimdict(already_dict, ip, s_path, False)
                continue
            elif ip not in ips1 and count1 < size1:
                ips1.add(ip)
                spath_dict[ip] = s_path
                self._addtwodimdict(already_dict, ip, s_path, False)
                temp_table.append([ip, path])
                count1 +=1
                continue

            # P2 static routing 

            if ip not in ips2 and count2 < size2:
                ips2.add(ip)
                symbol = symbol2 + str(var_count2)
                cond = [symbol + ' == '  + path  ]
                var_count2  += 1
                p2_ctable.append([ip,symbol, -1, cond ])    
                count2 +=1

                # p2_rtable.append([ip,path, -1, []])
                p2_rtable.append([ip,path, -1])
                continue         
            elif ip in ips2:
                # p2_rtable.append([ip,path, -1,[]])
                p2_rtable.append([ip,path, -1])
                continue

            # p3: filter
            
            if ip not in ips3 and count3 < size3:
                ips3.add(ip)

                s1 = symbol3 + str(var_count3)
                s2= symbol3 + str(int(var_count3) + 1)

                #p3_cond.append(symbol + ' != ' + ip)
                p3_ctable.append([ip,s2, -1, [str(s2) + ' == nopath'] ])
                p3_rtable.append([ip,path, -1])
                count3 += 1
                continue


        #p1 
        ips1_temp = set()
        for t in temp_table:
            if t[0] not in ips1_temp:

                cond = ['l(' + symbol1 + str(var_count1)+ ')' + ' <= ' + str(spath_dict[t[0]])]

                p1_ctable.append([t[0], symbol1 +  str(var_count1) , spath_dict[t[0]], cond]  )
                var_count1 += 2
                ips1_temp.add(t[0])
            # p1_rtable.append([t[0],t[1], spath_dict[t[0]], [] ])
            p1_rtable.append([t[0],t[1], spath_dict[t[0]]])
        
        # p3
        # num = var_count3 + 1
        # p3_ctable.append([symbol3+str(var_count3),symbol3+str(num), '_', p3_cond  ])



        # UPDATE table
        upf = open(update_file, 'r')

        for line in upf:

            record = line.split('|')

            ip = record[5]
            if ip in ips1 or ip in ips2 or ip in ips3:
                path = record[6].replace('{', '').replace('}', '')
                s_path = len(path.split(' '))
                if 'A' in record[2]:
                    operation = 'A'
                else: 
                    operation = 'W'

                list = [ip,operation, path, s_path]
                s = " ".join([str(item) for item in list]) # list to string

                if s not in upd_set: # if str in upd_set that means this record already in update_table
                    update_table.append(list)
                    upd_set.add(s)

        for i in range(5):
            print(p1_ctable[i])

        for i in range(5):
            print(p1_rtable[i])

        for i in range(5):
            print(p2_ctable[i])
        for i in range(5):
            print(p2_rtable[i])

        #print(p3_ctable)
        for i in range(5):
            print(p3_ctable[i])    
        for i in range(5):
            print(p3_rtable[i])     
        ctable = p1_ctable + p2_ctable + p3_ctable

        rtable = p1_rtable + p2_rtable + p3_rtable

        print(len(ctable), len(rtable), len(update_table))


        return ctable , rtable, update_table



shortcut = "bgp"
description = "BGP simulation"
console = BGPConsole

if __name__ == '__main__':
    bgp = BGPConsole()
    bgp.do_loaddata()
