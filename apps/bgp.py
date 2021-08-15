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
    def do_loaddata(self,line):
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
    
    def do_best_routes(self, policy, routes):
        name = "{}_join_{}".format(policy, routes)
        try:
            print("Step1: Create Data Content")
            print("DROP TABLE IF EXISTS output;")
            self.db.cursor.execute("DROP TABLE IF EXISTS {}};".format(name))

            sql = "CREATE UNLOGGED TABLE {} \
                    AS SELECT {}.dest, {}.dest AS {}_dest, \
                    {}.path, {}.path AS {}_path, \
                    {}.min_len, {}.min_len AS {}_min_len, \
                    {}.condition \
                    FROM {}, {} where equal({}.dest, {}.dest) \
                    AND equal({}.path, {}.path) \
                    AND {}.min_len = {}.min_len ; ".format(name, policy, routes, routes, policy, routes, routes, policy, routes, routes, policy, policy, routes, policy, routes, policy, routes, policy, routes)
            print(sql)
            self.db.cursor.execute(sql)

            print("\nStep2: Update Conditions\n \
                    2.1: Insert Join Conditions")

            sql = "UPDATE {} SET condition = array_append(condition, dest || ' == ' || {}_dest);".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET condition = array_append(condition, path || ' == ' || {}_path);".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET condition = array_append(condition, min_len || ' == ' || {}_min_len);".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "update {} set condition = array_append(condition, 'l(' || path || ') == ' || l({}_path));".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            print("2.2: Projection and drop duplicated attributes")

            sql = "UPDATE {} SET dest = {}_dest WHERE not is_var(dest);".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET path = {}_path WHERE not is_var(path);".format(name, routes)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET min_len = {}_min_len WHERE min_len > {}_min_len;".format(name, routes, routes)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "ALTER TABLE {} DROP COLUMN {}_dest,DROP COLUMN {}_path,DROP COLUMN {}_min_len;".format(name, routes, routes)
            print(sql)
            self.db.cursor.execute(sql)

            print("\nStep3: Normalization\n")
            sql = "DELETE FROM {} WHERE is_contradiction(condition);".format(name)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET condition = '{}' WHERE is_tauto(condition);".format(name)
            print(sql)
            self.db.cursor.execute(sql)

            sql = "UPDATE {} SET condition = remove_redundant(condition) where has_redundant(condition);".format(name)
            print(sql)
            self.db.cursor.execute(sql) 

            print("\nStep 4: extending values")
            sql = "DROP TABLE IF EXISTS current_best_routes;"
            print(sql)
            self.db.cursor.execute(sql)

            sql = "create table current_best_routes as \
                    select dest, set_path_val(path, condition) as path, \
                    len_path\
                    from {};".format(name)
            print(sql)
            self.db.cursor.execute(sql)

        except psycopg2.ProgrammingError as e:
            print(e)
            return

        try:
            print('************************************************************************')
            print("")
            self.db.cursor.execute("select * from current_best_routes;")
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
