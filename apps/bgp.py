from ravel.app import AppConsole

rib_file = './topo/RouteView/rib.txt'
upd_file = './topo/RouteView/update.txt'

class BGPConsole(AppConsole):
    def do_echo(self, line):
        print("test", line)
    
    def do_loaddata(self,line):
        # args = line.split()
        # if len(args) != 2:
        #     print("Invalid syntax") 
        #     return
        
        # rib_file = args[0]
        # upd_file = args[1]
        ptable, rtable, update_table = self.gen_ptable()
        self.db.cursor.executemany("INSERT INTO bgp_policy VALUES (%s, %s, %s, %s);", ptable)
        self.db.cursor.executemany("INSERT INTO routes VALUES (%s, %s);", rtable)
        self.db.cursor.executemany("INSERT INTO routes_delta VALUES (%s, %s, %s, %s);", update_table)

        

    def addtwodimdict(self, thedict, key_a, key_b, val): 
        if key_a in thedict:
            thedict[key_a].update({key_b: val})
        else:
            thedict.update({key_a:{key_b: val}})  

    def gen_ptable(self, filename = rib_file, update_file = upd_file, size1 = 333,size2 = 333,size3 = 334,symbol1 = 'x', symbol2 = 'y', symbol3 = 'z'):
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
                    self.addtwodimdict(already_dict, ip, s_path, False)
                if[ip, path] not in temp_table:
                    temp_table.append([ip, path])
                    self.addtwodimdict(already_dict, ip, s_path, False)
                continue
            elif ip not in ips1 and count1 < size1:
                ips1.add(ip)
                spath_dict[ip] = s_path
                self.addtwodimdict(already_dict, ip, s_path, False)
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
                p2_rtable.append([ip,path])
                continue         
            elif ip in ips2:
                # p2_rtable.append([ip,path, -1,[]])
                p2_rtable.append([ip,path])
                continue

            # p3: filter
            
            if ip not in ips3 and count3 < size3:
                ips3.add(ip)

                s1 = symbol3 + str(var_count3)
                s2= symbol3 + str(int(var_count3) + 1)

                #p3_cond.append(symbol + ' != ' + ip)
                p3_ctable.append([ip,s2, -1, [str(s2) + ' == nopath'] ])
                p3_rtable.append([ip,path ])
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
            p1_rtable.append([t[0],t[1] ])
        
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
