import os
import sys
import MySQLdb 
reload(os.sys)
os.sys.setdefaultencoding('utf8')


class Reader(object):
    def __init__(self):
        if not hasattr(self, 'conn'):
            dbdict = eval(sys.argv[3]) #conn conf
            host = dbdict.get('host')
            user = dbdict.get('user')
            pwd = dbdict.get('password')
            db = dbdict.get('db')
            self.conn = MySQLdb.Connection(host, user, pwd, db)
            self.account_extra = {} 
            # key is accountId, value [accountId,accountName,info,form,type,os,version,ip,serverId,time]
            self.role_extra = {} 
            # key is roleId, value [accountId,roleName,roleLevel,roleCareer,accountName,info,form,type,os,version,ip]
            self.formatStr = {} #sql style
            self.sqls = {} # sql
            self.commitMax = 10 #commit size
            self.currCommit = 0
            self.currRead = 0 # one time read count
            self.readMax = 100000
            self.fillList = ['role_offline', 'role_diamond_cost']
            
    
    def init_path(self, inputPath, outputPath):
        self.input_file = open(inputPath, 'r')
        self.out_file = open(outputPath, 'w')
        
    def do_read_file(self):
        lineStr = self.input_file.readline()
        while True:
            if not lineStr:
                break;
            line = lineStr.strip('\n')
            list_arrays = line.split('|')
            if('account_extra' == list_arrays[1]):
                self.account_extra[list_arrays[2]] = list_arrays #list_arrays[2] is accountId
            else:
                self.set_infos(list_arrays[1], list_arrays) # list_arrays[1] is tableName
                self.replace_infos(list_arrays[1], list_arrays) #list_arrays[1] is tableName
            self.get_format_sql(list_arrays)
            if(self.currRead == 0 and self.currCommit == 0):
                self.out_file.write('set autocommit = 0;\n')
            self.currRead += 1
            if (self.currRead >= self.readMax):
                self.write_sql()
                self.currRead = 0
                self.currCommit += 1
            if (self.currCommit >= self.commitMax):
                self.out_file.write('set autocommit = 1;\n')
                self.currCommit = 0
            # commit
#            if(not sql):
#                continue
            #self.out_file.write(sql)
            lineStr = self.input_file.readline()
        if(self.currRead > 0 or self.currCommit > 0):
            self.write_sql()
            self.out_file.write('set autocommit = 1;\n')
            
    def replace_infos(self, tableName, list_arrays):
        if(tableName == 'role_level_up'):
            #role_level_up|roleId|accountId|roleLevel|serverId|time
            role_infos = self.role_extra.get(list_arrays[2]) # list_arrays[2] is roleId
            #role_infos can't be null
            role_infos[3] = list_arrays[4]
        elif(tableName == 'role_name_change'):
            #role_name_change|roleId|accountId|roleName|serverId|time
            role_infos = self.role_extra.get(list_arrays[2]) # list_arrays[2] is roleId
            role_infos[2] = list_arrays[4]
      
    def set_infos(self, tableName, list_arrays):
        if (tableName == 'role_login' or tableName == 'role_register'):# set login elements to role_infos
#            account_infos = self.accout_extra[list_arrays[3]]
            if(not self.account_extra.has_key(list_arrays[3])):
                query_sql = "select * from account_extra where accountId=%s order by createdAt desc limit 0,1"
                cur = self.conn.cursor()
                queryNum = cur.execute(query_sql, list_arrays[3])
                if(not queryNum):
                    self.account_extra[list_arrays[3]] = [list_arrays[3], '', '', 0, 0, '', '', '', 0, 0]
                else:
                    list_result = list(cur.fetchone())
                    self.account_extra[list_arrays[3]] = list_result
                cur.close()
            # make role_extra list
            if(tableName == 'role_login'):
                list_arrays.extend(self.account_extra[list_arrays[3]])
                del list_arrays[len(list_arrays) - 10] #del account_extra accountId
                del list_arrays[len(list_arrays) -1] # del account_extra serverId, time
                del list_arrays[len(list_arrays) - 1]
                self.role_extra[list_arrays[2]] = list_arrays
                
            #[accountId,accountName,info,form,type,os,version,ip,serverId,time]
            #[roleId,accountId,roleName,roleLevel,roleCareer,serverId,time,accountName,info,form,type,os,version,ip]
        elif(not self.role_extra.has_key(list_arrays[2]) and (tableName in self.fillList or tableName == 'role_level_up' or tableName == 'role_name_change')):
            query_sql = 'select * from role_login where roleId=%s order by createdAt desc limit 0,1'
            cur = self.conn.cursor()
            print list_arrays[2]
            queryNum = cur.execute(query_sql, list_arrays[2])
            if(not queryNum):
                self.role_extra[list_arrays[2]] = [list_arrays[2], 0, '', 0, 0, 0, 0, '', '', 0, 0, '', '', '']
            else:
                list_result = list(cur.fetchone())
                self.role_extra[list_arrays[2]] = list_result
            cur.close()
        
    def get_format_sql(self, list_arrays):
        tableName = list_arrays[1]
        del list_arrays[0:2] #delete logTime tablename
        if(tableName == 'role_register'):
           account_arrays = self.account_extra.get(list_arrays[1]) 
           list_arrays.extend(account_arrays)
           del list_arrays[len(list_arrays) - 10] #del account_extra accountId
           del list_arrays[len(list_arrays) - 1] # del account_extra serverId, time
           del list_arrays[len(list_arrays) - 1]
        elif(tableName in self.fillList):
            info_arrays = self.role_extra.get(list_arrays[0])
            list_arrays.extend(info_arrays)
            del list_arrays[len(list_arrays) - 14] # delete info roleId
            del list_arrays[len(list_arrays) - 9] # del info serverId, time
            del list_arrays[len(list_arrays) - 8]
        self.set_format_style(tableName, list_arrays) # set sql style
        self.add_sql_element(tableName, list_arrays)  # add sql elements
    
    def set_format_style(self, tableName, list): #set sql style
        if (not self.formatStr.has_key(tableName)):
            style_list ='(%s)' %(','.join(['\'%s\'']*len(list))) #[(%s,%s,%s...)]
            self.formatStr[tableName] = style_list
            
    def add_sql_element(self, tableName, list): #add sql elements
        if (not self.sqls.has_key(tableName)):
            self.sqls[tableName] = list
        else:
            allElements = self.sqls[tableName]
            allElements.extend(list)
    def write_sql(self):
        insertSql = 'INSERT INTO %s VALUES %s\n;'
        for (key, value) in self.sqls.items():
            style_list = self.formatStr[key]
            charSCount = style_list.count('%s')
            elemCount = len(value)
            sql_format = insertSql %(key, ','.join([style_list]*(elemCount/charSCount)))
            sql = sql_format % tuple(value)
            self.out_file.write(sql)
            del self.sqls[key]
    def close(self):
        self.input_file.close()
        self.out_file.close()
        self.conn.close()

if __name__ == "__main__":
    myClass = Reader()
    myClass.init_path(sys.argv[1], sys.argv[2])
    myClass.do_read_file()
    myClass.close()

