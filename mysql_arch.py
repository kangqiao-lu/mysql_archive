#!/usr/bin/python
#coding=utf-8
import MySQLdb,sys,time,string,socket,processing,logging,os

class MySQLHandler(object):
    def __init__(self,host,port,user):
        self.host = host
        self.port = int(port)
        self.user = user
        if self.user == 'dba_monitor':
            self._pw = 'XXXXX'
        elif self.user == 'dbbak':
            self._pw = 'XXXXXX'
        elif self.user == 'dbwebm':
            self._pw = 'XXXXXXXX'
        _failed_times = 0
        while True:
            try:
                self.con_db = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self._pw)
                self.con_db.autocommit(1)
                self.cursor = self.con_db.cursor()
            except:
                _failed_times += 1
                if _failed_times >= 3:
                    raise 
                else:
                    continue
            break

    def reconnect(self):
        _failed_times = 0
        while True:
            try:
                self.con_db = MySQLdb.connect(host=self.host,port=self.port,user=self.user,passwd=self._pw)
                self.con_db.autocommit(1)
                self.cursor = self.con_db.cursor()
            except:
                _failed_times += 1
                if _failed_times >= 3:
                            raise 
                else:
                    continue
            break
    
    def get_mysql_data(self,sql):
        try:
            self.cursor.execute(sql)
            sql_data = self.cursor.fetchall()
            return sql_data
        except MySQLdb.OperationalError as e:
            if 2006 == e.args[0]:
                self.reconnect()
                try:
                    self.cursor.execute(sql)
                    sql_data = self.cursor.fetchall()
                    return sql_data
                except MySQLdb.Error as e1:
                    print e1.args[1]
                    return 0
            else:
                print e.args[1]
                return 0
        except MySQLdb.Error as e2:
            print e2.args[1]
            return 0

    def execute_sql(self,sql):
        try:
            self.cursor.execute(sql)
            return 1
        except  MySQLdb.OperationalError as e:
            if 2006 == e.args[0]:
                self.reconnect()
                try:
                    self.cursor.execute(sql)
                    return 1
                except MySQLdb.Error as e1:
                    print e1.args[1]
                    return 0
            else:
                print e.args[1]
                return 0
        except  MySQLdb.Error as e2:
            print e2.args[1]
            return 0

class WriteLog(object):
    def write(self,log_lev,log_msg):
        logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %X',
                filename='/data/mysql/opbin/tb_arch/logs/tb_arch.log',
                filemode='a')
        if log_lev == 'd':
            logging.debug(log_msg)
        elif log_lev == 'i':
            logging.info(log_msg)
        elif log_lev == 'w':
            logging.warning(log_msg)
        elif log_lev == 'e':
            logging.error(log_msg)
        elif log_lev == 'c':
            logging.critical(log_msg)

class NewArch(object):
    def __init__(self,id,db,tb,port,clause,mysql_class):
        self.id = id
        self.db = db
        self.tb = tb
        self.port = port
        self.clause = clause
        self.mysql_class = mysql_class
        self.loger = WriteLog()
        try:
            self.logdb = MySQLHandler('log-ku-m00',3306,'dba_monitor')
            self.archdb = MySQLHandler('127.0.0.1',self.port,'dbbak')
        except:
            print "Connect MySQL has some problem...."
            self.loger.write('c','Connect MySQL has some problem in NewArch.__init__()')
            exit()
  
    #获取归档表的主键
    def get_tb_pk(self):
        get_pk_sql = "select CONSTRAINT_NAME,COLUMN_NAME,ORDINAL_POSITION from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_SCHEMA='%s' and TABLE_NAME='%s' and CONSTRAINT_NAME='PRIMARY' order by ORDINAL_POSITION;" % (self.db,self.tb)
        all_pk_data = self.archdb.get_mysql_data(get_pk_sql)
        if all_pk_data == 0:
            print 'mysql has some problem in get_tb_pk %s.%s' % (self.db,self.tb)
            log_msg = 'mysql has some problem in get_tb_pk %s.%s' % (self.db,self.tb)
            self.loger.write('e',log_msg)
            return 0
        #没有主键,判断有没有唯一索引,有的话就返回唯一索引,例如返回'id' or 'id,name' or 'id,sno,name'
        elif all_pk_data == ():
            get_unique_sql = "select CONSTRAINT_NAME,COLUMN_NAME,ORDINAL_POSITION from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_SCHEMA='%s' and TABLE_NAME='%s' order by ORDINAL_POSITION;" % (self.db,self.tb)
            all_unique_data = self.archdb.get_mysql_data(get_unique_sql)
            if all_unique_data == 0:
                print 'mysql has some proble in get_tb_pk %s.%s' % (self.db,self.tb)
                log_msg = 'mysql has some problem in get_tb_pk %s.%s' % (self.db,self.tb)
                self.loger.write('e',log_msg)
                return 0
            elif all_unique_data == ():
                return ''
            else:
                tmp_pks = ''
                for unique_data in all_unique_data:
                    pk = unique_data[1]
                    tmp_pks = tmp_pks + pk + ','
                pks = tmp_pks[:-1]
                return pks    
        else:
            tmp_pks = ''
            for pk_data in all_pk_data:
                pk = pk_data[1]
                tmp_pks = tmp_pks + pk + ','
            pks = tmp_pks[:-1]
            return pks

    #获取归档表的主键值,后面还有函数根据这个主键值删除归档数据
    def get_arch_id(self):
        pks = self.get_tb_pk()
        #没获取到主键或者没主键也没有唯一索引,按归档条件删除数据
        if pks == 0 or pks == '':
            print 'There is no primary key or unique key... %s.%s' % (self.db,self.tb)
            log_msg = 'There is no primary key or unique key... %s.%s' % (self.db,self.tb)
            self.loger.write('w',log_msg)
            return 0
        else:
            sql = 'select %s from %s.%s where %s;' %  (pks,self.db,self.tb,self.clause)
            all_data = self.archdb.get_mysql_data(sql)
            ids = []
            if all_data == 0:
                print "Can not connect arch db while get arch id %s.%s" % (self.db,self.tb)
                log_msg = "Can not connect arch db while get arch id %s.%s" % (self.db,self.tb)
                self.loger.write('e',log_msg)
                return ids
            else:
                for data in all_data:
                    ids.append(data)
            #ids looks like [(1L,), (2L,), (3L,), (4L,)] or [(1L, 'iverson'), (2L, 'lbj'), (3L, 't-mac'), (4L, 'kobe')]
                return ids

    #返回key_name
    def get_key_name(self):
        pks = self.get_tb_pk()
        if pks == 0 or pks == '':
            print 'There is no primary key or unique key... %s.%s' % (self.db,self.tb)
            log_msg = 'There is no primary key or unique key... %s.%s' % (self.db,self.tb)
            self.loger.write('w',log_msg)
            key_name = '%s.%s.%s.NOPK.%s' % (self.id,self.db,self.tb,self.clause)
            return key_name
        else:
            key_name = '%s.%s.%s.%s.%s' % (self.id,self.db,self.tb,pks,self.clause)
            return key_name

    def get_arch_id_del(self):
        ids = []
        get_id_sql = 'select id from %s.%s where %s;' % (self.db,self.tb,self.clause)
        id_data = self.archdb.get_mysql_data(get_id_sql)
        if id_data == 0:
            print "Can not connect arch db while get arch id %s.%s" % (self.db,self.tb)
            log_msg = "Can not connect arch db while get arch id %s.%s" % (self.db,self.tb)
            self.loger.write('e',log_msg)
            return ids
        else:
            for i in id_data:
                ids.append(i[0])
            return ids

    ##记录当天的表结构
    def get_desc_tb(self):
        now_time = time.strftime('%Y-%m-%d',time.localtime())
        desc_file = '/data/mysql/opbin/tb_arch/data_bak/desc_%s.%s_%s' % (self.db,self.tb,now_time)
        get_desc_sql = 'desc %s.%s;' % (self.db,self.tb)
        all_desc_data = self.archdb.get_mysql_data(get_desc_sql)
        if all_desc_data != 0:
            df = open(desc_file,'a')
            all_col_list = []
            for col in all_desc_data:
                all_col_list.append(col[0])
                all_col_tuple = tuple(all_col_list)
            df.write(str(all_col_tuple) + '\n')
            df.close()
        else:
            print "write arch tb desc failed! %s.%s" % (self.db,self.tb)
            log_msg = "write arch tb desc failed! %s.%s" % (self.db,self.tb)
            self.loger.write('w',log_msg)
    
    def update_arch_status(self,arch_lines,arch_status):
        now_date = time.strftime('%Y-%m-%d',time.localtime()) 
        update_sql = "update dba_stats.arch_conf set last_arch_date='%s',last_arch_status=%s,last_arch_lines=%s where class='%s' and db_name='%s' and tb_name='%s';" % (now_date,arch_status,arch_lines,self.mysql_class,self.db,self.tb)
        self.logdb.execute_sql(update_sql)

    def arch_to_disk(self):
        ids = []
        now_time = time.strftime('%Y-%m-%d',time.localtime())
        out_file = '/data/mysql/opbin/tb_arch/data_bak/%s.%s.%s.%s.csv' % (self.id,self.db,self.tb,now_time)
        count_arch_sql = "select count(*) from %s.%s where %s;" % (self.db,self.tb,self.clause)
        count_data = self.archdb.get_mysql_data(count_arch_sql)
        if count_data == 0:
            print "Can not connect arch db while arch %s.%s" % (self.db,self.tb)
            log_msg = "Can not connect arch db while arch %s.%s" % (self.db,self.tb)
            self.loger.write('e',log_msg)
            return ids
        else:
            count = count_data[0][0]
            if count == 0:
                print "This no data need to arch %s.%s" % (self.db,self.tb)
                log_msg = "This no data need to arch %s.%s" % (self.db,self.tb)
                self.loger.write('i',log_msg)
                return ids
            else:
                self.get_desc_tb()
                ids = self.get_arch_id()
                if ids == []:
                    return ids
                else:
                    ##归档采用 into outfile形式,用'YangHaoYong'号隔开每个字段
                    arch_sql = "select * from %s.%s where %s into outfile '%s' FIELDS TERMINATED BY 'YangHaoYong';" % (self.db,self.tb,self.clause,out_file)
                    ##判断归档是否失败,失败了重试三次,如果还失败,就跳过归档
                    _failed_times = 0
                    while _failed_times <= 3:
                        arch_status = self.archdb.execute_sql(arch_sql)
                        if arch_status == 0:
                            _failed_times += 1
                            self.update_arch_status(0,0)
                            log_msg =  "%s.%s arch to disk failed" % (self.db,self.tb)
                            print log_msg
                            self.loger.write('e',log_msg)
                            _tmp_ids= []
                            continue
                        else:
                            self.update_arch_status(count,1)
                            _tmp_ids = ids
                            _failed_times = 4
                            log_msg =  "%s.%s arch to disk success" % (self.db,self.tb)
                            print log_msg
                            self.loger.write('i',log_msg)
                            
                    ids = _tmp_ids
                    return ids
                
 
    def arch_to_db(self):
        ids = self.arch_to_disk()
        if ids == []:
            log_msg = "ids is [] in arch_to_db,arch to db faild! %s.%s" % (self.db,self.tb)
            self.loger.write('e',log_msg)
            return ids
        else:
            sql = "select go_class,go_db,go_tb from dba_stats.arch_conf where class='%s' and db_name ='%s' and tb_name = '%s';" % (self.mysql_class,self.db,self.tb)
            all_goto_data = self.logdb.get_mysql_data(sql)
            if all_goto_data == 0 or all_goto_data == ():
                log_msg = "Cannot get sql_data in arch_to_db %s" % sql
                self.loger.write('e',log_msg)
                return ids
            else:
                go_class,go_db,go_tb = all_goto_data[0]
                sql = "select host,port from dba_stats.monitor_conf where class='%s' and is_master=1;" % go_class
                all_sql_data = self.logdb.get_mysql_data(sql)
                if all_sql_data == 0 or all_sql_data == ():
                    log_msg = "Cannot get mysql data in arch_to_db %s" % sql
                    self.loger.write('e',log_msg)
                    return ids
                else:
                    go_host,go_port = all_sql_data[0]
                    try:
                        godb = MySQLHandler(go_host,go_port,'dbwebm')
                    except:
                        log_msg = "Cannot connect to godb %s:%s" % (go_host,go_port)
                        self.loger.write('e',log_msg)
                        return ids
                    now_time = time.strftime('%Y-%m-%d',time.localtime())
                    arch_file = '/data/mysql/opbin/tb_arch/data_bak/%s.%s.%s.%s.csv' % (self.id,self.db,self.tb,now_time)
                    if os.path.isfile(arch_file):
                        sql = "load data local infile '%s' IGNORE into table %s.%s FIELDS TERMINATED BY 'YangHaoYong';" % (arch_file,go_db,go_tb)
                        arch_status = godb.execute_sql(sql)
                        if arch_status == 0:
                            log_msg = "%s.%s arch to db failed..." % (self.db,self.tb)
                            print log_msg
                            self.loger.write('e',log_msg)
                        else:
                            log_msg = "%s.%s arch to db success..." % (self.db,self.tb)
                            print log_msg
                            self.loger.write('i',log_msg)
                        return ids
                    else:
                        log_msg = "these is no arch file %s in arch_to_db" % arch_file
                        self.loger.write('w',log_msg)
                        return ids
    
    def arch_to_moosefs(self):
        pass
    
    def excute_arch_command(self):
        sql = self.clause
        exe_stats = self.archdb.execute_sql(sql)
        if exe_stats == 0:
            self.update_arch_status(0,0)
            log_msg = '%s.%s execute arch command %s failed...' % (self.db,self.tb,sql)
            print log_msg
            self.loger.write('e',log_msg)
        else:
            self.update_arch_status(0,1)
            log_msg = '%s.%s execute arch command %s success...' % (self.db,self.tb,sql)
            print log_msg
            self.loger.write('i',log_msg)


class DeleteArchData(object):

    def __init__(self,mysql_class,port,id_list,arch_id,db_name,tb_name,pks,arch_clause):
        self.mysql_class = mysql_class
        self.port = port
        self.id_list = id_list
        self.arch_id = arch_id
        self.db_name = db_name
        self.tb_name = tb_name
        self.pks = pks
        self.arch_clause = arch_clause
        self.loger = WriteLog()
        self.new_arch = NewArch(self.arch_id,self.db_name,self.tb_name,self.port,self.arch_clause,self.mysql_class)
        try:
            self.archdb = MySQLHandler('127.0.0.1',self.port,'dbbak')
        except:
            print "Connect MySQL has some problem.... in DeleteArchData.__init__()"
            exit()
          
    #按归档条件删除 
    def delete_arch_clause(self):
        count_sql = "select count(*) from %s.%s where %s;" % (self.db_name,self.tb_name,self.arch_clause)
        count_data = self.archdb.get_mysql_data(count_sql)
        if count_data == 0:
            print 'Can not get count_data in delete_arch_clause %s.%s ....' % (self.db_name,self.tb_name)
            log_msg = 'Can not get count_data in delete_arch_clause %s.%s ....' % (self.db_name,self.tb_name)
            self.loger.write('e',log_msg)
        else:
            count = count_data[0][0]
            if count == 0:
                print 'There is no data need to delete in delete_arch_clause %s.%s ....' % (self.db_name,self.tb_name)
                log_msg = 'There is no data need to delete in delete_arch_clause %s.%s ....' % (self.db_name,self.tb_name)
                self.loger.write('i',log_msg)
                return
            elif count >0 and count <= 1000:
                sql = "delete from %s.%s where %s;" % (self.db_name,self.tb_name,self.arch_clause)
                self.archdb.execute_sql(sql)
                log_msg = 'all arch data in %s.%s has been deleted' %  (self.db_name,self.tb_name)
                print log_msg
                self.loger.write('i',log_msg)
            else:
                exec_cnt = count/200 + 1
                sql = "delete from %s.%s where %s limit 200;" % (self.db_name,self.tb_name,self.arch_clause)
                for i in xrange(exec_cnt):
                    now_hour = int(time.strftime('%H',time.localtime()))
                    if now_hour >= 6:
                        time.sleep(1)
                        self.archdb.execute_sql(sql)
                    else:
                        self.archdb.execute_sql(sql)
                log_msg = 'all arch data in %s.%s has been deleted' %  (self.db_name,self.tb_name)
                print log_msg
                self.loger.write('i',log_msg)
    
    def delete_arch_data(self):
        #没有主键或者唯一索引,按归档条件删除
        if self.pks == 'NOPK':
            self.delete_arch_clause()
        else:
            i = 0
            for ids in self.id_list:
                nu = 0
                tmp_delete_clause = ''
                for pk in string.split(self.pks,','):
                    tmp_str = "%s='%s' and " % (pk,ids[nu])
                    tmp_delete_clause = tmp_delete_clause + tmp_str
                    nu += 1
                delete_clause = tmp_delete_clause[:-5]
                sql = "delete from %s.%s where %s;" % (self.db_name,self.tb_name,delete_clause)
                now_hour = int(time.strftime('%H',time.localtime()))
                i += 1
                n = i%10000
                if now_hour >= 6 and n == 0:
                    time.sleep(1)
                    self.archdb.execute_sql(sql)
                else:
                    self.archdb.execute_sql(sql)
            log_msg = 'all arch data in %s.%s has been deleted' %  (self.db_name,self.tb_name)
            print log_msg
            self.loger.write('i',log_msg)

#检查本机有几个产品线,是否为主库
def whoami():
    loger=WriteLog()
    hostname = socket.gethostname()
    try:
        logdb = MySQLHandler('log-ku-m00',3306,'dba_monitor')
    except:
        print "Can not connect log mysql in whoami"
        loger.write('e','Can not connect log mysql in whoami')
        return 0
    
    sql = "select class,port,is_master from dba_stats.monitor_conf where realserver='%s';" % hostname
    sql_data = logdb.get_mysql_data(sql)
    if sql_data == 0:
        print 'Can not get log mysql data in whoami'
        loger.write('e','Can not get log mysql data in whoami')
        return 0
    elif sql_data == ():
        print 'This no mysql at this host'
        loger.write('w','This no mysql at this host')
        return 0
    else:
        ret = []
        for mysql_ints in sql_data:
            mysql_class,port,is_master = mysql_ints
            try:
                db = MySQLHandler('127.0.0.1',int(port),'dbbak')
            except:
                print 'Can not connect local %s mysql in whoami' % mysql_class
                log_msg = 'Can not connect local %s mysql in whoami' % mysql_class
                loger.write('e',log_msg)
                continue
            all_data = db.get_mysql_data('select @@read_only;')
            if all_data == 0:
                continue
            else:
                read_only = all_data[0][0]
                if read_only == 0 and is_master == 1:
                    ints = [mysql_class,int(port)]
                    ret.append(ints)
        return ret

def start_arch(mysql_class,port):
    loger = WriteLog()
    all_ids = {}
    try:
        logdb = MySQLHandler('log-ku-m00',3306,'dba_monitor')
    except:
        print "connect log mysql has some problem in start_arch...."
        loger.write('e','connect log mysql has some problem in start_arch....')
        exit()
    sql = "select id,db_name,tb_name,arch_type,goto,clause from dba_stats.arch_conf where class='%s';" % mysql_class
    all_arch_tb = logdb.get_mysql_data(sql)
    if all_arch_tb == 0:
        print "Cannot get arch tb in start_arch..."
        loger.write('e','Cannot get arch tb in start_arch...')
        exit()
    elif all_arch_tb == ():
        print "There is no tb needs to arch in start_arch...."
        loger.write('i','There is no tb needs to arch in start_arch....')
        exit()
    
    for arch_tb in all_arch_tb:
        id,db,tb,arch_type,goto,clause = arch_tb
        new_arch = NewArch(id,db,tb,port,clause,mysql_class)
        key_name = new_arch.get_key_name()
        if arch_type == 1:
            new_arch.excute_arch_command()
        else:
            #arch to disk
            if goto == 0:
                ids = new_arch.arch_to_disk()
                all_ids[key_name] = ids
            #arch to db
            elif goto == 1:
                ids = new_arch.arch_to_db()
                all_ids[key_name] = ids
            #arch to moosefs
            elif goto == 2:
                ids = new_arch.arch_to_moosefs()
                all_ids[key_name] = ids
    
    for db_tb in all_ids:
        row_id,db,tb,pks,arch_clause = string.split(db_tb,'.')
        ids = all_ids[db_tb]
        delete_data = DeleteArchData(mysql_class,port,ids,row_id,db,tb,pks,arch_clause)
        delete_data.delete_arch_data()

if __name__ == '__main__':
    loger = WriteLog()
    all_arch_db = whoami()
    if all_arch_db == 0 or all_arch_db == []:
        print 'This no arch job in this host'
        loger.write('i','This no arch job in this host')
    else:
        for arch_db in all_arch_db:
            mysql_class,port = arch_db
            arch_process = processing.Process(target=start_arch,args=[mysql_class,port])
            arch_process.start()
