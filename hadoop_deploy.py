"""configure and manage (start/stop) hadoop cluster.

1. The NameNode and DataNodes must be passwordless ssh accessable
2.   
"""

import os
import re
import subprocess
import multiprocessing
import time
import requests
import logging
import json
import socket
import getpass

import fabric.api
#from fabric.api import env,parallel




USER=getpass.getuser()
        
#Master Node's hostname
HOSTNAME=socket.gethostname()

#Master Node's user's home path
HOME=os.path.expanduser("~")

IP = socket.gethostbyname(socket.gethostname())

WORKING_DIR = os.getcwd()

HADOOP_HOME = "/home/hadoop/tools/hadoop-2.7.2" #os.getenv("HADOOP_HOME")
HADOOP_USER = USER
HADOOP_JOB_FOLDER_NAME = 'hadoop_jobs'



CONTAINERS_PER_NODE = 1

HADOOP_HADOOP_BIN_PATH = os.path.join(HADOOP_HOME,'bin')
HADOOP_HADOOP_EXEC = os.path.join(HADOOP_HADOOP_BIN_PATH,'hadoop')
HADOOP_HDFS_EXEC = os.path.join(HADOOP_HADOOP_BIN_PATH,'hdfs')

HADOOP_HDFS_PATH = "%s/hdfs" % HOME
HADOOP_CONF_PATH = os.path.join(HADOOP_HOME, 'etc/hadoop')
HADOOP_JOB_HOME = os.path.join('/user', HADOOP_USER, HADOOP_JOB_FOLDER_NAME)

HADOOP_VERSION = "2.7.2"
    
if not HADOOP_HOME:
    hadoop_paths = glob.glob('%s/hadoop-*' % HOME)
    if hadoop_paths:
        HADOOP_HOME = os.path.abspath(hadoop_paths[-1]) #get the latest version 
    
if not HADOOP_HOME: #download hadoop binary distribution
    HADOOP_DOWNLOAD_URL = "http://apache.cs.utah.edu/hadoop/common/hadoop-%s/hadoop-%s.tar.gz" % (HADOOP_VERSION)
    context.log_debug("Download hadoop from %s" % HADOOP_DOWNLOAD_URL)            
    subprocess.check_call('curl -L %s | tar -zx -C .' % HADOOP_DOWNLOAD_URL)
    HADOOP_HOME = os.path.join(HOME, 'hadoop-%s' % HADOOP_VERSION)

#context.log_debug("Hadoop home: %s" % HADOOP_HOME)            

HADOOP_HADOOP_BIN_PATH = os.path.join(HADOOP_HOME, 'bin')
HADOOP_HADOOP_EXEC = os.path.join(HADOOP_HADOOP_BIN_PATH, 'hadoop')
HADOOP_HDFS_EXEC = os.path.join(HADOOP_HADOOP_BIN_PATH, 'hdfs')

HADOOP_HDFS_PATH = "%s/hdfs" % HOME
HADOOP_CONF_PATH = os.path.join(HADOOP_HOME, 'etc/hadoop')
HADOOP_JOB_HOME = os.path.join('/user',HADOOP_USER,HADOOP_JOB_FOLDER_NAME)


JAVA_HOME = os.getenv("JAVA_HOME")
if not JAVA_HOME:
    try:
        JAVA_HOME=subprocess.check_output('readlink -f /usr/bin/java | sed "s:/jre/bin/java::"',shell=True).strip()
    except Exception,e:
        print 'Could not locate the Java Development Kit'

if not JAVA_HOME:
    try:
        subprocess.check_call('sudo apt-get install -y openjdk-7-jdk')
        JAVA_HOME=subprocess.check_output('readlink -f /usr/bin/java | sed "s:/jre/bin/java::"',shell=True).strip()
    except:
        print 'Could not install the Java Development Kit'

def run_local(cmd,**kwargs):
    """run a command on Master Node
    """
    #log_debug(cmd)
    with fabric.api.settings(fabric.api.quiet()):
        result = fabric.api.local(cmd,capture=True)
        if result.succeeded:
            return result.stdout
        else:
            #log_error('Failed: %s' % cmd)
            raise Exception(result.stderr)

def run_execute(cmd,hosts):
    """run a command on every server in "hosts"
    """
    #log_debug(cmd)
    fabric.api.env.timeout=3
    fabric.api.env.keepalive=1
    fabric.api.env.connection_attempts=20
    with fabric.api.settings(fabric.api.quiet()):
        return fabric.api.execute(cmd,hosts=hosts)

def run_remote(cmd,**kwargs):
    """run a command on a remote server which has been specified before calling this function
    """
    #log_debug(cmd,logging.DEBUG)
    fabric.api.env.timeout=3
    fabric.api.env.keepalive=1
    fabric.api.env.connection_attempts=20
    with fabric.api.settings(fabric.api.quiet()):
        result = fabric.api.run(cmd,**kwargs)
        if result.succeeded:
            return result.stdout
        else:
            #log_error('%s failed -  %s' % (cmd,result.stderr))
            raise Exception(result.stderr)
        
def run_sudo(cmd):
    """run a command as sudo on Master Node
    """
    #log_debug(cmd,logging.DEBUG)
    fabric.api.env.timeout=3
    fabric.api.env.keepalive=1
    fabric.api.env.connection_attempts=20
    with fabric.api.settings(fabric.api.quiet()):
        result = fabric.api.sudo(cmd)
        if result.succeeded:
            return result.stdout
        else:
            #log_error('%s failed -  %s' % (cmd,result.stderr))
            raise Exception(result.stderr)


class Hadoop:
    def __init__(self, slaves=["n1", "n2", "n3", "n4"]):
        self.slaves = slaves
    
    def get_datanode_memory_mb(self):
        """
        get the total memory (MB) of a DataNode. 
        """
        try:
            
            fabric.api.env.host_string = self.slaves[0]
            fabric.api.env.user = USER
            print fabric.api.env.host_string, fabric.api.env.user
            pattern = re.compile("MemTotal:\\s+(\\d+)\\s*kB");
            memory_kb = pattern.findall(run_remote("cat /proc/meminfo"))
            if memory_kb:
                return int(memory_kb[0])/1024
        except Exception, e:
            print e
            print 'Unable to determine DataNode memory capacity'
            return 8192*0.9
            
    def get_datanode_cpu_count(self):
        """
        get the total CPU cores of a DataNode. 
        """
        try:
            fabric.api.env.host_string = self.slaves[0]
            fabric.api.env.user = USER
            return run_remote("grep -c ^processor /proc/cpuinfo")
        except:
            return 1
    
    
    def setup_passwordless_connection(self,user_passwd_or_keyfile):
        """set up passwordless SSH connection between NameNode and DataNodes
        """
        #s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #s.connect(('8.8.8.8', 0))
        #master_ip = s.getsockname()[0]
    
        master_ip = run_local('hostname --ip-address')
        master_hostname = '%s\\t%s' % (master_ip, socket.gethostname())
        NoHost="Host *\n\tStrictHostKeyChecking no\n\tUserKnownHostsFile /dev/null\n\tLogLevel quiet\n"
    
        ssh_dir = '%s/.ssh' % HOME
        ssh_config= os.path.join(ssh_dir, 'config')
        private_RSA_key= os.path.join(ssh_dir, 'id_rsa')
        public_RSA_key = os.path.join(ssh_dir, 'id_rsa.pub')
        authorized_keys = os.path.join(ssh_dir, 'authorized_keys')

        cmd_keygen="echo 'y\\n' | ssh-keygen -t rsa -q -N '' -f %s" % private_RSA_key
    
        run_local('rm -fr .ssh/*')
        run_local('echo "%s" >> %s' % (NoHost, ssh_config))
        #if not os.path.exists(private_RSA_key):
        run_local(cmd_keygen)
        run_local('cat %s >> %s/.ssh/authorized_keys' % (public_RSA_key, HOME))


        fabric.api.env.timeout = 30
        fabric.api.env.hosts = ['%s@%s' % (self.user,d) for d in self.datanodes]
        
        if os.path.exists(os.path.abspath(user_passwd_or_keyfile)):
            fabric.api.env.key_filename = user_passwd_or_keyfile
        else:
            fabric.api.env.password = user_passwd_or_keyfile
        
        hosts_file  = '/etc/hosts'
        run_local('''sudo bash -c "echo -e '%s' > %s" ''' % (master_hostname, hosts_file))
        pool_size=len(fabric.api.env.hosts)
    
        @fabric.api.parallel(pool_size=pool_size)
        def exchange_credentials():
            """TODO: use 'ssh-copy-id'? 
            """
            context.run_remote('rm -fr .ssh/*')
            context.run_remote(cmd_keygen)
            #config
            context.run_remote('echo "%s" >> %s' % (NoHost,ssh_config))
            
            #add Master's public key to Slave's authorized_keys, now the master can access this node without password
            context.run_remote('echo "%s" >> %s' % (context.run_local('cat %s' % public_RSA_key),authorized_keys))
            context.run_local('echo "%s" >> %s' % (context.run_remote('cat %s' % public_RSA_key),authorized_keys))

            hs = '%s\\t%s' % (context.run_remote("hostname -I | awk '{print $1}'"),context.run_remote('hostname'))
        
            #wirte on "/etc/hosts"
            context.run_sudo('''bash -c "echo -e '%s\\n%s' > %s" ''' % (master_hostname,hs,hosts_file))
        
            #append to "master@/etc/hosts"
            context.run_local('''sudo bash -c "echo -e '%s' >> %s" ''' % (hs,hosts_file))

        context.run_execute(exchange_credentials,fabric.api.env.hosts)
        
    def configure(self):
        
        #node_total_memory_mb= physical+virtual memory
        node_total_memory_mb=int(self.get_datanode_memory_mb() * 0.9) * 2 #not 1024, because we need leave some memory to the host
        
        print 'DataNodes memory MB: %d' % node_total_memory_mb
        
        #num_cpu_per_container = self.cluster.datanode_cpu_count if self.cluster.datanode_cpu_count >=1 else 1
        
        memory_mb_per_container = node_total_memory_mb / CONTAINERS_PER_NODE
        
        mapreduce_map_memory_mb = memory_mb_per_container
        
        mapreduce_reduce_memory_mb = memory_mb_per_container
        
        mapreduce_map_java_memory_mb = memory_mb_per_container
         
        mapreduce_reduce_java_memory_mb = memory_mb_per_container
             
             
        head='\n'.join(['<?xml version="1.0" encoding="UTF-8"?>',
                '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>',
                '<configuration>'])
        
        tail = '</configuration>'
        
        add_property = "<property><name>{name}</name><value>{value}</value></property>".format
        
        #support for oozie
        #<property><name>hadoop.proxyuser.<username>.hosts</name><value>*</value></property>
        #<property><name>hadoop.proxyuser.hadoop.groups</name><value>*</value></property>
        
        
        core_site_conf={'fs.defaultFS':'hdfs://%s:8020'% IP,
                             'hadoop.tmp.dir':'%s/tmp' % HADOOP_HDFS_PATH,
                             'hadoop.proxyuser.hadoop.hosts' : '*',
                             'hadoop.proxyuser.hadoop.groups' : '*'
                             }

        hdfs_site_conf={'dfs.namenode.name.dir':'%s/name' % HADOOP_HDFS_PATH,
                             'dfs.datanode.data.dir':'%s/data' % HADOOP_HDFS_PATH,
                             'dfs.namenode.http-address':'%s:50070'% IP,
                             'dfs.namenode.https-address':'%s:50470'% IP,
                             'dfs.namenode.secondary.http-address':'%s:50090'% IP,
                             'dfs.blocksize':'64m',
                             'dfs.replication':'1',
                             }

        mapred_site_conf={'mapreduce.framework.name':'yarn',
                               'mapreduce.cluster.temp.dir':'%s/mapreduce/cluster/temp' % HADOOP_HDFS_PATH,
                               'mapreduce.cluster.local.dir':'%s/mapreduce/cluster/local' % HADOOP_HDFS_PATH,
                               'mapreduce.task.timeout':'259200000',
                               'mapreduce.map.memory.mb':'%d' % mapreduce_map_memory_mb,
                               'mapreduce.reduce.memory.mb':'%d' % mapreduce_reduce_memory_mb,
                               'mapreduce.map.java.opts':'-Xmx%dm' % mapreduce_map_java_memory_mb,
                               'mapreduce.reduce.java.opts':'-Xmx%dm' % mapreduce_reduce_java_memory_mb,
                               'mapreduce.jobhistory.address':'%s:%d' % (IP,10020)
        }
        #http://hortonworks.com/blog/simplifying-user-logs-management-and-access-in-yarn/
        #!!!enable log aggregation!!!
        yarn_site_conf={'yarn.log-aggregation-enable':'true',
                            #Determines where the container-logs are stored on the node when the containers are running
                            #Default is ${yarn.log.dir}/userlogs.
                            #'yarn.nodemanager.log-dirs':'%s/nm/env.LOG' % HADOOP_HDFS_PATH,
                            
                            #'yarn.log-aggregation.retain-seconds': 10800,
                            
                            #'yarn.log-aggregation.retain-check-interval-seconds':3,
                            #HDFS path indictes where the NMs should aggregate logs to. 
                            #'yarn.nodemanager.remote-app-log-dir':'/tmp/logs',
                            #List of directories to store localized files in. An application's localized file directory
                            
                            #'yarn.nodemanager.docker-container-executor.exec-name':self.docker_exec,
                            #'yarn.nodemanager.container-executor.class':'org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor',
                            
                            'yarn.nodemanager.local-dirs':'%s/nm/local'% HADOOP_HDFS_PATH,
                            'yarn.nodemanager.aux-services':'mapreduce_shuffle',
                            'yarn.nodemanager.aux-services.mapreduce.shuffle.class':'org.apache.hadoop.mapred.ShuffleHandler',
                            'yarn.resourcemanager.address':'%s:8032' % IP,
                            'yarn.resourcemanager.scheduler.address':'%s:8030' % IP,
                            'yarn.resourcemanager.resource-tracker.address':'%s:8031' % IP,
                            'yarn.resourcemanager.admin.address':'%s:8033' % IP,
                            'yarn.resourcemanager.webapp.address':'%s:8088' % IP,
                            'yarn.nodemanager.resource.memory-mb':'%d' % node_total_memory_mb,
                            'yarn.scheduler.minimum-allocation-mb':'%d' % memory_mb_per_container,
                            'yarn.scheduler.maximum-allocation-mb':'%d' % memory_mb_per_container
                            
        }
        
        
        core_site_xml = "%s\n%s\n%s" % (head, '\n'.join([add_property(name=k, value=v) for k,v in core_site_conf.items()]), tail)
        hdfs_site_xml = "%s\n%s\n%s" % (head, '\n'.join([add_property(name=k, value=v) for k,v in hdfs_site_conf.items()]), tail)
        mapred_site_xml = "%s\n%s\n%s" % (head, '\n'.join([add_property(name=k, value=v) for k,v in mapred_site_conf.items()]), tail)
        yarn_site_xml = "%s\n%s\n%s" % (head, '\n'.join([add_property(name=k, value=v) for k,v in yarn_site_conf.items()]), tail)
        
            
        #util.log_debug("DataNodes:"+slaves)
        cmds = []
        cmds.append("echo '%s' > %s" % (core_site_xml, "%s/core-site.xml" % HADOOP_CONF_PATH))
        cmds.append("echo '%s' > %s" % (hdfs_site_xml, "%s/hdfs-site.xml" % HADOOP_CONF_PATH))
        cmds.append("echo '%s' > %s" % (mapred_site_xml, "%s/mapred-site.xml" % HADOOP_CONF_PATH))
        cmds.append("echo '%s' > %s" % (yarn_site_xml, "%s/yarn-site.xml" % HADOOP_CONF_PATH))
        cmds.append("echo '%s' > %s" % ('\n'.join(self.slaves), "%s/slaves" % HADOOP_CONF_PATH))
        
        cmds.append('echo "export JAVA_HOME=%s">> %s/hadoop-env.sh'  % (JAVA_HOME, HADOOP_CONF_PATH))
        cmds.append('echo "export HADOOP_HOME=%s" >> %s/hadoop-env.sh'  % (HADOOP_HOME, HADOOP_CONF_PATH))
        cmds.append('echo "export HADOOP_CONF_HOME=%s" >> %s/hadoop-env.sh'  % (HADOOP_CONF_PATH,HADOOP_CONF_PATH))
        cmds.append('echo "export HADOOP_OPTS=-Djava.net.preferIPv4Stack=true" >> %s/hadoop-env.sh' % HADOOP_CONF_PATH)
        
        print 'Write configuration to NameNode'
        run_local(' && '.join(cmds))
        
        #remote_hosts = 
        
        #pool_size=len(remote_hosts)
        #@parallel(pool_size=pool_size)
        #def synchronize_hadoop():
            #util.run_remote('rsync -arvue ssh --delete %s:%s .' % (self.cluster.master_ip,HADOOP_HOME))
        ##    run_remote('scp -r %s:%s .' % (IP, HADOOP_HOME))
        #    run_remote('rm -fr %s' % HADOOP_HDFS_PATH)
            
        print 'Write configuration to DataNode'
        for h in ['%s@%s' % (USER, h) for h in self.slaves]:
            print h
            #run_local('rsync -arue ssh %s %s:%s/' % (HADOOP_HOME, h, os.path.dirname(HADOOP_HOME)))
            subprocess.check_call('rsync -arue ssh %s %s:%s/' % (HADOOP_HOME, h, os.path.dirname(HADOOP_HOME)), shell=True)
            #run_remote('rm -fr %s' % HADOOP_HDFS_PATH)
            
        #run_execute(synchronize_hadoop, remote_hosts)

        print 'Clean HDFS'
        run_local('rm -fr %s' % HADOOP_HDFS_PATH)
        
        print 'Format HDFS'
        run_local("echo -e 'Y\\n' | %s namenode -format" % os.path.join(HADOOP_HOME, 'bin/hdfs'))
    
        print 'Start HDFS'
        run_local(os.path.join(HADOOP_HOME,'sbin/start-dfs.sh'))
    
        print 'Completed'
        
        hdfs_url = "http://%s:%s/" % (IP, '50070')
        print "Visit %s for status" % hdfs_url
        #run_local(os.path.join(HADOOP_HOME,'sbin/start-yarn.sh'))
    
    
    def start(self):
        """
        Start hadoop on AWS cluster.
        """
        if self.cluster.name == 'aws':
            #check if hadoop is running
            try:
                context.run_local('jps | grep "NameNode"')
                
            except:
                self.configure()
                
    def stop(self):
        """
        Stop hadoop on AWS cluster.
        """
        if self.cluster.name == 'aws':
            try:
                context.run_local('%s/sbin/stop-dfs.sh' % HADOOP_HOME)
                context.run_local('%s/sbin/stop-yarn.sh' % HADOOP_HOME)
            except:
                pass

if __name__=='__main__':
    hadoop =  Hadoop()
    hadoop.configure()
