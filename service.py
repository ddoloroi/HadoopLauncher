#!/usr/bin/python
"""
The RESTful API exposed by the process engine.



"""

import sys
import subprocess
import binascii
import os
import time
from collections import defaultdict
from multiprocessing import Process
import pymongo
import json

import boto.ec2

from flask import Flask, jsonify, abort, make_response,request
from flask_restful import Resource, Api, reqparse, abort,fields, marshal_with
from flask_httpauth import HTTPBasicAuth



import scheduler

app=Flask(__name__)
api=Api(app)
auth = HTTPBasicAuth()

#define authenticated users
USER='clownfish'
PASSWORD='utah20zer07'
users={USER:PASSWORD}
JOB_DB = {}


"""
#start MongoDB server
mongod --fork --dbpath /home/hadoop/mongodb --logpath /home/hadoop/log/mongodb/main.log

#start client
mongo
>show dbs; #show all dbs
>use jobs; #switch to db "jobs"
>show collections; #show collections in current db
"""

@auth.get_password
def get_password(username):
    #print "get passwd"
    return users.get(username,None)

@auth.verify_password
def verify_password(username, password):
    if users.get(username,None) == password:
        return True
    return False

@auth.error_handler
def unauthorized():
    # return 403 instead of 401 to prevent browsers from displaying the default
    # auth dialog
    return {'message': 'Unauthorized access'}, 403

def abort_if_job_doesnt_exist(job_uuid):
    if job_uuid not in JOB_DB:
        abort(404, error="job {0} doesn't exist".format(job_uuid))

#####################################
parserJobs = reqparse.RequestParser()

#uuid of this job, in the format of "Month-Day-Year-Hours-Min-Sec-uuid"
#parserJobs.add_argument('uuid', type=str,required=True) 
parserJobs.add_argument('state', type=str,required=True)
parserJobs.add_argument('info', type=str,required=True)

#the process id of launcher, in the format of "23765"
parserJobs.add_argument('launcher_id', type=str,default="0")
parserJobs.add_argument('cluster', type=str,default="aws")#
parserJobs.add_argument('cluster_namenode', type=str,default="not available") #IP of NameNode
parserJobs.add_argument('cluster_info', type=str,default="not available") #if cluster is aws , then list how many resources are used
parserJobs.add_argument('user', type=str,default="user")#
parserJobs.add_argument('group', type=str,default="group")#

public_ip = None
local_ip = '127.0.0.1'
port = 5050

AWS_EC2_PRIVATE_DNS_SUFFIX='compute.internal'
IS_AWS_CLUSTER=False



if subprocess.check_output('hostname -d',shell=True).strip().endswith(AWS_EC2_PRIVATE_DNS_SUFFIX): #AWS instance
    IS_AWS_CLUSTER = True
    try:
        #public_ip= subprocess.check_output('curl http://instance-data/latest/meta-data/public-ipv4',shell=True).strip()
        public_ip = subprocess.check_output('curl -s http://169.254.169.254/latest/meta-data/public-ipv4',shell=True).strip()
        local_ip = subprocess.check_output('curl -s http://169.254.169.254/latest/meta-data/local-ipv4',shell=True).strip()
    except:
        public_ip = None
        local_ip='127.0.0.1'
    
else: # local instance
    try:
        public_ip = subprocess.check_output('hostname -I',shell=True).strip().split()[0]
        local_ip = public_ip
    except:
        public_ip = None
        local_ip = '127.0.0.1'

#temporary patch for virtual machine
#if public_ip=='192.168.127.139': #VM
#    public_ip='10.2.3.174' #host

if public_ip=='192.168.135.128': #VM
    public_ip='192.168.1.16' #host

if public_ip=='192.168.127.139': #VM
    public_ip='192.168.1.121' #host
    
        
jobs = defaultdict(list)


def aws_region_exists(region):
    """
        If a specified AWS region exist.

        Parameters:
            region - a AWS region in string. example: "us-east-1"  

        Returns:
            true if region exists, false otherwise

        Raises:
            None
            
        Info:    
            [RegionInfo:us-east-1,
             RegionInfo:cn-north-1, 
             RegionInfo:ap-northeast-1, 
             RegionInfo:eu-west-1,
             RegionInfo:ap-southeast-1, 
             RegionInfo:ap-southeast-2, 
             RegionInfo:us-west-2,
             RegionInfo:us-gov-west-1, 
             RegionInfo:us-west-1,
             RegionInfo:eu-central-1, 
             RegionInfo:sa-east-1]
    """
    for r in [r.replace('RegionInfo:','',1) for r in boto.ec2.regions()]:
        if r == region:
            return True
    return False

class Database(object):
    """
#start MongoDB server
mongod --fork --dbpath /home/hadoop/mongodb --logpath /home/hadoop/log/mongodb/main.log

#start client
mongo
>show dbs; #show all dbs
>use jobs; #switch to db "jobs"
>show collections; #show collections in current db
    
    """
    def __init__(self):
        client = pymongo.MongoClient('mongodb://localhost:27017/')
        db = client['test-database']
        collection = db['test-collection']
    
class Job(Resource):
    """
    POST /api/v1/jobs/:job_uuid
    
    GET:     Get the status of a job
    PUT:     Update the status of a job
    POST:    Start a new job
    DELETE:  Terminate a submitted job
    """
    decorators = [auth.login_required]
    
    #@marshal_with(job_fields, envelope='resource')
    def get(self, job_uuid):
        """get the status of job 
        """
        abort_if_job_doesnt_exist(job_uuid)
        return JOB_DB[job_uuid]
    
    def do_delete(self,**kwargs):
        """helper function for deleting a job
        """
        return scheduler.Scheduler(**kwargs).delete_job()
    
    def delete(self, job_uuid):
        """
        DELETE /api/v1/jobs/:id

        delete a job
        
        """
        print 'DELETE'
        abort_if_job_doesnt_exist(job_uuid)
        cluster_name = JOB_DB[job_uuid]['cluster']['name']
        job_process_id = JOB_DB[job_uuid]['job_process_id']
        if cluster_name=='aws':
            cluster_region = JOB_DB[job_uuid]['cluster']['region']
            cluster_master_ip = JOB_DB[job_uuid]['cluster']['master_node_ip']
            cluster_user = scheduler.CLUSTER_CONFIG['aws']['zone'][cluster_region]['user']
            cluster_keyfile = scheduler.CLUSTER_CONFIG['aws']['zone'][cluster_region]['key_file']
        else:
            cluster_master_ip = scheduler.CLUSTER_CONFIG[cluster_name]['master_hostname']
            cluster_user = scheduler.CLUSTER_CONFIG[cluster_name]['user']
            cluster_keyfile = scheduler.CLUSTER_CONFIG[cluster_name]['key_file']
            
        try:
            
            
            Process(target=self.do_delete, 
                    kwargs={'job_uri':'https://%s:%s@%s:%d/api/v1/jobs/%s' % (USER,PASSWORD,public_ip,port,job_uuid),
                            'cluster_ip':cluster_master_ip,
                            'cluster_user':cluster_user,
                            'cluster_keyfile':cluster_keyfile,
                            'job_process_id':job_process_id}).start()

            JOB_DB[job_uuid]['state']='terminated'
            #print "delete job %s from cluster %s" % (job_uuid,JOB_DB[job_uuid]['cluster'])
        except Exception,e:
            JOB_DB[job_uuid]['state']='Failed'
            #JOB_DB[job_uuid]['info']='\n'.join((JOB_DB[job_uuid]['info'],'Unable to terminate'))

        #return '', 204
        return make_response(jsonify({'message': 'Job deleted access'}), 204)

    def put(self,job_uuid):
        """
        PUT /api/v1/jobs/:id
        
        update the status of job
        
        "progress":
        [
            {
                    "time":"2015-05-20 14:23:12",
                    "level":10,
                    "message":"Initializing"
                    },
                    
                    {
                    "time":"2015-05-20 14:24:12",
                    "level":10,
                    "message":"Start cluster"
                    }
        ]
        """
        
        abort_if_job_doesnt_exist(job_uuid)
        
        data = request.get_json(force=True)
        
        progress =  data.get('progress',None)
        if progress: #update progress
            progress[u'time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            prev_progress = JOB_DB[job_uuid]['progress']
            prev_progress.append(progress)
            JOB_DB[job_uuid]['progress'] = prev_progress

        cluster =  data.get('cluster',None)
        if cluster: #update progress
            prev_cluster = JOB_DB[job_uuid]['cluster']
            for k in cluster:
                prev_cluster[k] = cluster[k] 
            JOB_DB[job_uuid]['cluster'] = prev_cluster

        job =  data.get('job',None)
        if job: #update progress
            prev_job = JOB_DB[job_uuid]['job']
            for k in job:
                prev_job[k] = job[k] 
            JOB_DB[job_uuid]['job'] = prev_job

        job_state =  data.get('state',None)
        if job_state:
            JOB_DB[job_uuid]['state'] = job_state
            
        job_process_id =  data.get('job_process_id',None)
        if job_process_id:
            JOB_DB[job_uuid]['job_process_id'] = job_process_id
             
        return JOB_DB[job_uuid], 201


class JobList(Resource):
    """
    insert/query jobs status (the job must be submitted already)
    """
    decorators = [auth.login_required]
    
    #GET /api/v1/jobs
    def get(self):
        """
        get the status of all jobs
        """
        auth = request.authorization
        
        jobs = []
        for k in JOB_DB.keys():
            print k,JOB_DB[k],auth.username
            if JOB_DB[k]['username'] == auth.username:
                jobs.append(k)
        
        return jobs,201

    def do_post(self,**kwargs):
        #print kwargs
        try: 
            scheduler.Scheduler(**kwargs).create_job()
            #print 'post job "%s" to cluster "%s" with id "%s"' % (script,cluster,job_uuid)
        except Exception,e:
            print e
            #JOB_DB[job_uuid] = {'uuid': job_uuid,'state': 'Failed','info': str(e)}
        
    def post(self):
        """
        POST /api/v1/jobs

        
        Start a new job and post its initial status to JobList
        
        Parameters:
        
        Returns:
        
        Raises:
        
        Info:
        A typical JSON request to start a new job 
        {
            "job_uuid":
            {
                "submitter":"abc",
                "state":"Running",
                "cluster":
                {
                    "name":"aws",
                    "region":"us-west-2",
                    "master_node_ip":"1.2.3.4",
                    "master_node_type":"c4.2xlarge",
                    "slave_node_type":"c4.2xlarge",
                    "slave_node_num":10
                }
                "progress":
                [
                    {
                    "time":"2015-05-20 14:23:12",
                    "level":10,
                    "message":"Initializing"
                    },
                    
                    {
                    "time":"2015-05-20 14:24:12",
                    "level":10,
                    "message":"Start cluster"
                    }
                ]
            }
        }
        """
        data = request.get_json(force=True)
        auth = request.authorization
        try:
            cluster_name = data['cluster']
        except:
            return {"Error":"cluster name is required"}, 401

        try:
            job_script = data['job_script']
            print job_script
        except:
            return {"Error":"job script is required"}, 401

        cluster_region = None
        if cluster_name.startswith('aws'):
            toks = cluster_name.split('/')
            cluster_name = 'aws'
            if len(toks)== 2:
                cluster_region = toks[1]
            else:
                cluster_region = 'us-west-2'
            

        uuid_tag=binascii.hexlify(os.urandom(8))#8 bytes random as UUID
        #job_uuid = "-".join((time.strftime('%b-%d-%Y-%H-%M-%S'),uuid_tag))
        current_time = time.strftime('%Y-%m-%d %H:%M:%S')
        job_uuid = "-".join((time.strftime('%Y-%m-%d-%H-%M-%S'),uuid_tag))
        
        kwargs = {}
        kwargs['uuid'] = job_uuid
        kwargs['job_uri'] = 'https://%s:%s@%s:%d/api/v1/jobs/%s' % (USER,PASSWORD,public_ip,port,job_uuid)
        kwargs['cluster_name']=cluster_name
        kwargs['job_script']=job_script
        if cluster_region:
            kwargs['aws_region']=cluster_region
        
        #start the process    
        Process(target=self.do_post,kwargs=kwargs).start()
        
        status = {} #kwargs #"cluster" and "script"
    
        status['state'] = 'Running'
        cluster_dict = {}
        cluster_dict['name'] = cluster_name
        if cluster_region:
            cluster_dict['region'] = cluster_region
            
        status["cluster"] = cluster_dict
        progress_dict = {}
        progress_dict['time'] = current_time
        progress_dict['level'] = 10
        progress_dict['message'] = "Initialized"
        
        status["progress"] = [progress_dict]
        
        job_dict = {}
        job_dict['name'] = job_script
        
        status["job"] = job_dict
        status["username"] = auth.username
        status["job_process_id"] = "none"
        JOB_DB[job_uuid] = status
        
        #return make_response(jsonify({'job_uuid': job_uuid}), 201)
        #return {job_uuid: JOB_DB[job_uuid]}, 201
        #print JOB_DB[job_uuid]
        return job_uuid, 201
    

#####################################

api.add_resource(JobList, '/api/v1/jobs')
api.add_resource(Job, '/api/v1/jobs/<job_uuid>')    

def test():
    """
    
curl -H 'Content-Type: application/json' -H 'Accept: application/json' -k -X POST -d '{"cluster":{"name":"aws","master_node_type":"c4.2xlarge"}}' https://clownfish:utah20zer07@192.168.1.16:5050/api/v1/submission

#submit a job to home cluster
curl -k -X POST -d "cluster=home" -d "script=jobs/job_home_1m.sh" https://clownfish:utah20zer07@192.168.135.128:5050/api/v1/submission
curl -k -X GET https://clownfish:utah20zer07@192.168.135.128:5050/api/v1/jobs

#submit a job to AWS cluster
curl -k -X POST -d "cluster=aws" -d "script=jobs/job_aws_simple.sh" https://clownfish:utah20zer07@52.25.218.103:5050/api/v1/submission
curl -k -X POST -d "cluster=aws" -d "script=jobs/job_aws_NA12878.sh" https://clownfish:utah20zer07@52.25.218.103:5050/api/v1/submission
curl -k -X GET https://clownfish:utah20zer07@52.25.218.103:5050/api/v1/jobs

#submit a job to AWS cluster
curl -k -X POST -d "cluster=master" -d "script=jobs/job_master_100k.sh" https://clownfish:utah20zer07@192.168.127.139:5050/api/v1/submission
curl -k -X GET https://clownfish:utah20zer07@192.168.127.139:5050/api/v1/jobs
    """

def install_dep():    
    """
    #create TLS certificate
    mkdir certs
    openssl req -newkey rsa:2048 -nodes -keyout certs/domain.key -x509 -days 365 -out certs/domain.crt
    """


if __name__=='__main__':
    #if not public_ip:
    #    print "Error: unable to find public IP"
        
    if public_ip:
        print 'Public https://%s:%d' % (public_ip,port)
    #else:
    #print 'Bind to private %s:%d' % (local_ip,port)
    context = ('certs/domain.crt', 'certs/domain.key')
    app.run(host=local_ip,port=port,ssl_context=context,threaded=True,debug=True)
    
