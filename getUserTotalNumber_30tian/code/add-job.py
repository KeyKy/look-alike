#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# 通过cloud-manager接口，往上面提交任务

import time
import requests
import boto3
import config
import utils
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-n', '--name', dest='name',
        help='Job name.')
parser.add_option('-c', '--cluster', dest='cluster',
        help='Cluster name.')
parser.add_option('-f', '--script-path', dest='path',
        help='Script path.')
parser.add_option('-p', '--params', dest='params', default='',
        help='params for spark script, seperated with comma.')
parser.add_option('-t', '--task-nodes', dest='task_nodes', type='int', default = 0,
        help='number of task nodes to add.')
parser.add_option('-a', '--create-new-cluster', dest='new_cluster', type='int', default = 0,
        help='should create new cluster, 1: create new, 0: do not create new if cluster exists.')
parser.add_option('-d', '--deploy-mode', dest='deploy_mode', default = 'cluster',
        help='deploy mode [cluster|client]')
parser.add_option('-r', '--required-script', dest='required_script', default = '',
        help='Required Script path.')


def add_job(job_name, cluster_name, script_path, script_params, task_node_count=0, new_cluster=0, deploy_mode='cluster', required_script=''):
    payload = {
            'job_name': job_name,
            'cluster_name': cluster_name,
            'script_path': script_path,
            'script_params': script_params,
            'task_node_count': task_node_count,
            'new_cluster': new_cluster,
            'receivers': config.RECEIVERS,
            'deploy_mode':deploy_mode,
            'required_script': required_script,
        }

    data = utils.str_to_sign('POST', config.ADD_JOB_URL, payload)
    sign = utils.hash_hmac(config.SECRET_KEY, data)
    payload['sign'] = sign

    r = requests.post(config.ADD_JOB_URL, data=payload)
    print(r.text)


def main():
    (options, args) = parser.parse_args()
    if options.path is None or options.name is None or options.cluster is None:
        parser.print_help()
        return

    add_job(options.name, options.cluster, options.path, options.params,
            task_node_count=options.task_nodes,
            new_cluster=options.new_cluster,
            deploy_mode=options.deploy_mode,
            required_script=options.required_script)


if __name__ == '__main__':
    main()
