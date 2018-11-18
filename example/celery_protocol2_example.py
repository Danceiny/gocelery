import json
import os
import socket
import uuid

task_id = uuid.uuid4()
args = (2, 2)
kwargs = {}
# basic_publish(
#     message=json.dumps((args, kwargs, None),
#                        application_headers={
#                            'lang'      : 'py',
#                            'task'      : 'proj.tasks.add',
#                            'argsrepr'  : repr(args),
#                            'kwargsrepr': repr(kwargs),
#                            'origin'    : '@'.join([os.getpid(), socket.gethostname()])
#                        }
# properties = {
#     'correlation_id'  : task_id,
#     'content_type'    : 'application/json',
#     'content_encoding': 'utf-8',
# }
# )

std_msg = {
    args  : (
        'worker.add_reflect',
        u'0348a99e-129b-452c-86b4-238dd26164b7',
        {
            u'origin'        : u'gen64132@DanceinydeMacBook-Pro.local',
            u'lang'          : u'py',
            u'task'          : u'worker.add_reflect',
            u'group'         : None,
            u'root_id'       : u'0348a99e-129b-452c-86b4-238dd26164b7',
            u'delivery_info' :
                {
                    u'priority'   : 0,
                    u'redelivered': None,
                    u'routing_key': u'celery',
                    u'exchange'   : u''},
            u'expires'       : None,
            u'correlation_id': u'0348a99e-129b-452c-86b4-238dd26164b7',
            u'retries'       : 0,
            u'timelimit'     : [None, None],
            u'argsrepr'      : u'()',
            u'eta'           : None,
            u'parent_id'     : None,
            u'reply_to'      : u'0af62ba9-adce-3a76-b3f8-43c32e74ee9a',
            u'shadow'        : None,
            u'id'            : u'0348a99e-129b-452c-86b4-238dd26164b7',
            u'kwargsrepr'    : u"{'y': 2878, 'x': 5456}"
        },

        '[[], {"y": 2878, "x": 5456},{"chord": null,"callbacks": null,"errbacks": null, "chain": null}   ]',

        u'application/json',
        u'utf-8'),
    kwargs: {}
}

args:(
    'worker.add',
    '6adc064d-759c-4588-b4c2-85ab798ff76b',
    {
        'lang'          : 'go',
        'task'          : 'worker.add',
        'root_id'       : '7c8ccac3-a676-4a09-b0ea-fadbc34ddf5d',
        'delivery_info' : {
            'priority'   : None,
            'redelivered': None,
            'routing_key': '',
            'exchange'   : ''
        },
        'parent_id'     : 'ddca66e4-c05a-487f-bd0f-f578dcbbb82e',
        'correlation_id': '3dc0b02c-64ab-4d3a-96a7-227e2e76d619',
        'reply_to'      : None,
        'group_id'      : '4c7d5441-cf3e-45a5-8785-7944662f9d4f',
        'id'            : '6adc064d-759c-4588-b4c2-85ab798ff76b'
    },
    'eyJpZCI6IjZhZGMwNjRkLTc1OWMtNDU4OC1iNGMyLTg1YWI3OThmZjc2YiIsInRhc2siOiJ3b3JrZXIuYWRkIiwiYXJncyI6WzEsN10sImt3YXJncyI6e30sInJldHJpZXMiOjAsImV0YSI6IjIwMTgtMTEtMTlUMDA6MDY6NTQiLCJleHBpcmVzIjoiMjAxOC0xMS0xOVQwMDowODoyOSJ9',
    None,
    None)
kwargs:{

})
