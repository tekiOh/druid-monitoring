from django.shortcuts import render
from django.http import HttpResponse, HttpRequest, JsonResponse
import json
import datetime
import time
from . import detailview_quries
from . import data_handle


def get_detailview_data(query):
    json_response = data_handle.get_data_from_druid(query)
    metric_list = data_handle.make_json({}, json_response)

    data_handle.add_percent(metric_list)

    final_metrics_list = data_handle.get_final_json(metric_list)
    return final_metrics_list

def get_detailview_broker(query_format):
    query = detailview_quries.query_broker_detailview % (tuple(query_format))
    return get_detailview_data(query)

def get_detailview_historical(query_format):
    query = detailview_quries.query_historical_detailview % (tuple(query_format))
    return get_detailview_data(query)

def get_detailview_coordinator(query_format):
    query = detailview_quries.query_coordinator_detailview % (tuple(query_format))
    return get_detailview_data(query)

def get_detailview_overlord(query_format):
    query = detailview_quries.query_overlord_detailview % (tuple(query_format))
    return get_detailview_data(query)

def get_detailview_middleManager(query_format):
    query = detailview_quries.query_middleManager_detailview % (tuple(query_format))
    return get_detailview_data(query)

def request_handle(request):
    if request.method == "POST":
        body_unicode = request.body.decode('utf-8')
        body_unicode = body_unicode.replace("'","\"")
        body = json.loads(body_unicode)
        print(body)
        nodetype = body['node']
        query_format = [body["granuality"], body["start_time"],
                        body["end_time"], body["node"],
                        body["server"] + ":" + body["port"]]

        if 'historical' in nodetype:
            return HttpResponse(json.dumps(get_detailview_historical(query_format), indent=4, sort_keys=True))
        elif 'broker' in nodetype:
            return HttpResponse(json.dumps(get_detailview_broker(query_format), indent=4, sort_keys=True))
        elif 'coordinator' in nodetype:
            return HttpResponse(json.dumps(get_detailview_coordinator(query_format), indent=4, sort_keys=True))
        elif 'overlord' in nodetype:
            return HttpResponse(json.dumps(get_detailview_overlord(query_format), indent=4, sort_keys=True))
        else:
            return HttpResponse(json.dumps(get_detailview_middleManager(query_format), indent=4, sort_keys=True))

        return HttpResponse(json.dumps({'success' : True}))
