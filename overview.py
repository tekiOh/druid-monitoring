from django.shortcuts import render
from django.http import HttpResponse, HttpRequest, JsonResponse
import json
import datetime
import time
from . import overview_queries
from . import data_handle

def get_jvm_overview_kpi(query_format):
    query_format[0] = "hour"
    query = overview_queries.query_jvm_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)
    metric_list = data_handle.make_json({}, json_response)

    for v in metric_list.values():
        key_list = list(v.keys())
        for i in range(0, len(key_list)):
            for j in range(i + 1, len(key_list)):
                if key_list[i].split('/')[:-1] == key_list[j].split('/')[:-1]:
                    for f, s in zip(v[key_list[i]], v[key_list[j]]):
                        s['percent'] = round((s['avg'] / f['avg']) * 100, 1)
                    break

    for v in metric_list.values():
        for metric in ['jvm/bufferpool/capacity', 'jvm/gc/mem/max', 'jvm/mem/max', 'jvm/pool/max']:
            v.pop(metric, None)

    print("kpi!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
    print(metric_list)

    # print(json.dumps(metric_list, indent=4, sort_keys=True))

    dictresponse = data_handle.get_kpi_json({},metric_list)

    # print(json.dumps(dictresponse, indent=4, sort_keys=True))

    return dictresponse

def get_jvm_overview(nodename):
    print("jvm overview function in!!")
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    query_format = [granuality,interval_s, interval_e,nodename]

    query = overview_queries.query_jvm_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = data_handle.make_json({},json_response)
    start_time = time.clock()
    for v in metric_list.values():
        key_list = list(v.keys())
        for i in range(0, len(key_list)):
            for j in range(i + 1, len(key_list)):
                if key_list[i].split('/')[:-1] == key_list[j].split('/')[:-1]:
                    for f, s in zip(v[key_list[i]], v[key_list[j]]):
                        s['percent'] = round((s['avg'] / f['avg']) * 100, 1)
                    break

    for v in metric_list.values():
        for metric in ['jvm/bufferpool/capacity', 'jvm/gc/mem/max', 'jvm/mem/max', 'jvm/pool/max']:
            v.pop(metric, None)

    kpiresponse = get_jvm_overview_kpi(query_format)
    for k in kpiresponse.keys():
        if k in metric_list:
            for ik,iv in kpiresponse[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv


    # print("jvm overview kpi print")
    # print(json.dumps(metric_list, indent=4, sort_keys=True))
    # for k,v in metric_list.items():
    #     print(k)
    #     for ik,iv in metric_list[k].items():
    #         print(ik)


    return metric_list

def get_node_overview_kpi(query_format):
    query_format[0] = "hour"
    print(query_format)
    if "broker" in query_format[3]:
        query = overview_queries.query_broker_overview % (tuple(query_format))
    if "historical" in query_format[3]:
        query = overview_queries.query_historical_overview % (tuple(query_format))
    if "coordinator" in query_format[3]:
        query = overview_queries.query_coordinator_overview % (tuple(query_format))
    if "overlord" in query_format[3]:
        query = overview_queries.query_overlord_overview % (tuple(query_format))
    if "middleManager" in query_format[3]:
        query = overview_queries.query_middleManager_overview % (tuple(query_format))

    json_response = data_handle.get_data_from_druid(query)
    metric_list = data_handle.make_json({}, json_response)

    dictresponse = data_handle.get_kpi_json({}, metric_list)

    return dictresponse

def get_broker_overview(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/broker"
    query_format = [granuality,interval_s, interval_e,nodetype]
    print("&" * 100)
    print("&" * 100)
    print("&" * 100)
    print("&" * 100)
    print(servertime.minute)
    start_time = time.clock()

    query = overview_queries.query_broker_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/broker")
    metric_list = data_handle.make_json(metric_list, json_response)

    broker_kpi = get_node_overview_kpi(query_format)

    for k in metric_list.keys():
        if k in broker_kpi:
            for ik,iv in broker_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    # print("metric_list" * 10)
    # print(json.dumps(metric_list, indent=4, sort_keys=True))

    final_metrics_list = data_handle.get_final_json(metric_list)

    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    t = time.clock() - start_time
    print("broker overview takes...")
    print(t, "seconds")
    return HttpResponse(json.dumps(final_metrics_list, indent=4, sort_keys=True))


def get_historical_overview(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype =  "druid/dev/historical"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()

    query = overview_queries.query_historical_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/historical")
    metric_list = data_handle.make_json(metric_list, json_response)

    t = time.clock() - start_time
    print("historical overview takes...")
    print(t, "seconds")

    # for j in metric_list.keys():
    #     print(j)
    #     for ik,iv in metric_list[j].items():
    #         print(ik,iv)
    #         print(len(iv))

    historical_kpi = get_node_overview_kpi(query_format)
    for k in metric_list.keys():
        if k in historical_kpi:
            for ik,iv in historical_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)

    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    t = time.clock() - start_time
    print("historical overview takes...")
    print(t, "seconds")
    return HttpResponse(json.dumps(final_metrics_list, indent=4, sort_keys=True))

def get_coordinator_overview(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/coordinator"
    query_format = [granuality,interval_s, interval_e, nodetype]

    start_time = time.clock()

    query = overview_queries.query_coordinator_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/coordinator")
    metric_list = data_handle.make_json(metric_list, json_response)

    for j in metric_list.keys():
        print(j)
        for ik,iv in metric_list[j].items():
            print(ik,iv)
            print(len(iv))

    coordinator_kpi = get_node_overview_kpi(query_format)
    for k in metric_list.keys():
        if k in coordinator_kpi:
            for ik,iv in coordinator_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)
    t = time.clock() - start_time
    print("coordinator overview takes...")
    print(t, "seconds")
    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))

    return HttpResponse(json.dumps(final_metrics_list, indent=4, sort_keys=True))


def get_overlord_overview(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/overlord"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()

    query = overview_queries.query_overlord_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/overlord")
    metric_list = data_handle.make_json(metric_list, json_response)

    # for j in metric_list.keys():
    #     print(j)
    #     for ik,iv in metric_list[j].items():
    #         print(ik,iv)
    #         print(len(iv))

    overlord_kpi = get_node_overview_kpi(query_format)
    for k in metric_list.keys():
        if k in overlord_kpi:
            for ik,iv in overlord_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)
    t = time.clock() - start_time

    #print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    print("overlord overview takes...")
    print(t, "seconds")

    return HttpResponse(json.dumps(final_metrics_list, indent=4, sort_keys=True))

def get_middleManager_overview(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/middleManager"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()
    print("middleManager query make!!")
    query = overview_queries.query_middleManager_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/middleManager")
    metric_list = data_handle.make_json(metric_list, json_response)

    broker_kpi = get_node_overview_kpi(query_format)
    print("middlemanager_kpi!!!!!!!!"*10)
    #print(broker_kpi)
    for k in metric_list.keys():
        if k in broker_kpi:
            for ik,iv in broker_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)
    for k,v in final_metrics_list.items():
        print(k)
        for ik,iv in v.items():
            print(ik)
    #print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    t = time.clock() - start_time
    print("middleManager overview takes...")
    print(t, "seconds")

    return HttpResponse(json.dumps(final_metrics_list, indent=4, sort_keys=True))

def get_node_list(request):
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(days=99999)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    query_format = [interval_s, interval_e]

    start_time = time.clock()

    query = overview_queries.query_get_node_list % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    node_list = {}
    for j in json_response:
        ip = j["event"]["host"].split(':')[0]
        port = j["event"]["host"].split(':')[-1]
        if ip not in node_list:
            node_list[ip] = {}
        if j["event"]["service"] not in node_list[ip]:
            node_list[ip][j["event"]["service"]] = []
        if port not in node_list[ip][j["event"]["service"]]:
            node_list[ip][j["event"]["service"]].append(port)
    t = time.clock() - start_time
    print("[get_node_list] takes...")
    print(t, "seconds")

    print(node_list)

    return HttpResponse(json.dumps(node_list))


def get_metriclist(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/broker"
    query_format = [granuality, interval_s, interval_e, nodetype]

    start_time = time.clock()

    query = overview_queries.query_broker_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = {}
    for j in json_response:
        service_host = j['event']['service']+':'+j['event']['host']
        metric_name = j['event']['metric']
        if service_host not in metric_list:
            metric_list[service_host] = {}
        if metric_name not in metric_list[service_host]:
            metric_list[service_host][metric_name] = []
            metric_list[service_host][metric_name].append({'timestamp' : j['timestamp'],'avg' : j['event']['AVG(value)']})
    t = time.clock() - start_time
    print("[get_metric_list] takes...")
    print(t, "seconds")

    return HttpResponse(json.dumps(metric_list))

#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
#######################################################################
def get_broker_overview_p():
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/broker"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()

    query = overview_queries.query_broker_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/broker")
    metric_list = data_handle.make_json(metric_list, json_response)

    broker_kpi = get_node_overview_kpi(query_format)

    for k in metric_list.keys():
        if k in broker_kpi:
            for ik,iv in broker_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    # print("metric_list" * 10)
    # print(json.dumps(metric_list, indent=4, sort_keys=True))

    final_metrics_list = data_handle.get_final_json(metric_list)

    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    t = time.clock() - start_time
    print("broker overview takes...")
    print(t, "seconds")
    return final_metrics_list


def get_historical_overview_p():
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype =  "druid/dev/historical"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()

    query = overview_queries.query_historical_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/historical")
    metric_list = data_handle.make_json(metric_list, json_response)

    t = time.clock() - start_time
    print("historical overview takes...")
    print(t, "seconds")

    # for j in metric_list.keys():
    #     print(j)
    #     for ik,iv in metric_list[j].items():
    #         print(ik,iv)
    #         print(len(iv))

    historical_kpi = get_node_overview_kpi(query_format)
    for k in metric_list.keys():
        if k in historical_kpi:
            for ik,iv in historical_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)

    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    t = time.clock() - start_time
    print("historical overview takes...")
    print(t, "seconds")
    return final_metrics_list

def get_coordinator_overview_p():
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/coordinator"
    query_format = [granuality,interval_s, interval_e, nodetype]

    start_time = time.clock()

    query = overview_queries.query_coordinator_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/coordinator")
    metric_list = data_handle.make_json(metric_list, json_response)

    for j in metric_list.keys():
        print(j)
        for ik,iv in metric_list[j].items():
            print(ik,iv)
            print(len(iv))

    coordinator_kpi = get_node_overview_kpi(query_format)
    for k in metric_list.keys():
        if k in coordinator_kpi:
            for ik,iv in coordinator_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)
    t = time.clock() - start_time
    print("coordinator overview takes...")
    print(t, "seconds")
    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))

    return final_metrics_list


def get_overlord_overview_p():
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/overlord"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()

    query = overview_queries.query_overlord_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/overlord")
    metric_list = data_handle.make_json(metric_list, json_response)

    for j in metric_list.keys():
        print(j)
        for ik,iv in metric_list[j].items():
            print(ik,iv)
            print(len(iv))

    overlord_kpi = get_node_overview_kpi(query_format)
    for k in metric_list.keys():
        if k in overlord_kpi:
            for ik,iv in overlord_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)
    t = time.clock() - start_time

    print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    print("overlord overview takes...")
    print(t, "seconds")

    return final_metrics_list

def get_middleManager_overview_p():
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/middleManager"
    query_format = [granuality,interval_s, interval_e,nodetype]

    start_time = time.clock()
    print("middleManager query make!!")
    query = overview_queries.query_middleManager_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = get_jvm_overview("druid/dev/middleManager")
    metric_list = data_handle.make_json(metric_list, json_response)

    broker_kpi = get_node_overview_kpi(query_format)

    for k in metric_list.keys():
        if k in broker_kpi:
            for ik,iv in broker_kpi[k].items():
                if ik not in metric_list[k]:
                    metric_list[k][ik] = iv

    final_metrics_list = data_handle.get_final_json(metric_list)

    #print(json.dumps(final_metrics_list, indent=4, sort_keys=True))
    t = time.clock() - start_time
    print("middleManager overview takes...")
    print(t, "seconds")

    return final_metrics_list

def get_node_list(request):
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=10)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    query_format = [interval_s, interval_e]

    start_time = time.clock()

    query = overview_queries.query_get_node_list % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    node_list = {}
    for j in json_response:
        ip = j["event"]["host"].split(':')[0]
        port = j["event"]["host"].split(':')[-1]
        if ip not in node_list:
            node_list[ip] = {}
        if j["event"]["service"] not in node_list[ip]:
            node_list[ip][j["event"]["service"]] = []
        if port not in node_list[ip][j["event"]["service"]]:
            node_list[ip][j["event"]["service"]].append(port)
    t = time.clock() - start_time
    print("[get_node_list] takes...")
    print(t, "seconds")

    print(node_list)

    return HttpResponse(json.dumps(node_list))


def get_metriclist(request):
    granuality = "minute"
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(minutes=60)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()
    nodetype = "druid/dev/broker"
    query_format = [granuality, interval_s, interval_e, nodetype]

    start_time = time.clock()

    query = overview_queries.query_broker_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)

    metric_list = {}
    for j in json_response:
        service_host = j['event']['service']+':'+j['event']['host']
        metric_name = j['event']['metric']
        if service_host not in metric_list:
            metric_list[service_host] = {}
        if metric_name not in metric_list[service_host]:
            metric_list[service_host][metric_name] = []
            metric_list[service_host][metric_name].append({'timestamp' : j['timestamp'],'avg' : j['event']['AVG(value)']})
    t = time.clock() - start_time
    print("[get_metric_list] takes...")
    print(t, "seconds")

    return HttpResponse(json.dumps(metric_list))




def postjson(request):
    nowtime = datetime.datetime.now()
    servertime = nowtime - datetime.timedelta(hours=9)
    stime = servertime - datetime.timedelta(hours=1)
    interval_e = servertime.isoformat()
    interval_s = stime.isoformat()

    query = {}
    query["server"] = "localhost"
    query["port"] = "8083"
    query["node"] = "druid/dev/historical"
    query["start_time"] = interval_s
    query["end_time"] = interval_e
    query["granuality"] = "minute"
    response = data_handle.postdata(query)
    print(response)
    return HttpResponse(json.dumps(response))


def get_overview_all(request):
    overview_data = {}
    overview_data.update(get_broker_overview_p())
    overview_data.update(get_historical_overview_p())
    overview_data.update(get_coordinator_overview_p())
    overview_data.update(get_overlord_overview_p())
    overview_data.update(get_middleManager_overview_p())
    print("*" * 30)
    print("*" * 30)
    print("*" * 30)
    print(json.dumps(overview_data, indent=4, sort_keys=True))
    return HttpResponse(json.dumps(overview_data))

def test(request):
    st = datetime.datetime(2018, 8, 6, 5, 0, 0, 0)
    et = st - datetime.timedelta(minutes=61)
    query_format = ["hour",et.isoformat(),st.isoformat(),"druid/dev/broker"]
    query = overview_queries.query_broker_overview % (tuple(query_format))
    json_response = data_handle.get_data_from_druid(query)
    print(json_response)
    return HttpResponse(json.dumps(json_response))



