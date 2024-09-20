from prometheus_matcher import PrometheusMatcher

prom = PrometheusMatcher("http://prometheus.ran-metrics.hosts.prod.psi.rdu2.redhat.com/")

# for i in prom.get_all_metrics():
#     print(i)

print(prom.get_metric_dataframe_group('last_over_time(ranmetrics_cpu_infra_pods_steadyworkload_avg{baseline="false", cluster="cnfdg15", power_mode="performance", pod="collector-"}[30d])',"ranmetrics_cpu_infra_pods_steadyworkload_avg", groupby=['sw_version']))
# print(prom.get_metric_dataframe('ranmetrics_cpu_infra_pods_steadyworkload_max',"name"))