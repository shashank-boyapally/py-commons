from prometheus_api_client import PrometheusConnect
from logrus import SingletonLogger
import logging
import pandas as pd
import packaging.version

class PrometheusMatcher():
    
    def __init__(self,PROM_URL:str,debug_level:int = logging.INFO):
        self.prometheus_client = PrometheusConnect(url=PROM_URL,disable_ssl=True)
        self.logger = SingletonLogger(debug=debug_level, name="Matcher")
    
    def get_all_metrics(self):
        self.logger.info("Getting all available metrics")
        return self.prometheus_client.all_metrics()
    
    def get_metric_dataframe(self,query, name):
        self.logger.info(f"Getting the metrics for {name}")
        data = self.run_query(query)
        print(data)
        d ={"timestamp":[],
            "value" :[]}
        for i in data:
            d["timestamp"].append(i['value'][0])
            d["value"].append(i['value'][1])
        data = pd.DataFrame(d)
        data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')
        data = data.sort_values(by=["timestamp"])
        return data
    
    def get_metric_dataframe_group(self,query, name, groupby=[]):
        self.logger.info(f"Getting the metrics for {name}")
        data = self.run_query(query)
        print(data)
        d ={"timestamp":[],
            name :[]}
        for i in groupby:
            d[i]=[]
        for i in data:
            d["timestamp"].append(i['value'][0])
            d[name].append(i['value'][1])
            for key in i["metric"].keys():
                if key in d.keys():
                    d[key].append(i["metric"][key])
        df = pd.DataFrame(d)

        df[name] = pd.to_numeric(df[name], errors='coerce')

        df['parsed_version'] = df['sw_version'].apply(packaging.version.parse)

        df = df.sort_values(by='parsed_version')

        grouped_df = df.groupby(groupby, as_index=False).agg({
            'timestamp': 'first', 
            name : 'mean'        
        })

        grouped_df['parsed_version'] = grouped_df['sw_version'].apply(packaging.version.parse)
        grouped_df = grouped_df.sort_values(by='parsed_version').drop(columns=['parsed_version'])

        print(grouped_df)
        
    
    def run_query(self,query):
        return self.prometheus_client.custom_query(query)

class PrometheusQueryBuilder:
    def __init__(self, metric_name):
        self.metric_name = metric_name
        self.filters = []
    
    def add_filter(self, label, value):
        self.filters.append(f'{label}="{value}"')
        return self
    
    def over_time(self, duration):
        self.duration = duration
        return self
    
    def build(self):
        filter_str = "{" + ",".join(self.filters) + "}" if self.filters else ""
        return f'{self.metric_name}{filter_str}[{self.duration}]'