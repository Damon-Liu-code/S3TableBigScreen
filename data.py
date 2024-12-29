import json
from pyspark.sql import SparkSession

class SourceDataDemo:
    def __init__(self):
        # Initialize Spark session with S3 Tables configurations
        self.spark = SparkSession.builder \
            .appName("BigScreenData") \
            .config("spark.jars.packages", 
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3") \
            .config("spark.sql.catalog.s3tablesbucket", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.s3tablesbucket.catalog-impl", 
                    "software.amazon.s3tables.iceberg.S3TablesCatalog") \
            .config("spark.sql.extensions", 
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()

        # Load data from S3 Table
        self.load_data_from_s3()

    def load_data_from_s3(self):
        try:
            # Query map_data table
            df = self.spark.sql("""
                SELECT * FROM s3tablesbucket.testdb.map_data
            """)
            
            # Convert to dictionary for easier processing
            data_dict = {}
            for row in df.collect():
                chart_type = row['chart_type']
                if chart_type not in data_dict:
                    data_dict[chart_type] = []
                
                # Create data entry based on chart type
                data_entry = {
                    "name": row['name'],
                    "value": float(row['value'])
                }
                
                # Add optional fields if they exist
                if row['value2'] is not None:
                    data_entry["value2"] = float(row['value2'])
                if row['color'] is not None:
                    data_entry["color"] = row['color']
                if row['radius_start'] is not None and row['radius_end'] is not None:
                    data_entry["radius"] = [row['radius_start'], row['radius_end']]
                if row['symbol_size'] is not None:
                    data_entry["symbolSize"] = float(row['symbol_size'])
                
                data_dict[chart_type].append(data_entry)

            # Set class attributes based on chart types
            self.title = '大数据可视化展板通用模板'
            
            # Counter data
            counter_data = df.filter(df.chart_type == 'counter').collect()
            self.counter = {
                'name': counter_data[0]['chart_title'] if counter_data else '2023年总收入情况',
                'value': float(counter_data[0]['value']) if counter_data else 0
            }
            self.counter2 = {
                'name': counter_data[1]['chart_title'] if len(counter_data) > 1 else '2023年总支出情况',
                'value': float(counter_data[1]['value']) if len(counter_data) > 1 else 0
            }

            # Chart 1 data (Industry distribution)
            echart1_rows = df.filter(df.chart_type == 'industry').collect()
            self.echart1_data = {
                'title': '行业分布',
                'data': [{"name": row['name'], "value": float(row['value'])} for row in echart1_rows]
            }

            # Chart 2 data (Province distribution)
            echart2_rows = df.filter(df.chart_type == 'province').collect()
            self.echart2_data = {
                'title': '省份分布',
                'data': [{"name": row['name'], "value": float(row['value'])} for row in echart2_rows]
            }

            # Chart 3 data (Age, Occupation, Interest distribution)
            self.echarts3_1_data = {
                'title': '年龄分布',
                'data': [{"name": row['name'], "value": float(row['value'])} 
                        for row in df.filter(df.chart_type == 'age').collect()]
            }
            
            self.echarts3_2_data = {
                'title': '职业分布',
                'data': [{"name": row['name'], "value": float(row['value'])} 
                        for row in df.filter(df.chart_type == 'occupation').collect()]
            }
            
            self.echarts3_3_data = {
                'title': '兴趣分布',
                'data': [{"name": row['name'], "value": float(row['value'])} 
                        for row in df.filter(df.chart_type == 'interest').collect()]
            }

            # Chart 4 data (Time trend)
            echart4_rows = df.filter(df.chart_type == 'time_trend').collect()
            self.echart4_data = {
                'title': '时间趋势',
                'data': [],
                'xAxis': []
            }
            
            for row in echart4_rows:
                if row['value_list']:
                    values = json.loads(row['value_list'])
                    self.echart4_data['data'].append({
                        "name": row['name'],
                        "value": values
                    })
                if row['x_axis']:
                    self.echart4_data['xAxis'] = json.loads(row['x_axis'])

            # Chart 5 data (Province TOP)
            echart5_rows = df.filter(df.chart_type == 'province_top').collect()
            self.echart5_data = {
                'title': '省份TOP',
                'data': [{"name": row['name'], "value": float(row['value'])} 
                        for row in echart5_rows]
            }

            # Chart 6 data (First-tier cities)
            echart6_rows = df.filter(df.chart_type == 'first_tier').collect()
            self.echart6_data = {
                'title': '一线城市情况',
                'data': [{
                    "name": row['name'],
                    "value": float(row['value']),
                    "value2": float(row['value2']) if row['value2'] else 0,
                    "color": row['color'] if row['color'] else "01",
                    "radius": [row['radius_start'] or '20%', row['radius_end'] or '30%']
                } for row in echart6_rows]
            }

            # Map data
            map_rows = df.filter(df.chart_type == 'map').collect()
            self.map_1_data = {
                'symbolSize': float(map_rows[0]['symbol_size']) if map_rows and map_rows[0]['symbol_size'] else 100,
                'data': [{"name": row['name'], "value": float(row['value'])} 
                        for row in map_rows]
            }

        except Exception as e:
            print(f"Error loading data from S3 Table: {str(e)}")
            # Set default values in case of error
            self._set_default_values()

    def _set_default_values(self):
        # Set default values as in your original code
        self.title = '大数据可视化展板通用模板'
        self.counter = {'name': '2023年总收入情况', 'value': 0}
        self.counter2 = {'name': '2023年总支出情况', 'value': 0}
        # ... (rest of your default values)

    # Keep your existing property methods
    @property
    def echart1(self):
        data = self.echart1_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'series': [i.get("value") for i in data.get('data')]
        }
        return echart

    @property
    def echart2(self):
        data = self.echart2_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'series': [i.get("value") for i in data.get('data')]
        }
        return echart

    @property
    def echarts3_1(self):
        data = self.echarts3_1_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echarts3_2(self):
        data = self.echarts3_2_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echarts3_3(self):
        data = self.echarts3_3_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echart4(self):
        data = self.echart4_data
        echart = {
            'title': data.get('title'),
            'names': [i.get("name") for i in data.get('data')],
            'xAxis': data.get('xAxis'),
            'data': data.get('data'),
        }
        return echart

    @property
    def echart5(self):
        data = self.echart5_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'series': [i.get("value") for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echart6(self):
        data = self.echart6_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get("name") for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def map_1(self):
        data = self.map_1_data
        echart = {
            'symbolSize': data.get('symbolSize'),
            'data': data.get('data'),
        }
        return echart


class SourceData(SourceDataDemo):

    def __init__(self):
        """
        按照 SourceDataDemo 的格式覆盖数据即可
        """
        super().__init__()
        self.title = '大数据可视化展板通用模板'

class CorpData(SourceDataDemo):

    def __init__(self):
        """
        按照 SourceDataDemo 的格式覆盖数据即可
        """
        super().__init__()
        with open('corp.json', 'r', encoding='utf-8') as f:
            data = json.loads(f.read())
        self.title = data.get('title')
        self.counter = data.get('counter')
        self.counter2 = data.get('counter2')
        self.echart1_data = data.get('echart1_data')
        self.echart2_data = data.get('echart2_data')
        self.echarts3_1_data = data.get('echarts3_1_data')
        self.echarts3_2_data = data.get('echarts3_2_data')
        self.echarts3_3_data = data.get('echarts3_3_data')
        self.echart4_data = data.get('echart4_data')
        self.echart5_data = data.get('echart5_data')
        self.echart6_data = data.get('echart6_data')
        self.map_1_data = data.get('map_1_data')

class JobData(SourceDataDemo):

    def __init__(self):
        """
        按照 SourceDataDemo 的格式覆盖数据即可
        """
        super().__init__()
        with open('job.json', 'r', encoding='utf-8') as f:
            data = json.loads(f.read())
        self.title = data.get('title')
        self.counter = data.get('counter')
        self.counter2 = data.get('counter2')
        self.echart1_data = data.get('echart1_data')
        self.echart2_data = data.get('echart2_data')
        self.echarts3_1_data = data.get('echarts3_1_data')
        self.echarts3_2_data = data.get('echarts3_2_data')
        self.echarts3_3_data = data.get('echarts3_3_data')
        self.echart4_data = data.get('echart4_data')
        self.echart5_data = data.get('echart5_data')
        self.echart6_data = data.get('echart6_data')
        self.map_1_data = data.get('map_1_data')