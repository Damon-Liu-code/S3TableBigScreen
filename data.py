from pyspark.sql import SparkSession

class SourceDataDemo:

    def __init__(self, spark):
        self.spark = spark

        # 从 Spark SQL 查询中读取数据
        try:
            df = self.spark.sql("SELECT * FROM s3tablesbucket.testdb.map_data_0119")
            rows = df.collect()  # 将数据收集为一个列表
            data_list = [row.asDict() for row in rows]
        except Exception as e:
            print(f"Error fetching data from map_data_0119: {e}")
            data_list = []

        # 数据分类和构建
        self.title = '大数据可视化展板通用模板'  # 如果标题固定，可以直接赋值
        self.counter = {'name': '2018年总收入情况', 'value': 12581189}
        self.counter2 = {'name': '2018年总支出情况', 'value': 3912410}

        # 初始化各个图表的数据字典
        self.echart1_data = {'title': '', 'data': []}
        self.echart2_data = {'title': '', 'data': []}
        self.echarts3_1_data = {'title': '', 'data': []}
        self.echarts3_2_data = {'title': '', 'data': []}
        self.echarts3_3_data = {'title': '', 'data': []}
        self.echart4_data = {'title': '', 'data': [], 'xAxis': []}
        self.echart5_data = {'title': '', 'data': []}
        self.echart6_data = {'title': '', 'data': []}
        self.map_1_data = {'symbolSize': 100, 'data': []}

        # 遍历数据列表，将数据分类并构造相应的数据结构
        for item in data_list:
            chart_type = item.get('chart_type')

            if chart_type == 'echart1':
                # 行业分布
                self.echart1_data['title'] = item.get('chart_title', '行业分布')
                self.echart1_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            elif chart_type == 'echart2':
                # 省份分布
                self.echart2_data['title'] = item.get('chart_title', '省份分布')
                self.echart2_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            elif chart_type == 'echarts3_1':
                # 年龄分布
                self.echarts3_1_data['title'] = item.get('chart_title', '年龄分布')
                self.echarts3_1_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            elif chart_type == 'echarts3_2':
                # 职业分布
                self.echarts3_2_data['title'] = item.get('chart_title', '职业分布')
                self.echarts3_2_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            elif chart_type == 'echarts3_3':
                # 兴趣分布
                self.echarts3_3_data['title'] = item.get('chart_title', '兴趣分布')
                self.echarts3_3_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            elif chart_type == 'echart4':
                # 时间趋势
                self.echart4_data['title'] = item.get('chart_title', '时间趋势')
                # 处理 xAxis（仅需一次）
                if not self.echart4_data['xAxis'] and item.get('x_axis'):
                    self.echart4_data['xAxis'] = item.get('x_axis').split(',')

                # 处理 data
                value_list = item.get('value_list', '').split(',')
                value_list = [int(v) for v in value_list if v]  # 转换为整数列表
                self.echart4_data['data'].append({
                    'name': item.get('name'),
                    'value': value_list
                })

            elif chart_type == 'echart5':
                # 省份TOP
                self.echart5_data['title'] = item.get('chart_title', '省份TOP')
                self.echart5_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            elif chart_type == 'echart6':
                # 一线城市情况
                self.echart6_data['title'] = item.get('chart_title', '一线城市情况')
                self.echart6_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value'),
                    'value2': item.get('value2'),
                    'color': item.get('color'),
                    'radius': [item.get('radius_start'), item.get('radius_end')]
                })

            elif chart_type == 'map_1':
                # 地图数据
                self.map_1_data['symbolSize'] = item.get('symbol_size', 100)
                self.map_1_data['data'].append({
                    'name': item.get('name'),
                    'value': item.get('value')
                })

            # 根据需要，可以添加更多的图表类型处理

    # 属性方法保持不变

    @property
    def echart1(self):
        data = self.echart1_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
            'series': [i.get('value') for i in data.get('data')]
        }
        return echart

    @property
    def echart2(self):
        data = self.echart2_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
            'series': [i.get('value') for i in data.get('data')]
        }
        return echart

    @property
    def echarts3_1(self):
        data = self.echarts3_1_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echarts3_2(self):
        data = self.echarts3_2_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echarts3_3(self):
        data = self.echarts3_3_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echart4(self):
        data = self.echart4_data
        echart = {
            'title': data.get('title'),
            'names': [i.get('name') for i in data.get('data')],
            'xAxis': data.get('xAxis'),
            'data': data.get('data'),
        }
        return echart

    @property
    def echart5(self):
        data = self.echart5_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
            'series': [i.get('value') for i in data.get('data')],
            'data': data.get('data'),
        }
        return echart

    @property
    def echart6(self):
        data = self.echart6_data
        echart = {
            'title': data.get('title'),
            'xAxis': [i.get('name') for i in data.get('data')],
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

    def __init__(self, spark):
        super().__init__(spark)
