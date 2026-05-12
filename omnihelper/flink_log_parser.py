"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
            http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.
"""
import os
from datetime import datetime
import urllib.parse
import pandas as pd

from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger
from omnihelper.util.flink_excel_util import FlinkExcelWriterWithStyle
from omnihelper.flink.flink_request import FlinkRequester
from omnihelper.flink.operator.op_parse import FlinkParser


class FlinkLogParser:
    EXECUTE_PATH = CommonUtil.get_execute_path()
    TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

    def __init__(self, args):
        self.args = args  # 存储命令行参数
        self.excel_writer = FlinkExcelWriterWithStyle()  # 用于表格写
        self.analysis_result = []  # 存储最终的分析结果，用于表格写入
        self.parser = FlinkParser()
        self.target_metrics = [
            ".numRecordsIn", ".numRecordsInPerSecond",
            ".numRecordsOut", ".numRecordsOutPerSecond",
            ".numBytesIn", ".numBytesInPerSecond",
            ".numBytesOut", ".numBytesOutPerSecond",
        ]
        self.args_valid = self._get_arguments()  # 解析命令行并校验参数
        if self.args_valid:
            self.print_arguments()  # 打印初始化页面及参数

    def _get_arguments(self):
        # 输出目录默认值
        if self.args.output_dir is None:
            self.args.output_dir = os.path.join(self.EXECUTE_PATH, "output")

        os.makedirs(self.args.output_dir, exist_ok=True)

        # 1. 校验 URL 参数
        if not self._validate_url():
            return False

        # 2. 校验数值参数
        if not self._validate_numeric_args():
            return False

        # 3. 校验其他参数
        if not self._validate_other_args():
            return False

        # 处理 SSL 验证参数
        self.args.ssl_verify = not getattr(self.args, 'no_ssl_verify', False)

        # 解析自定义请求头
        self.args.parsed_headers = self._parse_headers()
        if self.args.parsed_headers is None:
            return False

        return True

    def _validate_url(self):
        """校验 URL 参数"""
        url = self.args.url
        try:
            parsed_url = urllib.parse.urlparse(url)

            # 校验协议
            if not parsed_url.scheme:
                # 如果没有协议，直接报错
                print("Error: Invalid URL: missing scheme. Please include http:// or https://.")
                return False
            elif parsed_url.scheme not in ['http', 'https']:
                print(f"Error: Invalid URL scheme: {parsed_url.scheme}. Only http and https are supported.")
                return False

            # 校验主机名
            if not parsed_url.netloc:
                print("Error: Invalid URL: missing host and port.")
                return False

            # 提取 host, port, use_https 信息，保持向后兼容
            self.args.host = parsed_url.hostname
            self.args.port = parsed_url.port
            if self.args.port is None:
                self.args.port = 80 if parsed_url.scheme == 'http' else 443
            self.args.use_https = parsed_url.scheme == 'https'

            # 校验端口范围
            if self.args.port < 1 or self.args.port > 65535:
                print(f"Error: Invalid port: {self.args.port}. Port must be between 1 and 65535.")
                return False
            return True
        except Exception as e:
            print(f"Error: Invalid URL format: {e}. Please check your URL and try again.")
            return False

    def _validate_numeric_args(self):
        """校验数值参数"""
        # 校验 interval
        if not isinstance(self.args.interval, int) or self.args.interval < 0 or self.args.interval > 30000:
            print(
                f"Error: Invalid interval: {self.args.interval}. Interval must be an integer between 0 and 30000 (ms).")
            return False

        # 校验 timeout
        if not isinstance(self.args.timeout, int) or self.args.timeout < 1 or self.args.timeout > 300:
            print(f"Error: Invalid timeout: {self.args.timeout}. Timeout must be an integer between 1 and 300 (s).")
            return False
        return True

    def _validate_other_args(self):
        """校验其他参数"""
        # 校验 jobid
        if self.args.jobid:
            for jobid in self.args.jobid:
                if not jobid or not isinstance(jobid, str):
                    print("Error: Invalid jobid: jobid must be a non-empty string.")
                    return False

        # 校验 output_dir 路径格式（基本校验）
        if self.args.output_dir:
            if not isinstance(self.args.output_dir, str):
                print("Error: Invalid output_dir: must be a string.")
                return False
            # 确保路径可以创建
            try:
                os.makedirs(self.args.output_dir, exist_ok=True)
            except Exception as e:
                print(f"Error: Invalid output_dir: {e}.")
                return False
        return True

    def _parse_headers(self):
        """解析 --header 参数为字典"""
        raw_headers = getattr(self.args, 'header', None)
        if not raw_headers:
            return {}
        headers = {}
        for h in raw_headers:
            if ':' not in h:
                print(f"Error: Invalid header format: '{h}'. Expected 'Key: Value'.")
                return None
            key, _, value = h.partition(':')
            key = key.strip()
            value = value.strip()
            if not key:
                print(f"Error: Invalid header format: '{h}'. Header key cannot be empty.")
                return None
            headers[key] = value
        return headers

    def print_arguments(self):
        # 打印配置信息
        print("=" * 60)
        print("  Flink Log Analysis Tool")
        print("=" * 60)

        print(f"Flink Dashboard URL: {self.args.url}")
        print(f"API Call Interval: {self.args.interval} ms")
        print(f"API Call Timeout: {self.args.timeout} s")
        print(f"SSL Verify: {self.args.ssl_verify}")
        print(f"Kerberos Auth: {getattr(self.args, 'kerberos', False)}")
        if getattr(self.args, 'kerberos', False):
            print(f"Kerberos Mutual Auth: {getattr(self.args, 'kerberos_mutual_auth', 'OPTIONAL')}")
        print(f"Output Directory: {os.path.realpath(self.args.output_dir)}")
        print(f"Show Op Details: {self.args.show_op_details}")
        print("-" * 60)

    def fetch_metrics(self, jid, vid, metrics, batch_size=10):
        results = []
        for i in range(0, len(metrics), batch_size):
            batch = metrics[i:i + batch_size]
            metric_values = self.requester.get_vertex_metrics(jid, vid, batch) if batch else []
            if metric_values:
                results.extend(metric_values)
        return results

    def _get_job_ids(self, jids):
        if jids is not None:
            logger.info(f"Using provided job IDS: {jids}")
            return jids
        overview = self.requester.get_jobs_overview()
        if not overview:
            logger.warning("No jobs overview data received")
            return []
        all_jobs = overview.get('jobs', [])
        return [j['jid'] for j in all_jobs]

    def _process_job(self, jid):
        detail = self.requester.get_job_detail(jid)
        if not detail:
            logger.warning(f"Failed to get detail for job {jid}")
            return None
        plan = detail.get("plan", "")
        if not plan:
            logger.warning(f"Failed to get plan for job {jid}")
            return None
        job_name = detail.get('name', 'Unknown')
        plan_nodes = {node['id']: node for node in plan.get('nodes', [])}
        vertices = {
            vertex["id"]: self._process_vertex(vertex["id"], vertex, plan_nodes, jid, detail)
            for vertex in detail.get("vertices", [])
        }
        return {
            "job_name": job_name,
            "vertices": {k: v for k, v in vertices.items() if v is not None}
        }

    def _process_vertex(self, vid, vertex, plan_nodes, jid, detail):
        metrics = self._get_vertex_metrics(vid, jid)
        if not metrics:
            return None
        stats = self.parser.parse_performance_stats(vid, metrics["values"],
                                                    self.parser.get_description(detail, jid))
        description = plan_nodes.get(vid, {}).get('description', '')
        op_chain = self.parser.parse_operator_chain(description, stats)

        return {
            "status": vertex.get('status'),
            "vertex_name": vertex.get("name"),
            "operator_chain": op_chain,
            "logic_metadata": {
                "full_description": description
            },
            "summary_metrics": stats
        }

    def _get_vertex_metrics(self, vid, jid):
        available = self.requester.get_vertex_metrics(jid, vid)
        if not available:
            logger.warning(f"No metrics available for vertex {vid} in job {jid}")
            return None
        needed_ids = self.parser.filter_num_data(available, self.target_metrics)
        return {
            "ids": needed_ids,
            "values": self.fetch_metrics(jid, vid, needed_ids) if needed_ids else []
        }

    def analyze_flink_logs(self):
        """
        实现 Flink 日志分析的核心功能
        """
        if not self.args_valid:
            print("Error: Invalid arguments. Please check your input and try again.")
            return

        self.requester = FlinkRequester(
            url=self.args.url,
            timeout=self.args.timeout,
            ssl_verify=self.args.ssl_verify,
            interval=self.args.interval,
            kerberos=getattr(self.args, 'kerberos', False),
            kerberos_mutual_auth=getattr(self.args, 'kerberos_mutual_auth', 'OPTIONAL'),
            headers=getattr(self.args, 'parsed_headers', {}),
        )

        full_report = {}
        jobs_ids = self._get_job_ids(self.args.jobid)
        for jid in jobs_ids:
            job_data = self._process_job(jid)
            if job_data:
                full_report[jid] = job_data

        self.analysis_result = self.parser.parse_job_data(full_report)
        logger.info(f"Generated report with {len(self.analysis_result)}")

    def generate_report(self):
        """
        生成分析报告
        """
        if not self.analysis_result:
            print("Result is empty, No data to display.")
            return

        # 定义列顺序，确保与表头配置一致
        columns = [
            'jobid', 'taskid', '状态',
            '算子名称', 'Input', 'Output', '出现频次', '运行时间(s)', '输入数据量', '输出数据量',
            '表达式/内置函数名称', 'Input', '嵌套内容', '出现频次'
        ]

        # 处理重复列名的情况
        # 为第二个 Input 和 出现频次 列添加临时后缀
        temp_columns = [
            'jobid', 'taskid', '状态',
            '算子名称', 'Input', 'Output', '出现频次', '运行时间(s)', '输入数据量', '输出数据量',
            '表达式/内置函数名称', 'Input_2', '嵌套内容', '出现频次_2'
        ]

        # 创建一个新的列表，将数据中的 '表达式Input' 和 '表达式出现频次' 映射到临时列名
        processed_data = []
        for item in self.analysis_result:
            processed_item = {
                'jobid': item.get('jobid'),
                'taskid': item.get('taskid'),
                '状态': item.get('状态'),
                '算子名称': item.get('算子名称'),
                'Input': item.get('Input'),
                'Output': item.get('Output'),
                '出现频次': item.get('出现频次'),
                '运行时间(s)': item.get('运行时间(s)'),
                '输入数据量': item.get('输入数据量'),
                '输出数据量': item.get('输出数据量'),
                '表达式/内置函数名称': item.get('表达式/内置函数名称'),
                'Input_2': item.get('表达式Input'),
                '嵌套内容': item.get('嵌套内容'),
                '出现频次_2': item.get('表达式出现频次')
            }
            processed_data.append(processed_item)

        # 创建 DataFrame
        df = pd.DataFrame(processed_data, columns=temp_columns)

        # 重命名列，将临时后缀去掉，实现重复列名
        df.columns = columns

        output_excel_path = os.path.join(self.args.output_dir, f"Omni_Analysis_All_Report_{self.TIMESTAMP}.xlsx")
        self.excel_writer.write_to_excel(df, output_excel_path)
