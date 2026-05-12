"""
   Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
   You can use this software according to the terms and conditions of the Mulan PSL v2.
   You may obtain a copy of Mulan PSL v2 at:
              http://license.coscl.org.cn/MulanPSL2
   THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
   EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
   MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
   See the Mulan PSL v2 for more details.

函数解析模块 - 负责从 Flink 物理计划中解析函数调用
"""

import json
import re
import os
from collections import defaultdict
from json import JSONDecodeError
from omnihelper.flink.function.function_parse import FlinkFunctionParser
from omnihelper.util.common_util import CommonUtil
from omnihelper.util.log import logger


class FlinkParser:
    """
    Parser Layer: Logic & Data Transformation
    """

    def __init__(self):
        self.function_parser = FlinkFunctionParser()
        self.op_dictionary = {}
        self.dictionary_path = os.path.join(self.get_resource_path(), "flink_op_dictionary.json")
        self._load_op_dictionary()

    @staticmethod
    def get_resource_path():
        """获取资源文件路径"""
        return os.path.join(CommonUtil.get_execute_path(), "resources")

    @staticmethod
    def parse_single_description_line(line):
        """处理单行描述的清理与解析"""
        if not line:
            return None
        clean_part = line.strip(" :+- \t")
        if not clean_part:
            return None

        if clean_part.startswith(("{", "[")):
            try:
                return json.loads(clean_part)
            except (JSONDecodeError, TypeError) as e:
                logger.debug(f"Failed to parse JSON: {e}")
                pass
        return clean_part

    @staticmethod
    def get_description(job_detail, job_id):
        plan = job_detail.get("plan", {})
        if not isinstance(plan, dict) or "nodes" not in plan:
            return {job_id: {}}

        vertex_map = {}
        for node in plan.get("nodes", []):
            vertex_id = node.get("id")
            if not vertex_id:
                continue
            description = node.get("description", "")
            if not description:
                continue
            raw_parts = re.split(r"<br/>|\n", description)
            description_data = [
                parsed_line for line in raw_parts
                if (parsed_line := FlinkParser.parse_single_description_line(line)) is not None
            ]

            vertex_map[vertex_id] = {"plan_desc": description_data}

        return {job_id: vertex_map}

    @staticmethod
    def filter_num_data(available, target_metrics):
        if not available:
            return []
        needed_ids = [m['id'] for m in available if
                      any(m['id'].endswith(s) for s in target_metrics)] if available else []
        return needed_ids

    @staticmethod
    def operator_analysis(jobs, metrics):
        op_pattern = r"\[(\d+)\]:([A-Za-z]+)"
        ops = {
            m.group(1): {"type": m.group(2), "vertex": vertex_id, "job": job_id}
            for job_id, vertices in jobs.items()
            for vertex_id, vertex in vertices.items()
            for desc in vertex["plan_desc"]
            if (m := re.match(op_pattern, desc))
        }

        metric_pattern = r"(\d+)\.([A-Za-z_]+)\[(\d+)\]\.(\w+)"
        agg = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for vertex_id, vertex_metrics in metrics.items():
            for key, value in vertex_metrics.items():
                if m := re.match(metric_pattern, key):
                    _, _, op_id, metric = m.groups()

                    agg[vertex_id][op_id][metric].append(float(value))
        out_put = {}
        for vertex_id, ops_metrics in agg.items():
            job_id = None
            for op_id, info in ops.items():
                if info["vertex"] == vertex_id:
                    job_id = info["job"]
                    break
            if job_id is None:
                continue
            out_put.setdefault(job_id, {})
            out_put[job_id].setdefault(vertex_id, [])

            for op_id, metrics_dict in ops_metrics.items():
                op_type = ops[op_id]["type"]
                out_put[job_id][vertex_id].append({
                    "op_id": int(op_id),
                    "op_type": op_type,
                    "metrics": {metric: sum(vals) for metric, vals in metrics_dict.items()}
                })
        return out_put

    @staticmethod
    def safe_float(val):
        if not val:
            return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def group_metrics_by_operator(raw_map):
        """把原始 metrics 按算子分组"""
        operator_stats = {}
        for full_id, val in raw_map.items():
            parts = full_id.split(".")
            if len(parts) < 2:
                continue
            operator = ".".join(parts[:-1])
            metric = parts[-1]
            if operator not in operator_stats:
                operator_stats[operator] = {
                    "numRecordsIn": 0,
                    "numRecordsInPerSecond": 0.0,
                    "numRecordsOut": 0,
                    "numRecordsOutPerSecond": 0.0,
                    "numBytesIn": 0,
                    "numBytesInPerSecond": 0.0,
                    "numBytesOut": 0,
                    "numBytesOutPerSecond": 0.0,
                }
            if metric in operator_stats[operator]:
                val_f = FlinkParser.safe_float(val)
                if "PerSecond" in metric:
                    operator_stats[operator][metric] = round(val_f, 2)
                else:
                    operator_stats[operator][metric] = int(val_f)
        return operator_stats

    @staticmethod
    def calc_active_duration(operator_stats):
        """计算每个算子的活跃时长"""
        for op, stats in operator_stats.items():
            rps_in = stats["numRecordsInPerSecond"]
            cnt_in = stats["numRecordsIn"]
            stats["active_duration_in"] = round(cnt_in / rps_in, 2) if rps_in > 0 else 0.0

            cnt_out = stats["numRecordsOut"]
            rps_out = stats["numRecordsOutPerSecond"]
            stats["active_duration_out"] = round(cnt_out / rps_out, 2) if rps_out > 0 else 0.0
        return operator_stats

    @staticmethod
    def calc_summary(operator_stats):
        """汇总整体"""
        return {
            "totalRecordsIn": sum(stats["numRecordsIn"] for stats in operator_stats.values()),
            "totalRecordsOut": sum(stats["numRecordsOut"] for stats in operator_stats.values()),
            "totalBytesIn": sum(stats["numBytesIn"] for stats in operator_stats.values()),
            "totalBytesOut": sum(stats["numBytesOut"] for stats in operator_stats.values()),
            "avgRecordsInPerSecond": round(sum(stats["numRecordsInPerSecond"] for stats in operator_stats.values())),
            "avgRecordsOutPerSecond": round(sum(stats["numRecordsOutPerSecond"] for stats in operator_stats.values())),
        }

    @staticmethod
    def restructure_by_op_type(analysis):
        """把operator_analysis 的结果op_type 重组"""
        operators_by_type = {}
        for job_id, vertices in analysis.items():
            for vertex_id, ops_list in vertices.items():
                for op in ops_list:
                    op_type = op["op_type"]
                    operators_by_type.setdefault(op_type, [])
                    operators_by_type[op_type].append({
                        "op_id": op["op_id"],
                        "metrics": op["metrics"]
                    })
        return operators_by_type

    @staticmethod
    def aggregate_metrics(op_list):
        """聚合同类算子的指标"""
        num_in = sum(op["metrics"].get("numRecordsIn", 0) for op in op_list)
        num_in_sec = sum(op["metrics"].get("numRecordsInPerSecond", 0.0) for op in op_list)
        num_out = sum(op["metrics"].get("numRecordsOut", 0) for op in op_list)
        num_out_sec = sum(op["metrics"].get("numRecordsOutPerSecond", 0.0) for op in op_list)
        return num_in, num_in_sec, num_out, num_out_sec

    @staticmethod
    def compute_runtime(num_in, num_in_sec, num_out, num_out_sec):
        run_time = 0.0
        if num_in_sec > 0:
            run_time += num_in / num_in_sec
        if num_out_sec > 0:
            run_time += num_out / num_out_sec
        return round(run_time, 2)

    @staticmethod
    def bytes_to_mb(value):
        """字节计算"""
        if not value:
            return 0.0
        return round(value / (1024 * 1024), 2)

    @staticmethod
    def parse_performance_stats(vid, metrics_raw, jobs=None):
        """Processes raw metrics into structured inbound/outbound stats."""
        if not metrics_raw:
            return {"operators": {}, "summary": {}, "analysis": {}}

        raw_map = {item['id']: item['value'] for item in metrics_raw}
        operator_stats = FlinkParser.group_metrics_by_operator(raw_map)
        operator_stats = FlinkParser.calc_active_duration(operator_stats)
        summary = FlinkParser.calc_summary(operator_stats)
        analysis = {}
        operators_by_type = {}
        if jobs is not None:
            analysis = FlinkParser.operator_analysis(jobs, {vid: raw_map})
            operators_by_type = FlinkParser.restructure_by_op_type(analysis)

        return {
            "operators": operators_by_type,
            "summary": summary,
            "analysis": analysis
        }

    def parse_operator_chain(self, description, stats=None):
        """
        解析运算符链字符串
        :param description: 运算符链字符串，如 "Map -> Calc -> Sink"
        :param stats: 统计信息（可选参数，为保持接口兼容）
        :return: 运算符列表
        """
        return self.function_parser.parse_operator_chain(description)

    def _analyze_operators_functions(self, description, task_id):
        """
        分析描述中的函数使用情况
        只输出 is_support_func: false 的函数（不支持的函数）
        :param description: 算子描述字符串（包含函数信息）
        :param task_id: 任务ID
        :return: 函数分析结果列表
        """
        if not description:
            return []

        # 直接调用新模块的方法
        unsupported_funcs = self.function_parser.analyze_unsupported_functions(description)

        # 转换为标准格式
        results = []
        for func in unsupported_funcs:
            results.append({
                "func_name": func["func_name"],
                "task_id": task_id,
                "input": [""],
                "times": func["times"]
            })

        return results

    def _load_op_dictionary(self):
        try:
            with open(self.dictionary_path, "r", encoding="utf-8") as f:
                self.op_dictionary = json.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Op dictionary file not found: {self.dictionary_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON format in dictionary file: {self.dictionary_path}")
        except Exception as e:
            raise Exception(f"Unexpected error while loading dictionary file: {self.dictionary_path}, error: {e}")

    def parse_job_data(self, json_data):
        """
        解析作业数据，输出格式与 flink_log_parser.py 完全兼容
        """
        result = []
        for job_id, job_info in json_data.items():
            self._process_job(job_id, job_info, result)
        return result

    def _process_job(self, job_id, job_info, result):
        vertices = job_info.get("vertices")
        if not vertices:
            return
        for task_id, task_info in vertices.items():
            self._process_task(job_id, task_id, task_info, result)

    def _process_task(self, job_id, task_id, task_info, result):
        status = task_info.get("status", "UNKNOWN")
        operators_metrics = task_info.get("summary_metrics", {}).get("operators", {})
        logic_metadata = task_info.get("logic_metadata", {})
        full_description = logic_metadata.get("full_description", "")

        # 收集所有有效算子（跳过 op_dictionary 中的类型）
        ops = []
        for op_type, op_list in operators_metrics.items():
            if op_type in self.op_dictionary:
                continue
            num_in, num_in_sec, num_out, num_out_sec = FlinkParser.aggregate_metrics(op_list)
            run_time = FlinkParser.compute_runtime(num_in, num_in_sec, num_out, num_out_sec)
            ops.append({
                "op_type": op_type,
                "num_in": num_in,
                "num_out": num_out,
                "run_time": run_time,
                "count": len(op_list)
            })

        # 分析函数并按名称聚合
        func_analysis = self._analyze_operators_functions(full_description, task_id)
        aggregated = {}
        for func_info in func_analysis:
            name = func_info.get("func_name")
            if not name:
                continue
            if name not in aggregated:
                aggregated[name] = {"count": 0, "inputs": set()}
            aggregated[name]["count"] += func_info.get("times", 0)
            aggregated[name]["inputs"].update(func_info.get("input", []))

        func_list = list(aggregated.items())

        # 无数据直接返回
        if not ops and not func_list:
            return

        # 公共行构建函数：根据 op 或 func 组合字段
        def _make_row(op=None, func_name="", func_inputs_str="", func_count=""):
            return {
                'jobid': job_id,
                'taskid': task_id,
                '状态': status,
                '算子名称': op["op_type"] if op else "",
                'Input': op["num_in"] if op else "",
                'Output': op["num_out"] if op else "",
                '出现频次': op["count"] if op else "",
                '运行时间(s)': op["run_time"] if op else "",
                '输入数据量': f"{FlinkParser.bytes_to_mb(op['num_in'])}MB" if op else "",
                '输出数据量': f"{FlinkParser.bytes_to_mb(op['num_out'])}MB" if op else "",
                '表达式/内置函数名称': func_name,
                '表达式Input': func_inputs_str,
                '嵌套内容': "",
                '表达式出现频次': func_count
            }

        # 1) 输出所有算子行
        for op in ops:
            result.append(_make_row(op=op))

        # 2) 输出所有函数行
        for func_name, func_data in func_list:
            inputs_str = ",".join(func_data["inputs"]) if func_data["inputs"] else ""
            result.append(_make_row(op=None, func_name=func_name,
                                    func_inputs_str=inputs_str, func_count=func_data["count"]))
