package com.tencent.bkrepo.opdata.service

import com.tencent.bkrepo.opdata.constant.OPDATA_CAP_SIZE
import com.tencent.bkrepo.opdata.constant.OPDATA_CUSTOM
import com.tencent.bkrepo.opdata.constant.OPDATA_CUSTOM_NUM
import com.tencent.bkrepo.opdata.constant.OPDATA_CUSTOM_SIZE
import com.tencent.bkrepo.opdata.constant.OPDATA_GRAFANA_NUMBER
import com.tencent.bkrepo.opdata.constant.OPDATA_GRAFANA_STRING
import com.tencent.bkrepo.opdata.constant.OPDATA_NODE_NUM
import com.tencent.bkrepo.opdata.constant.OPDATA_PIPELINE
import com.tencent.bkrepo.opdata.constant.OPDATA_PIPELINE_NUM
import com.tencent.bkrepo.opdata.constant.OPDATA_PIPELINE_SIZE
import com.tencent.bkrepo.opdata.constant.OPDATA_PROJECT_NUM
import com.tencent.bkrepo.opdata.constant.OPDATA_STAT_LIMIT
import com.tencent.bkrepo.opdata.constant.PROJECT_NAME
import com.tencent.bkrepo.opdata.model.ProjectModel
import com.tencent.bkrepo.opdata.model.TProjectMetrics
import com.tencent.bkrepo.opdata.pojo.Columns
import com.tencent.bkrepo.opdata.pojo.NodeResult
import com.tencent.bkrepo.opdata.pojo.QueryRequest
import com.tencent.bkrepo.opdata.pojo.QueryResult
import com.tencent.bkrepo.opdata.pojo.Target
import com.tencent.bkrepo.opdata.pojo.enums.Metrics
import com.tencent.bkrepo.opdata.repository.ProjectMetricsRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

@Service
class GrafanaService @Autowired constructor(
    private val projectModel: ProjectModel,
    private val projectMetricsRepository: ProjectMetricsRepository
) {
    fun search(): List<String> {
        var data = mutableListOf<String>()
        for (metric in Metrics.values()) {
            data.add(metric.name)
        }
        return data
    }

    fun query(request: QueryRequest): List<Any> {
        var result = mutableListOf<Any>()
        request.targets.forEach {
            when (it.target) {
                Metrics.PROJECTNUM -> {
                    dealProjectNum(it, result)
                }
                Metrics.PROJECLIST -> {
                    dealProjectList(it, result)
                }
                Metrics.REPOLIST -> {
                    dealProjectList(it, result)
                }
                Metrics.PROJECTNODENUM -> {
                    dealProjectNodeNum(result)
                }
                Metrics.PROJECTNODESIZE -> {
                    dealProjectNodeSize(result)
                }
                Metrics.CAPSIZE -> {
                    dealCapSize(it, result)
                }
                Metrics.NODENUM -> {
                    dealNodeNum(it, result)
                }
                else -> {
                    dealNodeNum(it, result)
                }
            }
        }
        return result
    }

    private fun dealProjectNum(target: Target, result: MutableList<Any>) {
        val count = projectModel.getProjectNum()
        val column = Columns(OPDATA_PROJECT_NUM, OPDATA_GRAFANA_NUMBER)
        val row = listOf(count)
        val data = QueryResult(listOf(column), listOf(row), target.type)
        result.add(data)
    }

    private fun dealCapSize(target: Target, result: MutableList<Any>) {
        var size = 0L
        val projects = projectMetricsRepository.findAll()
        projects.forEach {
            size += it.capSize
        }
        val column = Columns(OPDATA_CAP_SIZE, OPDATA_GRAFANA_NUMBER)
        val row = listOf(size)
        val data = QueryResult(listOf(column), listOf(row), target.type)
        result.add(data)
    }

    private fun dealNodeNum(target: Target, result: MutableList<Any>) {
        var num = 0L
        val projects = projectMetricsRepository.findAll()
        projects.forEach {
            num += it.nodeNum
        }
        val column = Columns(OPDATA_NODE_NUM, OPDATA_GRAFANA_NUMBER)
        val row = listOf(num)
        val data = QueryResult(listOf(column), listOf(row), target.type)
        result.add(data)
    }

    private fun dealProjectList(target: Target, result: MutableList<Any>) {
        var rows = mutableListOf<List<Any>>()
        var columns = mutableListOf<Columns>()
        val info = projectMetricsRepository.findAll()
        columns.add(Columns(TProjectMetrics::projectId.name, OPDATA_GRAFANA_STRING))
        columns.add(Columns(TProjectMetrics::nodeNum.name, OPDATA_GRAFANA_NUMBER))
        columns.add(Columns(TProjectMetrics::capSize.name, OPDATA_GRAFANA_NUMBER))
        columns.add(Columns(OPDATA_CUSTOM_NUM, OPDATA_GRAFANA_NUMBER))
        columns.add(Columns(OPDATA_CUSTOM_SIZE, OPDATA_GRAFANA_NUMBER))
        columns.add(Columns(OPDATA_PIPELINE_NUM, OPDATA_GRAFANA_NUMBER))
        columns.add(Columns(OPDATA_PIPELINE_SIZE, OPDATA_GRAFANA_NUMBER))
        info.forEach {
            var customNum = 0L
            var customSize = 0L
            var pipelineNum = 0L
            var pipelineSize = 0L
            it.repoMetrics.forEach {
                if (it.repoName == OPDATA_CUSTOM) {
                    customNum = it.num
                    customSize = it.size
                }
                if (it.repoName == OPDATA_PIPELINE) {
                    pipelineNum = it.num
                    pipelineSize = it.size
                }
            }
            val row = listOf(it.projectId, it.nodeNum, it.capSize, customNum, customSize, pipelineNum, pipelineSize)
            rows.add(row)
        }
        val data = QueryResult(columns, rows, target.type)
        result.add(data)
    }

    private fun dealProjectNodeSize(result: MutableList<Any>): List<Any> {
        val projects = projectMetricsRepository.findAll()
        var tmpMap = HashMap<String, Long>()
        projects.forEach {
            val projectId = it.projectId
            if (it.capSize != 0L) {
                tmpMap.put(projectId, it.capSize)
            }
        }
        return convToDisplayData(tmpMap, result)
    }

    private fun dealProjectNodeNum(result: MutableList<Any>): List<Any> {
        val projects = projectMetricsRepository.findAll()
        var tmpMap = HashMap<String, Long>()
        projects.forEach {
            val projectId = it.projectId
            if (it.nodeNum != 0L && projectId != PROJECT_NAME) {
                tmpMap.put(projectId, it.nodeNum)
            }
        }
        return convToDisplayData(tmpMap, result)
    }

    private fun convToDisplayData(mapData: HashMap<String, Long>, result: MutableList<Any>): List<Any> {
        mapData.toList().sortedByDescending { it.second }.subList(0, OPDATA_STAT_LIMIT).forEach {
            val projectId = it.first
            val data = listOf(it.second, System.currentTimeMillis())
            val element = listOf(data)
            if (it.second != 0L) {
                result.add(NodeResult(projectId, element))
            }
        }
        return result
    }
}
