package com.tencent.bkrepo.opdata.resource

import com.tencent.bkrepo.common.api.util.JsonUtils
import com.tencent.bkrepo.opdata.api.Grafana
import com.tencent.bkrepo.opdata.pojo.QueryRequest
import com.tencent.bkrepo.opdata.service.GrafanaService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController

@RestController
class GrafanaImpl @Autowired constructor(
    private val grafanaService: GrafanaService
) : Grafana {
    override fun ping(): ResponseEntity<Any> {
        return ResponseEntity.ok().header("Content-Type", "application/json").body("{}")
    }

    override fun search(): ResponseEntity<Any> {
        val result = grafanaService.search()
        return ResponseEntity.ok().header("Content-Type", "application/json").body(result)
    }

    override fun query(request: QueryRequest): ResponseEntity<Any> {
        var result = grafanaService.query(request)
        val response = JsonUtils.objectMapper.writeValueAsString(result)
        return ResponseEntity.ok().header("Content-Type", "application/json").body(response)
    }

    override fun annotations(): ResponseEntity<Any> {
        return ResponseEntity.ok().header("Content-Type", "application/json").body("{}")
    }
}