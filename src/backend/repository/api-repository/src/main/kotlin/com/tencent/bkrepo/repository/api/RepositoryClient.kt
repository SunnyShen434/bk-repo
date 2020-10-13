package com.tencent.bkrepo.repository.api

import com.tencent.bkrepo.common.api.pojo.Page
import com.tencent.bkrepo.common.api.pojo.Response
import com.tencent.bkrepo.repository.constant.SERVICE_NAME
import com.tencent.bkrepo.repository.pojo.repo.RepoCreateRequest
import com.tencent.bkrepo.repository.pojo.repo.RepoDeleteRequest
import com.tencent.bkrepo.repository.pojo.repo.RepoUpdateRequest
import com.tencent.bkrepo.repository.pojo.repo.RepositoryInfo
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import org.springframework.cloud.openfeign.FeignClient
import org.springframework.context.annotation.Primary
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping

/**
 * 仓库服务接口
 */
@Api("仓库服务接口")
@Primary
@FeignClient(SERVICE_NAME, contextId = "RepositoryResource")
@RequestMapping("/service/repo")
interface RepositoryClient {

    @ApiOperation("根据名称类型查询仓库")
    @GetMapping("/query/{projectId}/{name}/{type}")
    fun detail(
        @ApiParam(value = "所属项目", required = true)
        @PathVariable projectId: String,
        @ApiParam(value = "仓库名称", required = true)
        @PathVariable name: String,
        @ApiParam(value = "仓库类型", required = true)
        @PathVariable type: String
    ): Response<RepositoryInfo?>

    @ApiOperation("根据名称查询仓库")
    @GetMapping("/query/{projectId}/{name}")
    fun detail(
        @ApiParam(value = "所属项目", required = true)
        @PathVariable projectId: String,
        @ApiParam(value = "仓库名称", required = true)
        @PathVariable name: String
    ): Response<RepositoryInfo?>

    @ApiOperation("列表查询项目所有仓库")
    @GetMapping("/list/{projectId}")
    fun list(
        @ApiParam(value = "项目id", required = true)
        @PathVariable projectId: String
    ): Response<List<RepositoryInfo>>

    @ApiOperation("分页查询项目所有仓库")
    @GetMapping("/page/{page}/{size}/{projectId}")
    fun page(
        @ApiParam(value = "当前页", required = true, example = "0")
        @PathVariable page: Int,
        @ApiParam(value = "分页大小", required = true, example = "20")
        @PathVariable size: Int,
        @ApiParam(value = "项目id", required = true)
        @PathVariable projectId: String
    ): Response<Page<RepositoryInfo>>

    @ApiOperation("创建仓库")
    @PostMapping
    fun create(
        @RequestBody repoCreateRequest: RepoCreateRequest
    ): Response<RepositoryInfo>

    @ApiOperation("修改仓库")
    @PutMapping
    fun update(
        @RequestBody repoUpdateRequest: RepoUpdateRequest
    ): Response<Void>

    @ApiOperation("删除仓库")
    @DeleteMapping
    fun delete(
        @RequestBody repoDeleteRequest: RepoDeleteRequest
    ): Response<Void>

    @ApiOperation("分页查询指定类型仓库")
    @GetMapping("/page/repoType/{page}/{size}/{repoType}")
    fun pageByType(
            @ApiParam(value = "当前页", required = true, example = "0")
            @PathVariable page: Int,
            @ApiParam(value = "分页大小", required = true, example = "20")
            @PathVariable size: Int,
            @ApiParam(value = "仓库类型", required = true)
            @PathVariable repoType: String
    ): Response<Page<RepositoryInfo>>
}
