package com.tencent.bkrepo.generic.api

import com.tencent.bkrepo.common.api.pojo.Response
import com.tencent.bkrepo.common.artifact.api.ArtifactFile
import com.tencent.bkrepo.common.artifact.api.ArtifactInfo
import com.tencent.bkrepo.common.artifact.api.ArtifactPathVariable
import com.tencent.bkrepo.common.artifact.api.ArtifactPathVariable.Companion.ARTIFACT_COORDINATE_URI
import com.tencent.bkrepo.generic.pojo.BlockInfo
import com.tencent.bkrepo.generic.pojo.upload.UploadCompleteRequest
import com.tencent.bkrepo.generic.pojo.upload.UploadTransactionInfo
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestAttribute
import org.springframework.web.bind.annotation.RequestMapping

/**
 * 上传接口
 *
 * @author: carrypan
 * @date: 2019-09-27
 */
@Api("上传接口")
@RequestMapping
interface UploadResource {

    @ApiOperation("简单上传")
    @PutMapping(ARTIFACT_COORDINATE_URI)
    fun simpleUpload(
        @RequestAttribute
        userId: String,
        @ArtifactPathVariable
        artifactInfo: ArtifactInfo,
        file: ArtifactFile
    ): Response<Void>

    @ApiOperation("分块上传预检")
    @PostMapping("/precheck/$ARTIFACT_COORDINATE_URI")
    fun preCheck(
        @RequestAttribute
        userId: String,
        @ArtifactPathVariable
        artifactInfo: ArtifactInfo
    ): Response<UploadTransactionInfo>

    @ApiOperation("分块上传")
    @PutMapping("/block/{uploadId}/{sequence}")
    fun blockUpload(
        @RequestAttribute
        userId: String,
        @ApiParam("分块上传事物id", required = true)
        @PathVariable
        uploadId: String,
        @ApiParam("分块序号，从1开始", required = true)
        @PathVariable
        sequence: Int,
        file: ArtifactFile
    ): Response<Void>

    @ApiOperation("取消分块上传")
    @DeleteMapping("/abort/{uploadId}")
    fun abortUpload(
        @RequestAttribute
        userId: String,
        @ApiParam("上传事物id", required = true)
        @PathVariable
        uploadId: String
    ): Response<Void>

    @ApiOperation("完成分块上传")
    @PostMapping("/complete/{uploadId}")
    fun completeUpload(
        @RequestAttribute
        userId: String,
        @ApiParam("上传事物id", required = true)
        @PathVariable
        uploadId: String,
        request: UploadCompleteRequest
    ): Response<Void>

    @ApiOperation("查询上传分块")
    @GetMapping("/block/{uploadId}")
    fun getUploadedBlockList(
        @RequestAttribute
        userId: String,
        @ApiParam("上传事物id", required = true)
        @PathVariable
        uploadId: String
    ): Response<List<BlockInfo>>

}
