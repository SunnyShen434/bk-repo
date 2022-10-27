/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2022 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bkrepo.conan.service.impl

import com.tencent.bkrepo.common.artifact.api.ArtifactFile
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactContextHolder
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactDownloadContext
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactUploadContext
import com.tencent.bkrepo.conan.pojo.artifact.ConanArtifactInfo
import com.tencent.bkrepo.conan.service.ConanUploadDownloadService
import com.tencent.bkrepo.conan.utils.PathUtils.generateFullPath
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/**
 * conan文件上传 下载
 */
@Service
class ConanUploadDownloadServiceImpl : ConanUploadDownloadService {

    @Autowired
    lateinit var commonService: CommonService

    override fun uploadFile(conanArtifactInfo: ConanArtifactInfo, artifactFile: ArtifactFile) {
        if (artifactFile.getSize() != 0L) {
            val context = ArtifactUploadContext(artifactFile)
            ArtifactContextHolder.getRepository().upload(context)
        } else {
            // conan客户端上传文件前会使用同样请求去确认文件是否存在
            val fullPath = generateFullPath(conanArtifactInfo)
            commonService.checkNodeExist(conanArtifactInfo.projectId, conanArtifactInfo.repoName, fullPath)
        }
    }

    override fun downloadFile(conanArtifactInfo: ConanArtifactInfo) {
        val context = ArtifactDownloadContext()
        ArtifactContextHolder.getRepository().download(context)
    }
}
