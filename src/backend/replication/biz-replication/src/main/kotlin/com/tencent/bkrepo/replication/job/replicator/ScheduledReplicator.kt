/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.tencent.bkrepo.replication.job.replicator

import com.tencent.bkrepo.common.artifact.path.PathUtils
import com.tencent.bkrepo.common.artifact.pojo.RepositoryType
import com.tencent.bkrepo.replication.config.DEFAULT_VERSION
import com.tencent.bkrepo.replication.config.ReplicationProperties
import com.tencent.bkrepo.replication.job.ReplicaContext
import com.tencent.bkrepo.replication.job.ReplicaExecutionContext
import com.tencent.bkrepo.replication.manager.LocalDataManager
import com.tencent.bkrepo.replication.pojo.record.ExecutionResult
import com.tencent.bkrepo.replication.pojo.record.ExecutionStatus
import com.tencent.bkrepo.replication.pojo.record.request.RecordDetailInitialRequest
import com.tencent.bkrepo.replication.pojo.task.objects.PackageConstraint
import com.tencent.bkrepo.replication.pojo.task.objects.PathConstraint
import com.tencent.bkrepo.replication.pojo.task.setting.ErrorStrategy
import com.tencent.bkrepo.replication.service.ReplicaRecordService
import com.tencent.bkrepo.repository.pojo.node.NodeInfo
import com.tencent.bkrepo.repository.pojo.packages.PackageListOption
import com.tencent.bkrepo.repository.pojo.packages.PackageSummary
import com.tencent.bkrepo.repository.pojo.packages.PackageVersion
import com.tencent.bkrepo.repository.pojo.packages.VersionListOption
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value

/**
 * 调度类任务同步器
 * 一次replica执行负责一个任务下的一个集群，在子线程中执行
 */
@Suppress("TooGenericExceptionCaught")
abstract class ScheduledReplicator : Replicator {

    @Value("\${spring.application.version}")
    private var version: String = DEFAULT_VERSION

    @Autowired
    private lateinit var replicaRecordService: ReplicaRecordService

    @Autowired
    protected lateinit var localDataManager: LocalDataManager

    @Autowired
    protected lateinit var replicationProperties: ReplicationProperties

    override fun replica(context: ReplicaContext) {
        with(context) {
            // 检查版本
            checkVersion(this)
            // 同步项目
            replicaProject(this)
            // 同步仓库
            replicaRepo(this)
            // 按仓库同步
            if (includeAllData(this)) {
                replicaByRepo(this)
                return
            }
            // 按包同步
            taskObject.packageConstraints.orEmpty().forEach {
                replicaByPackageConstraint(this, it)
            }
            // 按路径同步
            taskObject.pathConstraints.orEmpty().forEach {
                replicaByPathConstraint(this, it)
            }
        }
    }

    /**
     * 校验和远程集群版本是否一致
     */
    protected open fun checkVersion(context: ReplicaContext) {
        with(context) {
            val remoteVersion = artifactReplicaClient.version().data.orEmpty()
            if (version != remoteVersion) {
                logger.warn("Local cluster's version[$version] is different from remote cluster[$remoteVersion].")
            }
        }
    }

    /**
     * 同步整个仓库数据
     */
    private fun replicaByRepo(replicaContext: ReplicaContext) {
        val context = initialExecutionContext(replicaContext)
        try {
            if (replicaContext.taskObject.repoType == RepositoryType.GENERIC) {
                // 同步generic节点
                val root = localDataManager.findNodeDetail(
                    projectId = replicaContext.localProjectId,
                    repoName = replicaContext.localRepoName,
                    fullPath = PathUtils.ROOT
                ).nodeInfo
                replicaByPath(context, root)
                return
            }
            // 同步包
            val option = PackageListOption(pageNumber = 1, pageSize = PAGE_SIZE)
            var packages = localDataManager.listPackagePage(
                projectId = replicaContext.localProjectId,
                repoName = replicaContext.localRepoName,
                option = option
            )
            while (packages.isNotEmpty()) {
                packages.forEach {
                    replicaByPackage(context, it)
                }
                option.pageNumber += 1
                packages = localDataManager.listPackagePage(
                    projectId = replicaContext.localProjectId,
                    repoName = replicaContext.localRepoName,
                    option = option
                )
            }
        } catch (throwable: Throwable) {
            setErrorStatus(context, throwable)
        } finally {
            completeRecordDetail(context)
        }
    }

    /**
     * 同步指定包的数据
     */
    private fun replicaByPackageConstraint(replicaContext: ReplicaContext, constraint: PackageConstraint) {
        val context = initialExecutionContext(replicaContext)
        try {
            // 查询本地包信息
            val packageSummary = localDataManager.findPackageByKey(
                projectId = replicaContext.localProjectId,
                repoName = replicaContext.taskObject.localRepoName,
                packageKey = constraint.packageKey
            )
            replicaByPackage(context, packageSummary, constraint.versions)
        } catch (throwable: Throwable) {
            setErrorStatus(context, throwable)
        } finally {
            completeRecordDetail(context)
        }
    }

    /**
     * 同步指定路径的数据
     */
    private fun replicaByPathConstraint(replicaContext: ReplicaContext, constraint: PathConstraint) {
        val context = initialExecutionContext(replicaContext)
        try {
            val nodeInfo = localDataManager.findNodeDetail(
                projectId = replicaContext.localProjectId,
                repoName = replicaContext.localRepoName,
                fullPath = constraint.path
            ).nodeInfo
            replicaByPath(context, nodeInfo)
        } catch (throwable: Throwable) {
            setErrorStatus(context, throwable)
        } finally {
            completeRecordDetail(context)
        }
    }

    /**
     * 同步节点
     * 采用广度优先遍历
     */
    private fun replicaByPath(context: ReplicaExecutionContext, node: NodeInfo) {
        with(context) {
            if (!node.folder) {
                val executed = replicaFile(replicaContext, node)
                if (executed) {
                    progress.success += 1
                } else {
                    progress.skip += 1
                }
                return
            }
            replicaDir(replicaContext, node)
            // 查询子节点
            localDataManager.listNode(
                projectId = replicaContext.localProjectId,
                repoName = replicaContext.localRepoName,
                fullPath = node.fullPath
            ).forEach {
                if (it.folder) {
                    replicaByPath(this, it)
                    return@forEach
                }
                try {
                    val executed = replicaFile(replicaContext, it)
                    if (executed) {
                        progress.success += 1
                    } else {
                        progress.skip += 1
                    }
                } catch (throwable: Throwable) {
                    progress.failed += 1
                    setErrorStatus(this, throwable)
                    if (replicaContext.task.setting.errorStrategy == ErrorStrategy.FAST_FAIL) {
                        throw throwable
                    }
                }
            }
        }
    }

    /**
     * 根据[packageSummary]和版本列表[versionNames]执行同步
     */
    private fun replicaByPackage(
        context: ReplicaExecutionContext,
        packageSummary: PackageSummary,
        versionNames: List<String>? = null
    ) {
        with(context) {
            replicaPackage(replicaContext, packageSummary)
            val versions = if (versionNames == null) {
                // 查询所有版本
                localDataManager.listAllVersion(
                    projectId = replicaContext.localProjectId,
                    repoName = replicaContext.localRepoName,
                    packageKey = packageSummary.key,
                    option = VersionListOption()
                )
            } else {
                // 查询指定版本
                versionNames.orEmpty().map {
                    localDataManager.findPackageVersion(
                        projectId = replicaContext.localProjectId,
                        repoName = replicaContext.taskObject.localRepoName,
                        packageKey = packageSummary.key,
                        version = it
                    )
                }
            }
            versions.forEach {
                try {
                    val executed = replicaPackageVersion(replicaContext, packageSummary, it)
                    if (executed) {
                        progress.success += 1
                    } else {
                        progress.skip += 1
                    }
                } catch (throwable: Throwable) {
                    progress.failed += 1
                    setErrorStatus(this, throwable)
                    if (replicaContext.task.setting.errorStrategy == ErrorStrategy.FAST_FAIL) {
                        throw throwable
                    }
                }
            }
        }
    }

    /**
     * 初始化执行过程context
     */
    private fun initialExecutionContext(context: ReplicaContext): ReplicaExecutionContext {
        // 创建详情
        val request = RecordDetailInitialRequest(
            recordId = context.taskRecord.id,
            localCluster = context.taskDetail.task.projectId,
            remoteCluster = context.remoteCluster.name,
            localRepoName = context.localRepoName,
            repoType = context.localRepoType
        )
        val recordDetail = replicaRecordService.initialRecordDetail(request)
        return ReplicaExecutionContext(context, recordDetail)
    }

    /**
     * 设置状态为失败状态
     */
    private fun setErrorStatus(context: ReplicaExecutionContext, throwable: Throwable) {
        context.status = ExecutionStatus.FAILED
        context.appendErrorReason(throwable.message.orEmpty())
        context.replicaContext.status = ExecutionStatus.FAILED
    }

    /**
     * 同步项目
     */
    abstract fun replicaProject(context: ReplicaContext)

    /**
     * 同步仓库
     */
    abstract fun replicaRepo(context: ReplicaContext)

    /**
     * 同步包具体逻辑，由子类实现
     */
    abstract fun replicaPackage(context: ReplicaContext, packageSummary: PackageSummary)

    /**
     * 同步包版本具体逻辑，由子类实现
     * @return 是否执行了同步，如果远程存在相同版本，则返回false
     */
    abstract fun replicaPackageVersion(
        context: ReplicaContext,
        packageSummary: PackageSummary,
        packageVersion: PackageVersion
    ): Boolean

    /**
     * 同步文件具体逻辑，由子类实现
     * @return 是否执行了同步，如果远程存在相同文件，则返回false
     */
    abstract fun replicaFile(context: ReplicaContext, node: NodeInfo): Boolean

    /**
     * 同步目录节点具体逻辑，由子类实现
     * @return 是否执行了同步，如果远程存在相同目录，则返回false
     */
    abstract fun replicaDir(context: ReplicaContext, node: NodeInfo)

    /**
     * 是否包含所有仓库数据
     */
    private fun includeAllData(context: ReplicaContext): Boolean {
        return context.taskObject.packageConstraints != null &&
            context.taskObject.pathConstraints != null
    }

    /**
     * 持久化同步进度
     */
    private fun completeRecordDetail(context: ReplicaExecutionContext) {
        with(context) {
            val result = ExecutionResult(
                status = status,
                progress = progress,
                errorReason = buildErrorReason()
            )
            replicaRecordService.completeRecordDetail(detail.id, result)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ScheduledReplicator::class.java)
        private const val PAGE_SIZE = 1000
    }
}
