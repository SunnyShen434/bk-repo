package com.tencent.bkrepo.repository.job

import com.tencent.bkrepo.common.api.util.JsonUtils
import com.tencent.bkrepo.common.service.log.LoggerHolder
import com.tencent.bkrepo.common.storage.core.StorageService
import com.tencent.bkrepo.common.storage.credentials.StorageCredentials
import com.tencent.bkrepo.common.storage.util.FileDigestUtils
import com.tencent.bkrepo.repository.dao.NodeDao
import com.tencent.bkrepo.repository.model.TNode
import com.tencent.bkrepo.repository.repository.RepoRepository
import kotlin.concurrent.thread
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

/**
 * 计算文件md5
 * @author: carrypan
 * @date: 2019/12/24
 */
@Component
@Order(Ordered.LOWEST_PRECEDENCE)
class Md5CalculateJob : ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private lateinit var nodeDao: NodeDao

    @Autowired
    private lateinit var storageService: StorageService

    @Autowired
    private lateinit var repoRepository: RepoRepository

    @Async
    @SchedulerLock(name = "Md5CalculateJob", lockAtMostFor = "P1D")
    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        thread { calculate() }
    }

    fun calculate() {
        logger.info("Starting to calculate md5.")
        var totalCount = 0L
        var cleanupCount = 0L
        var failedCount = 0L
        var fileMissingCount = 0L
        val startTimeMillis = System.currentTimeMillis()

        repoRepository.findAll().forEach { repo ->
            val query = Query.query(Criteria.where(TNode::projectId.name).`is`(repo.projectId)
                .and(TNode::repoName.name).`is`(repo.name)
                .and(TNode::folder.name).`is`(false)
                .and(TNode::md5.name).`is`(null)
            )
            val storageCredentials = repo.storageCredentials?.let { property ->
                JsonUtils.objectMapper.readValue(property, StorageCredentials::class.java)
            }
            nodeDao.find(query).forEach { node ->
                try {
                    if (storageService.exist(node.sha256!!, storageCredentials)) {
                        val file = storageService.load(node.sha256!!, storageCredentials)!!
                        val md5 = FileDigestUtils.fileMd5(file.inputStream())
                        val nodeQuery = Query.query(Criteria.where(TNode::projectId.name).`is`(repo.projectId)
                            .and(TNode::repoName.name).`is`(repo.name)
                            .and(TNode::fullPath.name).`is`(node.fullPath)
                            .and(TNode::deleted.name).`is`(node.deleted)
                        )
                        val nodeUpdate = Update.update("md5", md5)
                        nodeDao.updateFirst(nodeQuery, nodeUpdate)
                        cleanupCount += 1
                    } else {
                        logger.warn("File[${node.sha256}] is missing on [$storageCredentials], skip calculating.")
                        fileMissingCount += 1
                    }
                } catch (exception: Exception) {
                    logger.error("Failed to calculate md5 of file[${node.sha256}] on [$storageCredentials].", exception)
                    failedCount += 1
                }
                totalCount += 1
            }
        }

        val elapseTimeMillis = System.currentTimeMillis() - startTimeMillis
        logger.info("Calculate [$totalCount] files, success[$cleanupCount], failed[$failedCount], " +
            "file missing[$fileMissingCount], elapse [$elapseTimeMillis] ms totally.")
    }

    companion object {
        private val logger = LoggerHolder.JOB
    }
}