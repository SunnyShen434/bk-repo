package com.tencent.bkrepo.repository.service

import com.tencent.bkrepo.auth.api.ServiceRoleResource
import com.tencent.bkrepo.auth.api.ServiceUserResource
import com.tencent.bkrepo.common.artifact.config.AuthProperties
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.mongodb.core.MongoTemplate

abstract class AbstractService {

    @Autowired
    open lateinit var roleResource: ServiceRoleResource

    @Autowired
    open lateinit var userResource: ServiceUserResource

    @Autowired
    open lateinit var mongoTemplate: MongoTemplate

    @Autowired
    private lateinit var eventPublisher: ApplicationEventPublisher

    @Autowired
    private lateinit var authProperties: AuthProperties

    fun publishEvent(any: Any) {
        eventPublisher.publishEvent(any)
    }

    fun createRepoManager(projectId: String, repoName: String, userId: String) {
        try {
            val repoManagerRoleId = roleResource.createRepoManage(projectId, repoName).data!!
            userResource.addUserRole(userId, repoManagerRoleId)
        } catch (exception: Exception) {
            if (authProperties.enabled) {
                throw exception
            } else {
                logger.warn("Create repository manager failed, ignore exception due to auth disabled[${exception.message}].")
            }
        }
    }

    fun createProjectManager(projectId: String, operator: String) {
        try {
            val projectManagerRoleId = roleResource.createProjectManage(projectId).data!!
            userResource.addUserRole(operator, projectManagerRoleId)
        } catch (exception: Exception) {
            if (authProperties.enabled) {
                throw exception
            } else {
                logger.warn("Create project manager failed, ignore exception due to auth disabled[${exception.message}].")
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AbstractService::class.java)
    }
}