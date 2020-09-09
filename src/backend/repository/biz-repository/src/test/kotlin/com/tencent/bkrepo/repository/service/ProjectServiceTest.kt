package com.tencent.bkrepo.repository.service

import com.tencent.bkrepo.common.api.exception.ErrorCodeException
import com.tencent.bkrepo.repository.UT_PROJECT_DESC
import com.tencent.bkrepo.repository.UT_PROJECT_ID
import com.tencent.bkrepo.repository.UT_PROJECT_DISPLAY
import com.tencent.bkrepo.repository.UT_USER
import com.tencent.bkrepo.repository.dao.repository.ProjectRepository
import com.tencent.bkrepo.repository.pojo.project.ProjectCreateRequest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest

@DisplayName("项目服务测试")
@DataMongoTest
class ProjectServiceTest @Autowired constructor(
    private val projectService: ProjectService,
    private val projectRepository: ProjectRepository
) : ServiceBaseTest() {

    @BeforeEach
    fun beforeEach() {
        initMock()
        projectRepository.deleteAll()
    }

    @Test
    @DisplayName("测试创建项目")
    fun `test create project`() {
        val request = ProjectCreateRequest(UT_PROJECT_ID, UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)
    }

    @Test
    @DisplayName("测试创建同名项目")
    fun `should throw exception when project exists`() {
        val request = ProjectCreateRequest(UT_PROJECT_ID, UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)
        assertThrows<ErrorCodeException> { projectService.create(request) }
    }

    @Test
    @DisplayName("测试非法项目名称")
    fun `should throw exception with illegal name`() {
        var request = ProjectCreateRequest("1", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        assertThrows<ErrorCodeException> { projectService.create(request) }

        request = ProjectCreateRequest("11", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        assertThrows<ErrorCodeException> { projectService.create(request) }

        request = ProjectCreateRequest("a".repeat(33), UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        assertThrows<ErrorCodeException> { projectService.create(request) }

        request = ProjectCreateRequest("test_1", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)

        request = ProjectCreateRequest("test-1", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)

        request = ProjectCreateRequest("a1", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)

        request = ProjectCreateRequest("_prebuild", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)

        request = ProjectCreateRequest("CODECC_a1", UT_PROJECT_DISPLAY, UT_PROJECT_DESC, UT_USER)
        projectService.create(request)
    }

    @Test
    @DisplayName("测试非法项目显示名")
    fun `should throw exception with illegal display name`() {
        var request = ProjectCreateRequest(UT_PROJECT_ID, "", UT_PROJECT_DESC, UT_USER)
        assertThrows<ErrorCodeException> { projectService.create(request) }

        request = ProjectCreateRequest(UT_PROJECT_ID, "1".repeat(33), UT_PROJECT_DESC, UT_USER)
        assertThrows<ErrorCodeException> { projectService.create(request) }

        request = ProjectCreateRequest(UT_PROJECT_ID, "1".repeat(32), UT_PROJECT_DESC, UT_USER)
        projectService.create(request)

        projectRepository.deleteAll()
        request = ProjectCreateRequest(UT_PROJECT_ID, "1", UT_PROJECT_DESC, UT_USER)
        projectService.create(request)

        projectRepository.deleteAll()
        request = ProjectCreateRequest(UT_PROJECT_ID, "123-abc", UT_PROJECT_DESC, UT_USER)
        projectService.create(request)
    }
}
