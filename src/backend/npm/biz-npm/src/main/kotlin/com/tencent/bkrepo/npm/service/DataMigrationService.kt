package com.tencent.bkrepo.npm.service

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.gson.JsonObject
import com.google.gson.reflect.TypeToken
import com.tencent.bkrepo.auth.pojo.enums.PermissionAction
import com.tencent.bkrepo.auth.pojo.enums.ResourceType
import com.tencent.bkrepo.common.api.constant.StringPool
import com.tencent.bkrepo.common.api.exception.ErrorCodeException
import com.tencent.bkrepo.common.api.message.CommonMessageCode
import com.tencent.bkrepo.common.artifact.permission.Permission
import com.tencent.bkrepo.common.artifact.util.http.HttpClientBuilderFactory
import com.tencent.bkrepo.npm.artifact.NpmArtifactInfo
import com.tencent.bkrepo.npm.constants.DIST
import com.tencent.bkrepo.npm.constants.VERSIONS
import com.tencent.bkrepo.npm.dao.repository.MigrationErrorDataRepository
import com.tencent.bkrepo.npm.model.TMigrationErrorData
import com.tencent.bkrepo.npm.pojo.NpmDataMigrationResponse
import com.tencent.bkrepo.npm.pojo.migration.MigrationErrorDataInfo
import com.tencent.bkrepo.npm.pojo.migration.service.MigrationErrorDataCreateRequest
import com.tencent.bkrepo.npm.utils.GsonUtils
import com.tencent.bkrepo.npm.utils.ThreadPoolManager
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

@Service
class DataMigrationService {

    @Value("\${npm.data.url: ''}")
    private val url: String = StringPool.EMPTY

    @Value("\${npm.client.registry: ''}")
    private val registry: String = StringPool.EMPTY

    @Value("\${npm.package.count: 100}")
    private val count: Int = 100

    @Autowired
    private lateinit var migrationErrorDataRepository: MigrationErrorDataRepository

    @Autowired
    private lateinit var mongoTemplate: MongoTemplate

    private val okHttpClient: OkHttpClient by lazy {
        HttpClientBuilderFactory.create().readTimeout(60L, TimeUnit.SECONDS).build()
    }

    private var totalDataSet = mutableSetOf<String>()
    private val successSet = mutableSetOf<String>()
    private val errorSet = mutableSetOf<String>()

    private final fun initTotalDataSetByUrl() {
        if (totalDataSet.isNullOrEmpty() && StringUtils.isNotEmpty(url)) {
            var response: Response? = null
            try {
                val request = Request.Builder().url(url).get().build()
                val response = okHttpClient.newCall(request).execute()
                totalDataSet = response.body()!!.byteStream().use { GsonUtils.transferInputStreamToJson(it) }.keySet()
            } catch (exception: Exception) {
                logger.error("http send [$url] for get package name data failed, {}", exception.message)
                throw exception
            } finally {
                response?.body()?.close()
            }
        }
    }

    private final fun initTotalDataSetByFile() {
        if (totalDataSet.isNullOrEmpty()) {
            val inputStream: InputStream? = this.javaClass.classLoader.getResourceAsStream(FILE_NAME)
            totalDataSet = GsonUtils.gson.fromJson<JsonObject>(
                InputStreamReader(inputStream!!),
                object : TypeToken<JsonObject>() {}.type
            ).keySet()
        }
    }

    fun dataMigrationByFile(artifactInfo: NpmArtifactInfo): NpmDataMigrationResponse<String> {
        if (counter.get() == 0) {
            initTotalDataSetByFile()
        } else {
            val result = find(artifactInfo.projectId, artifactInfo.repoName, counter.get())
            totalDataSet = result?.errorData as MutableSet<String>
        }
        return dataMigration(artifactInfo)
    }

    fun dataMigrationByUrl(artifactInfo: NpmArtifactInfo): NpmDataMigrationResponse<String> {
        if (counter.get() == 0) {
            initTotalDataSetByUrl()
        } else {
            val result = find(artifactInfo.projectId, artifactInfo.repoName, counter.get())
            totalDataSet = result?.errorData as MutableSet<String>
        }
        return dataMigration(artifactInfo)
    }

    @Permission(ResourceType.REPO, PermissionAction.READ)
    @Transactional(rollbackFor = [Throwable::class])
    fun dataMigration(artifactInfo: NpmArtifactInfo): NpmDataMigrationResponse<String> {
        logger.info("npm total pkgName size : ${totalDataSet.size}")
        val start = System.currentTimeMillis()
        val list = split(totalDataSet, count)
        val callableList: MutableList<Callable<Set<String>>> = mutableListOf()
        list.forEach {
            callableList.add(Callable {
                doDataMigration(artifactInfo, it.toSet())
                errorSet
            })
        }
        val resultList = ThreadPoolManager.execute(callableList)
        val elapseTimeMillis = System.currentTimeMillis() - start
        logger.info("npm history data migration, success[${successSet.size}], fail[${errorSet.size}], elapse [${elapseTimeMillis / 1000}] s totally")
        val collect = resultList.stream().flatMap { set -> set.stream() }.collect(Collectors.toSet())
        if (collect.isNotEmpty()) {
            insertErrorData(artifactInfo, counter.incrementAndGet(), collect)
        }
        return NpmDataMigrationResponse(
            "数据迁移信息展示：",
            totalDataSet.size,
            successSet.size,
            errorSet.size,
            elapseTimeMillis / 1000,
            collect
        )
    }

    fun doDataMigration(artifactInfo: NpmArtifactInfo, data: Set<String>) {
        logger.info("current Thread : ${Thread.currentThread().name}")
        data.forEach { pkgName ->
            var response: Response? = null
            try {
                val request =
                    Request.Builder().url(registry.trimEnd('/').plus("/$pkgName")).header("isMigration", "true").get()
                        .build()
                response = okHttpClient.newCall(request).execute()
                val searchPackageInfo = response.body()!!.byteStream().use { GsonUtils.transferInputStreamToJson(it) }
                if (checkResponse(response)) {
                    installTgzFile(searchPackageInfo)
                }
                logger.info("npm package name: [$pkgName] migration success!")
                successSet.add(pkgName)
                if (successSet.size % 10 == 0) {
                    logger.info("progress rate : successRate:[${successSet.size}/${totalDataSet.size}], failRate[${errorSet.size}/${totalDataSet.size}]")
                }
            } catch (exception: Exception) {
                logger.error("failed to install [$pkgName.json] file", exception)
                errorSet.add(pkgName)
            } finally {
                response?.body()?.close()
            }
        }
    }

    fun installTgzFile(jsonObject: JsonObject) {
        val versions = jsonObject.getAsJsonObject(VERSIONS)
        versions.keySet().forEach { version ->
            var tarball: String? = null
            var response: Response? = null
            try {
                tarball = versions.getAsJsonObject(version).getAsJsonObject(DIST).get("tarball").asString
                val request = Request.Builder().url(tarball).header("isMigration", "true").get().build()
                response = okHttpClient.newCall(request).execute()
            } catch (exception: IOException) {
                logger.error("http send [$tarball]  throw Exception", exception)
                throw exception
            } finally {
                response?.body()?.close()
            }
        }
    }

    fun <T> split(set: Set<T>, count: Int = 1000): List<List<T>> {
        val list = set.toList()
        if (set.isEmpty()) {
            return emptyList()
        }
        val resultList = mutableListOf<List<T>>()
        var itemList: MutableList<T>?
        val size = set.size

        if (size < count) {
            resultList.add(list)
        } else {
            val pre = size / count
            val last = size % count
            for (i in 0 until pre) {
                itemList = mutableListOf()
                for (j in 0 until count) {
                    itemList.add(list[i * count + j])
                }
                resultList.add(itemList)
            }
            if (last > 0) {
                itemList = mutableListOf()
                for (i in 0 until last) {
                    itemList.add(list[pre * count + i])
                }
                resultList.add(itemList)
            }
        }
        return resultList
    }

    fun checkResponse(response: Response): Boolean {
        if (!response.isSuccessful) {
            logger.warn("Download file from remote failed: [${response.code()}]")
            return false
        }
        return true
    }

    private fun insertErrorData(artifactInfo: NpmArtifactInfo, counter: Int, collect: Set<String>) {
        val dataCreateRequest = MigrationErrorDataCreateRequest(
            projectId = artifactInfo.projectId,
            repoName = artifactInfo.repoName,
            counter = counter,
            errorData = jacksonObjectMapper().writeValueAsString(collect)
        )
        create(dataCreateRequest)
    }

    @Transactional(rollbackFor = [Throwable::class])
    fun create(dataCreateRequest: MigrationErrorDataCreateRequest) {
        with(dataCreateRequest) {
            this.takeIf { errorData.isNotBlank() } ?: throw ErrorCodeException(
                CommonMessageCode.PARAMETER_MISSING,
                this::errorData.name
            )
            // repositoryService.checkRepository(projectId, repoName)
            val errorData = TMigrationErrorData(
                projectId = projectId,
                repoName = repoName,
                counter = counter,
                errorData = errorData,
                createdBy = operator,
                createdDate = LocalDateTime.now(),
                lastModifiedBy = operator,
                lastModifiedDate = LocalDateTime.now()
            )
            migrationErrorDataRepository.insert(errorData)
                .also { logger.info("Create module deps [$dataCreateRequest] success.") }
        }
    }

    fun find(projectId: String, repoName: String, counter: Int): MigrationErrorDataInfo? {
        // repositoryService.checkRepository(projectId, repoName)
        val criteria =
            Criteria.where(TMigrationErrorData::projectId.name).`is`(projectId).and(TMigrationErrorData::repoName.name)
                .`is`(repoName)
                .and(TMigrationErrorData::counter.name).`is`(counter).and(TMigrationErrorData::deleted.name).`is`(null)
        return mongoTemplate.findOne(Query.query(criteria), TMigrationErrorData::class.java)?.let { convert(it)!! }
    }

    companion object {
        private const val FILE_NAME = "pkgName.json"
        val logger: Logger = LoggerFactory.getLogger(DataMigrationService::class.java)
        private val counter = AtomicInteger(0)

        fun convert(tMigrationErrorData: TMigrationErrorData?): MigrationErrorDataInfo? {
            return tMigrationErrorData?.let {
                MigrationErrorDataInfo(
                    counter = it.counter,
                    errorData = jacksonObjectMapper().readValue(it.errorData, Set::class.java),
                    projectId = it.projectId,
                    repoName = it.repoName,
                    createdBy = it.createdBy,
                    createdDate = it.createdDate.format(DateTimeFormatter.ISO_DATE_TIME)
                )
            }
        }
    }
}