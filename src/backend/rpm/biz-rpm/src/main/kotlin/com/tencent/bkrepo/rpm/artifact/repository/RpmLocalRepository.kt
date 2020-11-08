package com.tencent.bkrepo.rpm.artifact.repository

import com.tencent.bkrepo.common.api.constant.StringPool.DOT
import com.tencent.bkrepo.common.api.constant.StringPool.SLASH
import com.tencent.bkrepo.common.api.exception.ErrorCodeException
import com.tencent.bkrepo.common.api.util.toJsonString
import com.tencent.bkrepo.common.artifact.api.ArtifactFile
import com.tencent.bkrepo.common.artifact.exception.UnsupportedMethodException
import com.tencent.bkrepo.common.artifact.hash.sha1
import com.tencent.bkrepo.common.artifact.message.ArtifactMessageCode
import com.tencent.bkrepo.common.artifact.pojo.configuration.local.repository.RpmLocalConfiguration
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactRemoveContext
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactSearchContext
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactTransferContext
import com.tencent.bkrepo.common.artifact.repository.context.ArtifactUploadContext
import com.tencent.bkrepo.common.artifact.repository.local.LocalRepository
import com.tencent.bkrepo.common.artifact.resolve.file.ArtifactFileFactory
import com.tencent.bkrepo.common.service.util.HeaderUtils
import com.tencent.bkrepo.common.service.util.HttpContextHolder
import com.tencent.bkrepo.repository.pojo.node.service.NodeCreateRequest
import com.tencent.bkrepo.repository.pojo.node.service.NodeDeleteRequest
import com.tencent.bkrepo.repository.pojo.repo.RepositoryInfo
import com.tencent.bkrepo.rpm.GZ
import com.tencent.bkrepo.rpm.INDEXER
import com.tencent.bkrepo.rpm.NO_INDEXER
import com.tencent.bkrepo.rpm.REPODATA
import com.tencent.bkrepo.rpm.artifact.StorageManager
import com.tencent.bkrepo.rpm.exception.RpmArtifactFormatNotSupportedException
import com.tencent.bkrepo.rpm.exception.RpmArtifactMetadataResolveException
import com.tencent.bkrepo.rpm.job.JobService
import com.tencent.bkrepo.rpm.pojo.ArtifactFormat
import com.tencent.bkrepo.rpm.pojo.ArtifactFormat.RPM
import com.tencent.bkrepo.rpm.pojo.ArtifactFormat.XML
import com.tencent.bkrepo.rpm.pojo.ArtifactRepeat
import com.tencent.bkrepo.rpm.pojo.ArtifactRepeat.DELETE
import com.tencent.bkrepo.rpm.pojo.ArtifactRepeat.FULLPATH
import com.tencent.bkrepo.rpm.pojo.ArtifactRepeat.FULLPATH_SHA256
import com.tencent.bkrepo.rpm.pojo.ArtifactRepeat.NONE
import com.tencent.bkrepo.rpm.pojo.IndexType
import com.tencent.bkrepo.rpm.pojo.RepodataUri
import com.tencent.bkrepo.rpm.pojo.RpmDeleteResponse
import com.tencent.bkrepo.rpm.pojo.RpmRepoConf
import com.tencent.bkrepo.rpm.pojo.RpmUploadResponse
import com.tencent.bkrepo.rpm.pojo.RpmVersion
import com.tencent.bkrepo.rpm.util.GZipUtils.gZip
import com.tencent.bkrepo.rpm.util.RpmHeaderUtils.getRpmBooleanHeader
import com.tencent.bkrepo.rpm.util.RpmVersionUtils
import com.tencent.bkrepo.rpm.util.RpmVersionUtils.toMetadata
import com.tencent.bkrepo.rpm.util.RpmVersionUtils.toRpmVersion
import com.tencent.bkrepo.rpm.util.XmlStrUtils
import com.tencent.bkrepo.rpm.util.XmlStrUtils.getGroupNodeFullPath
import com.tencent.bkrepo.rpm.util.rpm.RpmFormatUtils
import com.tencent.bkrepo.rpm.util.rpm.RpmMetadataUtils
import com.tencent.bkrepo.rpm.util.xStream.pojo.RpmMetadataChangeLog
import com.tencent.bkrepo.rpm.util.xStream.pojo.RpmMetadataFileList
import com.tencent.bkrepo.rpm.util.xStream.pojo.RpmPackageChangeLog
import com.tencent.bkrepo.rpm.util.xStream.pojo.RpmPackageFileList
import com.tencent.bkrepo.rpm.util.xStream.pojo.RpmXmlMetadata
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import org.springframework.util.StopWatch
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import java.nio.channels.Channels

@Component
class RpmLocalRepository(
    val storageManager: StorageManager
) : LocalRepository() {

    @Autowired
    private lateinit var jobService: JobService

    override fun onUploadBefore(context: ArtifactUploadContext) {
        super.onUploadBefore(context)
        val overwrite = HeaderUtils.getRpmBooleanHeader("X-BKREPO-OVERWRITE")
        if (!overwrite) {
            with(context.artifactInfo) {
                val node = nodeClient.detail(projectId, repoName, artifactUri).data
                if (node != null) {
                    throw ErrorCodeException(ArtifactMessageCode.NODE_EXISTED, artifactUri)
                }
            }
        }
    }

    private fun rpmNodeCreateRequest(context: ArtifactUploadContext, metadata: MutableMap<String, String>?): NodeCreateRequest {
        val nodeCreateRequest = super.getNodeCreateRequest(context)
        return nodeCreateRequest.copy(
            metadata = metadata,
            overwrite = true
        )
    }

    fun initMark(context: ArtifactSearchContext) {
        with(context.artifactInfo) {
            val nodeList = nodeClient.list(
                projectId, repoName, artifactUri,
                includeFolder = false, deep = true
            ).data?.filter {
                !it.path.contains("repodata") && it.name.endsWith(".rpm")
            }
            nodeList?.let {
                for (node in nodeList) {
                    val rpmLocation = node.fullPath.removePrefix("$artifactUri/")
                    initIndexMark(context, artifactUri, mutableMapOf(), IndexType.PRIMARY, rpmLocation)
                }
            }
        }
    }

    fun xmlIndexNodeCreate(
        userId: String,
        repositoryInfo: RepositoryInfo,
        fullPath: String,
        artifactFile: ArtifactFile,
        metadata: MutableMap<String, String>?
    ): NodeCreateRequest {
        val sha256 = artifactFile.getFileSha256()
        val md5 = artifactFile.getFileMd5()
        return NodeCreateRequest(
            projectId = repositoryInfo.projectId,
            repoName = repositoryInfo.name,
            folder = false,
            overwrite = true,
            fullPath = fullPath,
            size = artifactFile.getSize(),
            sha256 = sha256,
            md5 = md5,
            operator = userId,
            metadata = metadata
        )
    }

    /**
     * 查询rpm仓库属性
     */
    private fun getRpmRepoConf(context: ArtifactTransferContext): RpmRepoConf {
        val rpmConfiguration = context.repositoryInfo.configuration as RpmLocalConfiguration
        val repodataDepth = rpmConfiguration.repodataDepth ?: 0
        val enabledFileLists = rpmConfiguration.enabledFileLists ?: false
        val groupXmlSet = rpmConfiguration.groupXmlSet ?: mutableSetOf()
        return RpmRepoConf(repodataDepth, enabledFileLists, groupXmlSet)
    }

    /**
     * 检查请求uri地址的层级是否 > 仓库设置的repodata 深度
     * @return true 将会计算rpm包的索引
     * @return false 只提供文件服务器功能，返回提示信息
     */
    private fun checkNeedIndex(context: ArtifactUploadContext, repodataDepth: Int): Boolean {
        val depth = context.artifactInfo.artifactUri.removePrefix(SLASH).split(SLASH).size
        return depth > repodataDepth
    }

    /**
     * 生成并存储构件索引mark文件
     */
    private fun mark(context: ArtifactUploadContext, repeat: ArtifactRepeat, rpmRepoConf: RpmRepoConf): RpmVersion {
        val repodataDepth = rpmRepoConf.repodataDepth
        val repodataUri = XmlStrUtils.resolveRepodataUri(context.artifactInfo.artifactUri, repodataDepth)
        val artifactFile = context.getArtifactFile()
        val sha1Digest = artifactFile.getInputStream().sha1()
        val artifactRelativePath = repodataUri.artifactRelativePath
        val stopWatch = StopWatch("mark")

        stopWatch.start("getRpmFormat")
        val rpmFormat = Channels.newChannel(artifactFile.getInputStream()).use { RpmFormatUtils.resolveRpmFormat(it) }
        val rpmMetadata = RpmMetadataUtils.interpret(rpmFormat, artifactFile.getSize(), sha1Digest, artifactRelativePath)
        stopWatch.stop()
        val rpmVersion = RpmVersion(
            rpmMetadata.packages[0].name,
            rpmMetadata.packages[0].arch,
            rpmMetadata.packages[0].version.epoch.toString(),
            rpmMetadata.packages[0].version.ver,
            rpmMetadata.packages[0].version.rel
        )
        val markFileMatedata = rpmVersion.toMetadata()
        val othersIndexData = RpmMetadataChangeLog(
            listOf(
                RpmPackageChangeLog(
                    rpmMetadata.packages[0].checksum.checksum,
                    rpmMetadata.packages[0].name,
                    rpmMetadata.packages[0].version,
                    rpmMetadata.packages[0].format.changeLogs
                )
            ),
            1L
        )
        stopWatch.start("storeOthers")
        storeIndexMarkFile(context, repodataUri, repeat, markFileMatedata, IndexType.OTHERS, othersIndexData)
        stopWatch.stop()
        if (rpmRepoConf.enabledFileLists) {
            val fileListsIndexData = RpmMetadataFileList(
                listOf(
                    RpmPackageFileList(
                        rpmMetadata.packages[0].checksum.checksum,
                        rpmMetadata.packages[0].name,
                        rpmMetadata.packages[0].version,
                        rpmMetadata.packages[0].format.files
                    )
                ),
                1L
            )
            stopWatch.start("storeFilelists")
            storeIndexMarkFile(context, repodataUri, repeat, markFileMatedata, IndexType.FILELISTS, fileListsIndexData)
            stopWatch.stop()
        }

        rpmMetadata.filterRpmFileLists()
        rpmMetadata.packages[0].format.changeLogs.clear()
        stopWatch.start("storePrimary")
        storeIndexMarkFile(context, repodataUri, repeat, markFileMatedata, IndexType.PRIMARY, rpmMetadata)
        stopWatch.stop()
        if (logger.isDebugEnabled) {
            logger.debug("markStat: $stopWatch")
        }
        return rpmVersion
    }

    /**
     * 保存索引的时候新增一个标志文件。
     */
    private fun initIndexMark(
        context: ArtifactTransferContext,
        repodataPath: String,
        metadata: MutableMap<String, String>,
        indexType: IndexType,
        rpmLocation: String
    ) {
        val markFile = ArtifactFileFactory.build(ByteArrayInputStream("mark".toByteArray()))
        metadata["repeat"] = NONE.name
        val xmlFileNode = xmlIndexNodeCreate(
            context.userId,
            context.repositoryInfo,
            "/$repodataPath/$REPODATA/${indexType.value}/$rpmLocation",
            markFile,
            metadata
        )
        storageService.store(xmlFileNode.sha256!!, markFile, context.storageCredentials)
        with(xmlFileNode) { logger.info("Success to store $projectId/$repoName/$fullPath") }
        markFile.delete()
        nodeClient.create(xmlFileNode)
        logger.info("Success to insert $xmlFileNode")
    }

    /**
     * 保存索引的时候新增一个标志文件。
     */
    private fun storeIndexMarkFile(
        context: ArtifactTransferContext,
        repodataUri: RepodataUri,
        repeat: ArtifactRepeat,
        metadata: MutableMap<String, String>,
        indexType: IndexType,
        rpmXmlMetadata: RpmXmlMetadata? = null
    ) {
        logger.info("storeIndexMarkFile, repodataUri: $repodataUri, repeat: $repeat, indexType: $indexType, metadata: $metadata")
        val artifactFile = when (repeat) {
            FULLPATH_SHA256 -> {
                logger.warn("artifact repeat is $FULLPATH_SHA256, skip")
                return
            }
            NONE, FULLPATH -> {
                ArtifactFileFactory.build(ByteArrayInputStream(XmlStrUtils.toMarkFileXml(rpmXmlMetadata!!, indexType).toByteArray()))
            }
            else -> {
                ArtifactFileFactory.build(ByteArrayInputStream("mark".toByteArray()))
            }
        }
        storageService.store(artifactFile.getFileSha256(), artifactFile, context.storageCredentials)
        val fullPath = "/${repodataUri.repodataPath}/$REPODATA/${indexType.value}/${repodataUri.artifactRelativePath}"
        metadata["repeat"] = repeat.name
        val markFileNode = xmlIndexNodeCreate(
            context.userId,
            context.repositoryInfo,
            fullPath,
            artifactFile,
            metadata
        )
        nodeClient.create(markFileNode)
        logger.info("mark file [${context.artifactInfo.projectId}|${context.artifactInfo.repoName}|$fullPath] created")
    }


    /**
     * 检查上传的构件是否已在仓库中，判断条件：uri && sha256
     * 降低并发对索引文件的影响
     * ArtifactRepeat.FULLPATH_SHA256 存在完全相同构件，不操作索引
     * ArtifactRepeat.FULLPATH 请求路径相同，但内容不同，更新索引
     * ArtifactRepeat.NONE 无重复构件
     */
    private fun checkRepeatArtifact(context: ArtifactUploadContext): ArtifactRepeat {
        val artifactUri = context.artifactInfo.artifactUri
        val artifactSha256 = context.getArtifactFile().getFileSha256()

        return with(context.artifactInfo) {
            val node = nodeClient.detail(projectId, repoName, artifactUri).data
            if (node == null) {
                NONE
            } else {
                if (node.sha256 == artifactSha256) {
                    FULLPATH_SHA256
                } else {
                    FULLPATH
                }
            }
        }
    }

    private fun successUpload(context: ArtifactUploadContext, needIndex: Boolean, repodataDepth: Int) {
        val response = HttpContextHolder.getResponse()
        response.contentType = "application/json; charset=UTF-8"
        with(context.artifactInfo) {
            val description = if (needIndex) {
                INDEXER
            } else {
                String.format(NO_INDEXER, "$projectId/$repoName", repodataDepth, artifactUri)
            }
            val rpmUploadResponse = RpmUploadResponse(
                projectId, repoName, artifactUri,
                context.getArtifactFile().getFileSha256(), context.getArtifactFile().getFileMd5(), description
            )
            response.writer.print(rpmUploadResponse.toJsonString())
        }
    }

    private fun deleteFailed(context: ArtifactRemoveContext, description: String) {
        val response = HttpContextHolder.getResponse()
        response.contentType = "application/json; charset=UTF-8"
        with(context.artifactInfo) {
            val rpmUploadResponse = RpmDeleteResponse(projectId, repoName, artifactUri, description)
            response.writer.print(rpmUploadResponse.toJsonString())
        }
    }

    private fun getArtifactFormat(context: ArtifactUploadContext): ArtifactFormat {
        val format = context.artifactInfo.artifactUri
            .split(SLASH).last().split(".").last()
        return when (format) {
            "xml" -> XML
            "rpm" -> RPM
            else -> {
                with(context.artifactInfo) { logger.info("$projectId/$repoName/$artifactUri: 格式不被接受") }
                throw RpmArtifactFormatNotSupportedException("rpm not supported `$format` artifact")
            }
        }
    }

    // 保存分组文件
    private fun storeGroupFile(context: ArtifactUploadContext) {
        val xmlByteArray = context.getArtifactFile().getInputStream().readBytes()
        val filename = context.artifactInfo.artifactUri.split("/").last()

        // 保存xml
        val xmlSha1 = context.getArtifactFile().getInputStream().sha1()
        val xmlSha1ArtifactFile = ArtifactFileFactory.build(xmlByteArray.inputStream())
        val metadata = mutableMapOf(
            "indexName" to filename,
            "indexType" to "group",
            "checksum" to xmlSha1,
            "size" to (xmlSha1ArtifactFile.getSize().toString()),
            "timestamp" to System.currentTimeMillis().toString()
        )
        val xmlNode = xmlIndexNodeCreate(
            context.userId,
            context.repositoryInfo,
            getGroupNodeFullPath(context.artifactInfo.artifactUri, xmlSha1),
            xmlSha1ArtifactFile,
            metadata
        )
        storageManager.store(context, xmlNode, xmlSha1ArtifactFile)

        // 保存xml.gz
        val groupGZFile = xmlByteArray.gZip()
        try {
            val xmlGZFileSha1 = FileInputStream(groupGZFile).sha1()
            val groupGZArtifactFile = ArtifactFileFactory.build(FileInputStream(groupGZFile))
            val metadataGZ = mutableMapOf(
                "indexName" to "${filename}_gz",
                "indexType" to "group_gz",
                "checksum" to xmlGZFileSha1,
                "size" to (groupGZArtifactFile.getSize().toString()),
                "timestamp" to System.currentTimeMillis().toString()
            )
            val groupGZNode = xmlIndexNodeCreate(
                context.userId,
                context.repositoryInfo,
                getGroupNodeFullPath("${context.artifactInfo.artifactUri}$DOT$GZ", xmlGZFileSha1),
                groupGZArtifactFile,
                metadataGZ
            )
            storageManager.store(context, groupGZNode, groupGZArtifactFile)
        } finally {
            groupGZFile.delete()
        }

        // todo 删除多余节点
        flushRepoMdXML(context, null)
    }

    /**
     * 默认刷新匹配请求路径对应的repodata目录下的`repomd.xml`内容，
     * 当[repoDataPath]不为空时，刷新指定的[repoDataPath]目录下的`repomd.xml`内容
     */
    fun flushRepoMdXML(context: ArtifactTransferContext, repoDataPath: String?) {
        logger.info("flushRepoMdXML: artifactInfo: ${context.artifactInfo}, repoDataPath: $repoDataPath")
        // 查询添加的groups
        val rpmRepoConf = getRpmRepoConf(context)
        val repodataDepth = rpmRepoConf.repodataDepth
        val indexPath = if (repoDataPath == null) {
            val repodataUri = XmlStrUtils.resolveRepodataUri(context.artifactInfo.artifactUri, repodataDepth)
            "/${repodataUri.repodataPath}$REPODATA"
        } else {
            repoDataPath
        }
        jobService.flushRepoMdXML(context.repositoryInfo, indexPath)
    }

    @Transactional(rollbackFor = [Throwable::class])
    override fun onUpload(context: ArtifactUploadContext) {
        val artifactFormat = getArtifactFormat(context)
        val rpmRepoConf = getRpmRepoConf(context)
        val needIndex: Boolean = checkNeedIndex(context, rpmRepoConf.repodataDepth)
        val repeat = checkRepeatArtifact(context)
        logger.info("onUpload, artifactFormat: $artifactFormat, needIndex: $needIndex, repeat: $repeat, artifactUri: ${context.artifactInfo.artifactUri}")
        if (repeat != FULLPATH_SHA256) {
            val nodeCreateRequest = if (needIndex) {
                when (artifactFormat) {
                    RPM -> {
                        val rpmVersion = mark(context, repeat, rpmRepoConf)
                        val metadata = rpmVersion.toMetadata()
                        metadata["action"] = repeat.name
                        rpmNodeCreateRequest(context, metadata)
                    }
                    XML -> {
                        storeGroupFile(context)
                        rpmNodeCreateRequest(context, mutableMapOf())
                    }
                }
            } else {
                rpmNodeCreateRequest(context, mutableMapOf())
            }

            storageService.store(nodeCreateRequest.sha256!!, context.getArtifactFile(), context.storageCredentials)
            with(context.artifactInfo) { logger.info("Success to store $projectId/$repoName/$artifactUri") }
            nodeClient.create(nodeCreateRequest)
            logger.info("Success to insert $nodeCreateRequest")
            flushRepoMdXML(context, null)
        }
        successUpload(context, needIndex, rpmRepoConf.repodataDepth)
    }

    @Transactional(rollbackFor = [Throwable::class])
    override fun remove(context: ArtifactRemoveContext) {
        with(context.artifactInfo) {
            val node = nodeClient.detail(projectId, repoName, artifactUri).data
            if (node == null) {
                deleteFailed(context, "not found")
                return
            }
            if (node.folder) {
                throw UnsupportedMethodException("Delete folder is forbidden")
            }
            val nodeMetadata = node.metadata
            val artifactSha256 = node.sha256
            val rpmVersion = try {
                nodeMetadata.toRpmVersion(artifactUri)
            } catch (rpmArtifactMetadataResolveException: RpmArtifactMetadataResolveException) {
                logger.warn("$this not found metadata")
                RpmVersionUtils.resolverRpmVersion(artifactUri.split("/").last())
            }
            val artifactUri = context.artifactInfo.artifactUri
            // 定位对应请求的索引目录
            val rpmRepoConf = getRpmRepoConf(context)
            val repodataDepth = rpmRepoConf.repodataDepth
            val repodataUri = XmlStrUtils.resolveRepodataUri(context.artifactInfo.artifactUri, repodataDepth)

            storeIndexMarkFile(context, repodataUri, DELETE, rpmVersion.toMetadata(), IndexType.PRIMARY)
            storeIndexMarkFile(context, repodataUri, DELETE, rpmVersion.toMetadata(), IndexType.OTHERS)
            if (rpmRepoConf.enabledFileLists) {
                storeIndexMarkFile(context, repodataUri, DELETE, rpmVersion.toMetadata(), IndexType.FILELISTS)
            }

            val nodeDeleteRequest = NodeDeleteRequest(projectId, repoName, artifactUri, context.userId)
            nodeClient.delete(nodeDeleteRequest)
            logger.info("node [$projectId|$repoName|$artifactUri] deleted")
        }
    }

    /**
     * 刷新仓库下所有repodata
     */
    fun flushAllRepoData(context: ArtifactTransferContext) {
        // 查询仓库索引层级
        val rpmRepoConf = getRpmRepoConf(context)
        val targetSet = mutableSetOf<String>()
        listAllRepoDataFolder(context, "/", rpmRepoConf.repodataDepth, targetSet)
        for (repoDataPath in targetSet) {
            flushRepoMdXML(context, repoDataPath)
        }
    }

    private fun listAllRepoDataFolder(
        context: ArtifactTransferContext,
        fullPath: String,
        repodataDepth: Int,
        repoDataSet: MutableSet<String>
    ) {
        with(context.artifactInfo) {
            val nodeList = nodeClient.list(projectId, repoName, fullPath).data ?: return
            if (repodataDepth == 0) {
                for (node in nodeList.filter { it.folder }.filter { it.name == REPODATA }) {
                    repoDataSet.add(node.fullPath)
                }
            } else {
                for (node in nodeList.filter { it.folder }) {
                    listAllRepoDataFolder(context, node.fullPath, repodataDepth.dec(), repoDataSet)
                }
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RpmLocalRepository::class.java)
    }
}
