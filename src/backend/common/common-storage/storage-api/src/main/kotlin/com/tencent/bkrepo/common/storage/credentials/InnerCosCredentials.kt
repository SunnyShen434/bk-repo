package com.tencent.bkrepo.common.storage.credentials

import com.tencent.bkrepo.common.storage.config.CacheProperties
import com.tencent.bkrepo.common.storage.config.UploadProperties

/**
 * inner cos 身份认证信息
 *
 * @author: carrypan
 * @date: 2019-09-17
 */
data class InnerCosCredentials(
    var secretId: String = "",
    var secretKey: String = "",
    var region: String = "",
    var bucket: String = "",
    var modId: Int? = null,
    var cmdId: Int? = null,
    var timeout: Float = 0.5F,
    override var cache: CacheProperties = CacheProperties(),
    override var upload: UploadProperties = UploadProperties()
) : StorageCredentials(cache, upload) {

    companion object {
        const val type = "innercos"
    }
}
