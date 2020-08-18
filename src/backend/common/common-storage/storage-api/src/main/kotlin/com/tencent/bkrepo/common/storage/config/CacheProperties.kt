package com.tencent.bkrepo.common.storage.config

/**
 * 本地文件缓存属性
 */
data class CacheProperties(
    /**
     * 缓存开关
     */
    var enabled: Boolean = false,
    /**
     * 存放缓存文件的本地目录
     */
    var path: String = "data/cached",
    /**
     * 优先从缓存加载文件
     */
    var loadCacheFirst: Boolean = true,
    /**
     * 缓存文件时间，单位天。小于或等于0则永久存储
     */
    var expireDays: Int = -1
)