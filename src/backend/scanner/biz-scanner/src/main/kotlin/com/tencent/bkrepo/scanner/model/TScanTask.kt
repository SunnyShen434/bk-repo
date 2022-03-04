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

package com.tencent.bkrepo.scanner.model

import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDateTime

@Document("scanner_config")
data class TScanTask(
    val id: String,
    val createdBy: String,
    val createdDate: LocalDateTime,
    val lastModifiedBy: String,
    val lastModifiedDate: LocalDateTime,
    /**
     * 项目id
     */
    val projectId: String? = null,
    /**
     * 仓库名
     */
    val repoName: String? = null,
    /**
     * 扫描的目录或文件路径
     */
    val fullPath: String? = null,
    /**
     * 扫描文件匹配规则
     */
    val rule: String? = null,
    /**
     * 触发扫描时间戳
     */
    val triggerTime: Long,
    /**
     * 开始扫描时间戳
     */
    val startTime: Long,
    /**
     * 结束扫描时间戳
     */
    val finishedTime: Long,
    /**
     * 触发类型，手动、新构件上传、定时扫描
     */
    val triggerType: String,
    /**
     * 任务状态
     */
    val status: String,
    /**
     * 需要扫描的文件数
     */
    val total: Long,
    /**
     * 已扫描文件数
     */
    val scanned: Long,
    /**
     * 使用的扫描器
     */
    val scannerKey: String,
    /**
     * 扫描结果统计信息
     */
    val scanResultOverview: ScanResultOverview
)

/**
 * 任务状态
 */
enum class TaskStatus{
    /**
     * 排队中
     */
    PENDING,
    /**
     * 扫描中
     */
    SCANNING,
    /**
     * 扫描暂停
     */
    PAUSE,
    /**
     * 扫描中止
     */
    STOPPED,
    /**
     * 扫描结束
     */
    FINISHED
}
