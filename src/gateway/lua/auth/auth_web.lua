--[[
Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.

Copyright (C) 2019 THL A29 Limited, a Tencent company.  All rights reserved.

BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.

A copy of the MIT License is included in this file.


Terms of the MIT License:
---------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
]]

--- 蓝鲸平台登录对接
--- 获取Cookie中bk_token
local bk_token, err = cookieUtil:get_cookie("bk_ticket")
local bkrepo_token, err2 = cookieUtil:get_ticket("bkrepo_ticket")
local ticket = nil

--- standalone模式下校验bkrepo_ticket
if config.mode == "standalone" then
  --- 跳过登录请求
  start_i = string.find(ngx.var.request_uri, "login")
  if start_i ~= nil then
    return
  end
  if bkrepo_token == nil then
    ngx.log(ngx.STDERR, "failed to read user request bkrepo_ticket: ", err2)
    ngx.exit(401)
    return
  end
  ticket = oauthUtil:verify_bkrepo_token(bkrepo_token)
end

--- 其它模式校验bk_ticket
local devops_access_token = ngx.var.http_x_devops_access_token
if bk_token == nil and devops_access_token == nil then
  ngx.log(ngx.STDERR, "failed to read user request bk_token or devops_access_token: ", err)
  ngx.exit(401)
  return
end
if devops_access_token ~= nill then
  ticket = oauthUtil:verify_token(devops_access_token)
else
  ticket = oauthUtil:get_ticket(bk_token)
end

--- 设置用户信息
ngx.header["x-bkrepo-uid"] = ticket.user_id
ngx.header["x-bkrepo-bk-token"] = bk_token
ngx.header["x-bkrepo-access-token"] = ticket.access_token
ngx.exit(200)