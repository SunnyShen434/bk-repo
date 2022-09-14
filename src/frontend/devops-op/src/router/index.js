import Vue from 'vue'
import Router from 'vue-router'
export const TITLE_HOME = sidebarTitle
export const ROUTER_NAME_SERVICE = 'Service'
export const ROUTER_NAME_NODE = 'Node'
export const ROUTER_NAME_INSTANCE = 'Instance'
export const ROUTER_NAME_STORAGE_CREDENTIALS = 'StorageCredentials'
export const ROUTER_NAME_EXT_PERMISSION = 'ExtPermission'
export const ROUTER_NAME_WEBHOOK = 'Webhook'
export const ROUTER_NAME_NOTIFY_CREDENTIALS = 'NotifyCredentials'
export const ROUTER_NAME_PLUGIN = 'Plugin'
export const ROUTER_NAME_SCANNERS = 'Scanners'
export const ROUTER_NAME_PROJECT_SCAN_CONFIGURATIONS = 'ProjectScanConfigurations'
export const ROUTER_NAME_JOB = 'Job'

Vue.use(Router)

/* Layout */
import Layout from '@/layout'
import { sidebarTitle } from '@/settings'

/**
 * Note: sub-menu only appear when route children.length >= 1
 * Detail see: https://panjiachen.github.io/vue-element-admin-site/guide/essentials/router-and-nav.html
 *
 * hidden: true                   if set true, item will not show in the sidebar(default is false)
 * alwaysShow: true               if set true, will always show the root menu
 *                                if not set alwaysShow, when item has more than one children route,
 *                                it will becomes nested mode, otherwise not show the root menu
 * redirect: noRedirect           if set noRedirect will no redirect in the breadcrumb
 * name:'router-name'             the name is used by <keep-alive> (must set!!!)
 * meta : {
    roles: ['admin','editor']    control the page roles (you can set multiple roles)
    title: 'title'               the name show in sidebar and breadcrumb (recommend set)
    icon: 'svg-name'/'el-icon-x' the icon show in the sidebar
    breadcrumb: false            if set false, the item will hidden in breadcrumb(default is true)
    activeMenu: '/example/list'  if set path, the sidebar will highlight the path you set
  }
 */

/**
 * constantRoutes
 * a base page that does not have permission requirements
 * all roles can be accessed
 */
export const constantRoutes = [
  {
    path: '/login',
    component: () => import('@/views/login/index'),
    hidden: true
  },

  {
    path: '/',
    redirect: '/services',
    meta: { title: TITLE_HOME, icon: 'bk' }
  },

  {
    path: '/404',
    component: () => import('@/views/404'),
    hidden: true
  }
]

/**
 * asyncRoutes
 * the routes that need to be dynamically loaded based on user roles
 */
export const asyncRoutes = [
  {
    path: '/services',
    component: Layout,
    children: [
      {
        path: '/',
        name: ROUTER_NAME_SERVICE,
        meta: { title: '服务管理', icon: 'service' },
        component: () => import('@/views/service/index')
      },
      {
        path: ':serviceName/instances',
        name: ROUTER_NAME_INSTANCE,
        hidden: true,
        component: () => import('@/views/service/Instance'),
        props: true,
        meta: { title: '服务实例' }
      }
    ]
  },
  {
    path: '/nodes',
    component: Layout,
    children: [
      {
        path: '/',
        name: ROUTER_NAME_NODE,
        meta: { title: '文件管理', icon: 'file' },
        component: () => import('@/views/node/index')
      }
    ]
  },
  {
    path: '/storage',
    alwaysShow: true,
    redirect: '/storage/credentials',
    component: Layout,
    meta: { title: '存储管理', icon: 'storage' },
    children: [
      {
        path: 'credentials',
        name: ROUTER_NAME_STORAGE_CREDENTIALS,
        component: () => import('@/views/storage/Credential'),
        meta: { title: '凭据', icon: 'credentials' }
      }
    ]
  },
  {
    path: '/ext-permission',
    component: Layout,
    children: [
      {
        path: '/',
        name: ROUTER_NAME_EXT_PERMISSION,
        meta: { title: '外部权限管理', icon: 'lock' },
        component: () => import('@/views/ext-permission/index')
      }
    ]
  },
  {
    path: '/webhook',
    component: Layout,
    meta: { title: 'WebHook管理', icon: 'webhook' },
    children: [
      {
        path: 'list',
        name: ROUTER_NAME_WEBHOOK,
        meta: { title: 'WebHook', icon: 'webhook' },
        component: () => import('@/views/webhook/index')
      },
      {
        path: 'log',
        name: ROUTER_NAME_WEBHOOK,
        meta: { title: 'WebHook日志', icon: 'file' },
        component: () => import('@/views/webhook/log/index')
      }
    ]
  },
  {
    path: '/scan',
    alwaysShow: true,
    redirect: '/scan/scanners',
    component: Layout,
    meta: { title: '制品扫描管理', icon: 'scan' },
    children: [
      {
        path: 'scanners',
        name: ROUTER_NAME_SCANNERS,
        component: () => import('@/views/scan/Scanner'),
        meta: { title: '扫描器', icon: 'scanner' }
      },
      {
        path: 'configurations',
        name: ROUTER_NAME_PROJECT_SCAN_CONFIGURATIONS,
        component: () => import('@/views/scan/ProjectScanConfiguration'),
        meta: { title: '项目扫描配置', icon: 'setting' }
      }
    ]
  },
  {
    path: '/notify',
    alwaysShow: true,
    redirect: '/notify/credentials',
    component: Layout,
    meta: { title: '通知管理', icon: 'notify' },
    children: [
      {
        path: 'credentials',
        name: ROUTER_NAME_NOTIFY_CREDENTIALS,
        component: () => import('@/views/notify/Credential'),
        meta: { title: '凭据', icon: 'credentials' }
      }
    ]
  },
  {
    path: '/plugin',
    component: Layout,
    children: [
      {
        path: '/',
        name: ROUTER_NAME_PLUGIN,
        meta: { title: '插件管理', icon: 'plugin' },
        component: () => import('@/views/plugin/index')
      }
    ]
  },
  {
    path: '/job',
    component: Layout,
    children: [
      {
        path: '/',
        name: ROUTER_NAME_JOB,
        meta: { title: '任务管理', icon: 'cc-process' },
        component: () => import('@/views/job/index')
      }
    ]
  },
  // 404 page must be placed at the end !!!
  { path: '*', redirect: '/404', hidden: true }
]

const createRouter = () => new Router({
  mode: 'history', // require service support
  base: `/${process.env.VUE_APP_BASE_DIR}`,
  scrollBehavior: () => ({ y: 0 }),
  routes: constantRoutes
})

const router = createRouter()

// Detail see: https://github.com/vuejs/vue-router/issues/1234#issuecomment-357941465
export function resetRouter() {
  const newRouter = createRouter()
  router.matcher = newRouter.matcher // reset router
}

export default router
